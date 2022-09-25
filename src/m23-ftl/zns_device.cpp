/*
 * MIT License
Copyright (c) 2021 - current
Authors:  Animesh Trivedi
This code is part of the Storage System Course at VU Amsterdam
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#include "zns_device.h"
#include "libnvme.h"
#include <cerrno>
#include <unordered_map>
#include <string.h>
#include <pthread.h>

extern "C"
{

#define ENTRY_INVALID                         (1L << 63)
#define address_2_zone(addr)                  ((addr) / (zns_dev_ex->blocks_per_zone * zns_dev->lba_size_bytes)) 
#define address_2_offset(addr)                ((addr) % (zns_dev_ex->blocks_per_zone * zns_dev->lba_size_bytes) / zns_dev->lba_size_bytes) 
#define zone_2_address(zone_no)                zone_no * (zns_dev_ex->blocks_per_zone * zns_dev->lba_size_bytes)
#define EMPTY 1
#define FULL 14


    std::unordered_map<int64_t, int64_t> log_mapping;
    std::unordered_map<int64_t, int64_t> data_mapping;

    pthread_mutex_t gc_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t gc_wakeup = PTHREAD_COND_INITIALIZER;
    pthread_cond_t gc_sleep = PTHREAD_COND_INITIALIZER;

    pthread_t gc_thread_id = 0;
    bool gc_thread_stop = false;

    struct zns_device_extra_info
    {
        int fd;
        uint32_t nsid;
        uint32_t blocks_per_zone;
        uint32_t log_zone_start;
        uint32_t log_zone_end;
        uint32_t data_zone_start; // for milestone 5
        uint32_t data_zone_end;   // for milestone 5
        uint8_t *zone_states;
        int gc_watermark;
        int log_zone_num_config;
        // ...
    };

    struct user_zns_device *zns_dev;
    struct zns_device_extra_info *zns_dev_ex;

    int get_free_lz_num()
    {
        return zns_dev_ex->log_zone_num_config - (zns_dev_ex->log_zone_end - zns_dev_ex->log_zone_start) / zns_dev_ex->blocks_per_zone;
    }

    // find the next empty zone address
    int find_next_empty_zone(){
        for (uint64_t i = zns_dev_ex->log_zone_num_config; i < zns_dev->tparams.zns_num_zones; i++)
        {
            if(zns_dev_ex->zone_states[i]==EMPTY)
            {
                return i * zns_dev_ex->blocks_per_zone;
            }
        }
	printf("no empty zone now!\n");
        return -1;    
    }

    // init the full merge to the next empty zone, zone_no is the virtual number
    int full_merge(int64_t zone_no,int64_t zone_address){
        
        int empty_zone_address = find_next_empty_zone();
        if (empty_zone_address==-1)
        {
            printf("ERROR: failed to find empty zone\n");
            return empty_zone_address;
        }

        // Read the data in the zone to the memeory
        __u64 res_lba;
        int64_t ret, nlb =  zns_dev_ex->blocks_per_zone, lsb = zns_dev->lba_size_bytes;
        char* buffer = (char *)calloc(1, nlb * lsb);
        char* log_buffer = (char *)calloc(1, lsb);

        ret = nvme_read(zns_dev_ex->fd, zns_dev_ex->nsid, zone_address, nlb-1, 0, 0, 0, 0, 0, nlb*lsb, buffer, 0, NULL);
        if (ret)
        {
            printf("ERROR: failed to read data block at 0x%lx, ret: %d, during full merge\n", zone_address, ret);
            return ret;
        }

        //loop the blocks' virtual address and check if it in log_mapping, if so update the new data to the data zone
        int64_t base = zone_2_address(zone_no);
        for(int i=0;i<nlb;i++){
            if (log_mapping.find(base+i*lsb)!=log_mapping.end()){
                ret = nvme_read(zns_dev_ex->fd, zns_dev_ex->nsid, log_mapping[base+i*lsb], 0, 0, 0, 0, 0, 0, lsb, log_buffer, 0, NULL);
                if (ret)
                {
                    printf("ERROR: failed to read log block at 0x%lx, ret: %d, during full merge\n", log_mapping[base+i*lsb], ret);
                    return ret;
                }

                // update log buffer to new data block, and delete the key-value pair in log mapping
                ret = nvme_zns_append(zns_dev_ex->fd, zns_dev_ex->nsid, empty_zone_address+i, 0, 0, 0, 0, 0, lsb, log_buffer, 0, NULL, &res_lba);
                if (ret)
                {
                    printf("ERROR: failed to write at 0x%d, ret: %d \n", empty_zone_address+i, ret);
                    return ret;
                }
                log_mapping.erase(base+i*lsb);      
            }
            else
            {
                ret = nvme_zns_append(zns_dev_ex->fd, zns_dev_ex->nsid, empty_zone_address+i, 0, 0, 0, 0, 0, lsb, buffer+i*lsb, 0, NULL, &res_lba);
                if (ret)
                {
                    printf("ERROR: failed to write at 0x%d, ret: %d \n", empty_zone_address+i, ret);
                    return ret;
                }
            }
        }
        data_mapping[zone_no]=empty_zone_address;
        // Reset the previous zone
        nvme_zns_mgmt_send(zns_dev_ex->fd, zns_dev_ex->nsid, zone_address, true, NVME_ZNS_ZSA_RESET, 0, NULL);
        //Update zone state
        zns_dev_ex->zone_states[empty_zone_address/lsb]=FULL;
        zns_dev_ex->zone_states[zone_address/lsb]=EMPTY;

        return 0;
    }

    //zone_no is the virtual number 
    int allocate_data_zone(int64_t zone_no)
    {
        int empty_zone_address = find_next_empty_zone();
        if (empty_zone_address==-1)
        {
            printf("ERROR: failed to find empty zone\n");
            return empty_zone_address;
        }

        // Write data in log mapping to data zone, if not then write \0
        __u64 res_lba;
        int64_t ret, nlb =  zns_dev_ex->blocks_per_zone, lsb = zns_dev->lba_size_bytes;
        char* buffer = (char *)calloc(1, nlb * lsb);
        char* log_buffer = (char *)calloc(1, lsb);

        int64_t base = zone_2_address(zone_no);

        for(int i=0;i<nlb;i++){
            if (log_mapping.find(base+i*lsb)!=log_mapping.end()){
                ret = nvme_read(zns_dev_ex->fd, zns_dev_ex->nsid, log_mapping[base+i*lsb], 0, 0, 0, 0, 0, 0, lsb, log_buffer, 0, NULL);
                if (ret)
                {
                    printf("ERROR: failed to read log block at 0x%lx, ret: %d, during full merge\n", log_mapping[base+i*lsb], ret);
                    return ret;
                }

                // update log buffer to new data block, and delete the key-value pair in log mapping
                ret = nvme_zns_append(zns_dev_ex->fd, zns_dev_ex->nsid, empty_zone_address+i, 0, 0, 0, 0, 0, lsb, log_buffer, 0, NULL, &res_lba);
                if (ret)
                {
                    printf("ERROR: failed to write at 0x%d, ret: %d \n", empty_zone_address+i, ret);
                    return ret;
                }
                log_mapping.erase(base+i*lsb);      
            }
            else
            {
                ret = nvme_zns_append(zns_dev_ex->fd, zns_dev_ex->nsid, empty_zone_address+i, 0, 0, 0, 0, 0, lsb, buffer+i*lsb, 0, NULL, &res_lba);
                if (ret)
                {
                    printf("ERROR: failed to write at 0x%d, ret: %d \n", empty_zone_address+i, ret);
                    return ret;
                }
            }
        }

        data_mapping[zone_no]=empty_zone_address;
        zns_dev_ex->zone_states[empty_zone_address/lsb]=FULL;
        
        return 0;
    }

    void *gc_loop(void *args)
    {
        while (1)
        {
            pthread_mutex_lock(&gc_mutex);
            while (!gc_thread_stop && get_free_lz_num() > zns_dev_ex->gc_watermark)
            {
                printf("GC is sleeping\n");
                pthread_cond_wait(&gc_wakeup, &gc_mutex);
                printf("GC in the house\n");
            }
            
            printf("GC is working now!\n");

            if (gc_thread_stop)
            {
                printf("GC out\n");
                pthread_mutex_unlock(&gc_mutex);
                break;
            }

            //for every data zone, update it.
            std::unordered_map<int64_t, int64_t>::iterator data_mapping_iter;
            data_mapping_iter=data_mapping.begin();
            while (data_mapping_iter!=data_mapping.end())
            {
                int ret = full_merge(data_mapping_iter->first,data_mapping_iter->second);
                if (ret) 
                {
                    printf("full_merge error: %d!\n",ret);
                }
            }

            //for the remaining data in the log zone, allocate empty data zone to it
            while(true){
                int64_t zone_no=-1;
                std::unordered_map<int64_t, int64_t>::iterator log_mapping_iter;
                char * buffer = (char *)calloc(1, zns_dev_ex->blocks_per_zone * zns_dev->lba_size_bytes);

                log_mapping_iter=log_mapping.begin();
                if (log_mapping_iter!=log_mapping.end())
                {
                    zone_no=address_2_zone(log_mapping_iter->first);
                }
                
                if(zone_no==-1)
                {
		    printf("log_mapping is empty now\n");
                    break;
                }
                else
                {
                    int ret=allocate_data_zone(zone_no);
                    if (ret) 
                    {
                        printf("allocate data zone error: %d!\n",ret);
                    }
		    printf("allocate data zone virtual number zone_no:%d successfully!\n",zone_no);
                }
            }

            pthread_cond_signal(&gc_sleep);
            pthread_mutex_unlock(&gc_mutex);
        }

        return (void *)0;
    }



    int init_ss_zns_device(struct zdev_init_params *params, struct user_zns_device **my_dev)
    {
        int fd = nvme_open(params->name);
        if (fd < 0)
        {
            printf("device %s opening failed %d errno %d \n", params->name, fd, errno);
            return -fd;
        }

        struct zns_device_extra_info *info = static_cast<struct zns_device_extra_info *>(calloc(sizeof(struct zns_device_extra_info), 1));
        (*my_dev) = static_cast<struct user_zns_device *>(calloc(sizeof(struct user_zns_device), 1));
        info->fd = fd;
        info->gc_watermark = params->gc_wmark;
        info->log_zone_num_config = params->log_zones;
        (*my_dev)->_private = info;

        int ret = nvme_get_nsid(fd, &(info->nsid));
        if (ret != 0)
        {
            printf("ERROR: failed to retrieve the nsid %d \n", ret);
            return ret;
        }

        struct nvme_id_ns ns;
        ret = nvme_identify_ns(fd, info->nsid, &ns);
        if (ret)
        {
            printf("ERROR: failed to retrieve the nsid struct %d \n", ret);
            return ret;
        }

        (*my_dev)->lba_size_bytes = 1 << ns.lbaf[(ns.flbas & 0xf)].ds;
        (*my_dev)->tparams.zns_lba_size = (*my_dev)->lba_size_bytes;

        struct nvme_zone_report report;
        ret = nvme_zns_mgmt_recv(fd, info->nsid, 0,
                                 NVME_ZNS_ZRA_REPORT_ZONES, NVME_ZNS_ZRAS_REPORT_ALL,
                                 0, sizeof(report), (void *)&report);
        if (ret != 0)
        {
            fprintf(stderr, "failed to report zones, ret %d \n", ret);
            return ret;
        }

        (*my_dev)->tparams.zns_num_zones = report.nr_zones;
        info->zone_states = (uint8_t *)calloc(report.nr_zones,sizeof(uint8_t));

        uint64_t total_size = sizeof(report) + (report.nr_zones * sizeof(struct nvme_zns_desc));
        char *zone_reports = (char *)calloc(1, total_size);
        // dont need to report all for milestone 2, but needed for milestone 5
        ret = nvme_zns_mgmt_recv(fd, info->nsid, 0,
                                 NVME_ZNS_ZRA_REPORT_ZONES, NVME_ZNS_ZRAS_REPORT_ALL,
                                 1, total_size, (void *)zone_reports);
        if (ret)
        {
            free(zone_reports);
            printf("ERROR: failed to get zone reports %d \n", ret);
            return ret;
        }

        uint64_t blocks_per_zone = ((struct nvme_zone_report *)zone_reports)->entries[0].zcap;
        info->blocks_per_zone = blocks_per_zone;
        (*my_dev)->tparams.zns_zone_capacity = blocks_per_zone * (*my_dev)->lba_size_bytes;
        // need to update this when doing persistence
        (*my_dev)->capacity_bytes = (report.nr_zones - params->log_zones) * ((*my_dev)->tparams.zns_zone_capacity);

        for (uint64_t i = params->log_zones; i < report.nr_zones; i++)
        {
            info->zone_states[i] = (((struct nvme_zone_report *)zone_reports)->entries[i].zs >> 4);
        } 

        free(zone_reports);

        // populate log_mapping for ms5
        // populate data_mapping for ms5

        // record log_zone_start and log_zone_end for ms5
        // record data_zone_start and data_zone_end for ms5

        if (params->force_reset)
        {
            ret = nvme_zns_mgmt_send(fd, info->nsid, 0, true, NVME_ZNS_ZSA_RESET, 0, NULL);
            if (ret)
            {
                printf("ERROR: failed to reset all zones %d \n", ret);
                return ret;
            }

            info->data_zone_start = info->data_zone_end = params->log_zones * blocks_per_zone;
            info->log_zone_start = info->log_zone_end = 0;
        }

        ret = pthread_create(&gc_thread_id, NULL, &gc_loop, NULL);
        if (ret)
        {
            printf("ERROR: failed to create gc thread %d \n", ret);
            return ret;
        }

        zns_dev = *my_dev;
        zns_dev_ex = info;

        return 0;
    }

    int zns_udevice_read(struct user_zns_device *my_dev, uint64_t address, void *buffer, uint32_t size)
    {
        if (size % my_dev->lba_size_bytes)
        {
            printf("INVALID: read size not aligned to block size\n");
            return -1;
        }

        int32_t ret, lba_s = my_dev->lba_size_bytes;
        uint32_t blocks = size / lba_s, num_read = 0;
        struct zns_device_extra_info *info = (struct zns_device_extra_info *)my_dev->_private;
        for (uint64_t i = address; i < address + blocks * lba_s; i += lba_s)
        {
            uint64_t entry;
            // the top bit 1 means invalid
            if (log_mapping.find(address) == log_mapping.end() || (entry & ENTRY_INVALID))
            {
                uint64_t zone_no = address_2_zone(address);
                if (data_mapping.find(zone_no) == data_mapping.end())
                {
                    printf("ERROR: no data at 0x%lx\n", address);
                    return ret;
                }

                entry = data_mapping[zone_no] + address_2_offset(address);
            }
            entry = log_mapping[i];
            ret = nvme_read(info->fd, info->nsid, (entry & ~ENTRY_INVALID), 0, 0, 0, 0, 0, 0, lba_s, (char *)buffer + num_read, 0, NULL);
            if (ret)
            {
                printf("ERROR: failed to read at 0x%lx, ret: %d\n", (entry & ~ENTRY_INVALID), ret);
                return ret;
            }
            num_read += lba_s;
        }

        return 0;
    }

    int zns_udevice_write(struct user_zns_device *my_dev, uint64_t address, void *buffer, uint32_t size)
    {
        if (size % my_dev->lba_size_bytes)
        {
            printf("INVALID: write size not aligned to block size\n");
            return -1;
        }

        struct zns_device_extra_info *info = (struct zns_device_extra_info *)my_dev->_private;
        pthread_mutex_lock(&gc_mutex);
        while (get_free_lz_num() <= info->gc_watermark)
        {
            pthread_cond_signal(&gc_wakeup);
            pthread_cond_wait(&gc_sleep, &gc_mutex);
        }

        uint32_t blocks = size / my_dev->lba_size_bytes;
        __u64 res_lba;
        int32_t ret, lz_end_before = info->log_zone_end, z_no = info->log_zone_end / info->blocks_per_zone;
        ret = nvme_zns_append(info->fd, info->nsid, z_no * info->blocks_per_zone, blocks - 1, 0, 0, 0, 0, size, buffer, 0, NULL, &res_lba);
        if (ret)
        {
            printf("ERROR: failed to write at 0x%d, ret: %d \n", info->log_zone_end, ret);
            return ret;
        }

        info->log_zone_end = res_lba + 1;
        for (uint32_t i = 0; i < blocks; i++)
        {
            log_mapping[address + i * my_dev->lba_size_bytes] = lz_end_before + i;
        }

        pthread_mutex_unlock(&gc_mutex);
        return 0;
    }

    int deinit_ss_zns_device(struct user_zns_device *my_dev)
    {
        gc_thread_stop = true;
        pthread_cond_signal(&gc_wakeup);

        // wait for gc stop
        pthread_join(gc_thread_id, NULL);

        pthread_mutex_destroy(&gc_mutex);
        pthread_cond_destroy(&gc_wakeup);

        free(zns_dev_ex->zone_states);
        free(my_dev->_private);
        free(my_dev);
        return 0;
    }
}
