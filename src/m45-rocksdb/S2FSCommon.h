#ifndef STOSYS_PROJECT_S2FILESYSTEM_COMMON_H
#define STOSYS_PROJECT_S2FILESYSTEM_COMMON_H

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"

#include <list>
#include <atomic>
#include <unordered_map>
#include <pthread.h>
#include <zns_device.h>

namespace ROCKSDB_NAMESPACE
{
    static std::atomic_int32_t id_alloc(0);

    class S2FSObject
    {
    private:
        uint32_t size;
        pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    public:
        S2FSObject(uint32_t size)
        : size(size) {}
        ~S2FSObject();

        inline int Lock() { return pthread_mutex_lock(&mutex); }
        inline int Unlock() { return pthread_mutex_unlock(&mutex); }

        virtual char *Serialize() = 0;  // for M5
    };

    class S2FSSegment : public S2FSObject
    {
    private:
        uint64_t addr_start;        // should be aligned to zone_no
        std::unordered_map<uint32_t, uint64_t> inode_map;
    public:
        S2FSSegment(/* args */);
        ~S2FSSegment();

        int Flush();
        int OnGC();
    };

    class S2FSINode : public S2FSObject
    {
    private:
        uint32_t id;
        std::list<uint64_t> offsets;
    public:
        S2FSINode(/* args */);
        ~S2FSINode();
    };
}

#endif