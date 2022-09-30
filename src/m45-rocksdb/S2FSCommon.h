#ifndef STOSYS_PROJECT_S2FILESYSTEM_COMMON_H
#define STOSYS_PROJECT_S2FILESYSTEM_COMMON_H

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"

#include <list>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <pthread.h>
#include <zns_device.h>

namespace ROCKSDB_NAMESPACE
{
    static std::atomic_int32_t id_alloc(0);

    enum INodeType
    {
        ITYPE_FILE_INODE,
        ITYPE_FILE_DATA,
        ITYPE_DIR_INODE,
        ITYPE_DIR_DATA
    };

    class S2FSObject
    {
    private:
        uint32_t _size;
        pthread_mutex_t _mutex = PTHREAD_MUTEX_INITIALIZER;
    public:
        S2FSObject(uint32_t size)
        : _size(size) {}
        ~S2FSObject();

        inline int Lock() { return pthread_mutex_lock(&_mutex); }
        inline int Unlock() { return pthread_mutex_unlock(&_mutex); }

        virtual char *Serialize() = 0;                  // for M5
        virtual void Deserialize(char *buffer) = 0;     // for M5
    };

    class S2FSFileAttr : public S2FSObject
    {
    private:
        std::string _name;
        uint64_t _size;
        uint64_t _create_time;
        // ...
    public:
        S2FSFileAttr(/* args */);
        ~S2FSFileAttr();
    };

    class S2FSBlock : public S2FSObject
    {
    private:
        uint32_t _id;
        INodeType _type;
        std::list<uint64_t> _offsets;
        std::list<S2FSFileAttr> _file_attrs;
        char *content;
    public:
        S2FSBlock(/* args */);
        ~S2FSBlock();
    };

    class S2FSSegment : public S2FSObject
    {
    private:
        struct user_zns_device * _zns_dev;
        uint64_t _addr_start;        // should be aligned to zone_no
        std::unordered_map<uint32_t, uint64_t> _inode_map;
        std::vector<S2FSBlock> _blocks;
    public:
        S2FSSegment(struct user_zns_device * zns_dev, uint32_t size, uint64_t addr);
        ~S2FSSegment();

        int Allocate();
        int Free();
        int Read();
        int Write();
        int Flush();
        int OnGC();
    };
}

#endif