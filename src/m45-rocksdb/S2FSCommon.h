#ifndef STOSYS_PROJECT_S2FILESYSTEM_COMMON_H
#define STOSYS_PROJECT_S2FILESYSTEM_COMMON_H

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "S2FileSystem.h"

#include <list>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <pthread.h>
#include <zns_device.h>

namespace ROCKSDB_NAMESPACE
{

    #define MAX_NAME_LENGTH 32
    #define map_contains(map, key)  (map.find(key) != map.end())
    #define addr_2_segment(addr)    (addr / S2FSSegment::Size())
    #define segment_2_addr(segm)    (segm * S2FSSegment::Size())
    #define addr_2_block(addr)    (addr / S2FSBlock::Size())
    #define block_2_addr(bloc)    (bloc * S2FSBlock::Size())

    static std::atomic_int32_t id_alloc(0);

    enum INodeType
    {
        ITYPE_FILE_INODE,
        ITYPE_FILE_DATA,
        ITYPE_DIR_INODE,
        ITYPE_DIR_DATA
    };

    class S2FileSystem;

    class S2FSObject
    {
    private:
        pthread_mutex_t _mutex = PTHREAD_MUTEX_INITIALIZER;
    public:
        static S2FileSystem *_fs;
        S2FSObject(){}
        ~S2FSObject(){}

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
        S2FSFileAttr(/* args */){}
        ~S2FSFileAttr(){}

        char *Serialize();                  // for M5
        void Deserialize(char *buffer);     // for M5
    };

    class S2FSBlock : public S2FSObject
    {
    private:
        // Only valid for INode types. Indicating next INode global offset
        uint64_t _next;
        // Only valid for INode types. Indicating previous INode global offset
        uint64_t _prev;
        std::string _name;
        uint32_t _id;
        INodeType _type;
        std::list<uint64_t> _offsets;
        std::list<S2FSFileAttr*> _file_attrs;
        char *_content;
    public:
        S2FSBlock(/* args */){}
        ~S2FSBlock(){}

        char *Serialize(){}                  // for M5
        void Deserialize(char *buffer){}     // for M5

        inline void AddOffset(uint64_t offset) { _offsets.push_back(offset); }

        static uint64_t Size();
    };

    class S2FSSegment : public S2FSObject
    {
    private:
        // should be aligned to zone_no
        uint64_t _addr_start;
        /* k: inode id, v: in-segment offset*/
        std::unordered_map<uint32_t, uint64_t> _inode_map;
        /* k: name, v: inode id*/
        std::unordered_map<std::string, uint32_t> _name_2_inode;
        std::vector<S2FSBlock*> _blocks;
    public:
        S2FSSegment(uint64_t addr);
        ~S2FSSegment(){}

        char *Serialize();                  // for M5
        void Deserialize(char *buffer);     // for M5

        // return: in-segment offset
        uint64_t GetEmptyBlock();
        uint64_t GetEmptyBlockNum();
        S2FSBlock *LookUp(const std::string &name);
        uint64_t Allocate(const std::string &name, INodeType type, uint64_t size, S2FSBlock **res);
        int Free();
        int Read();
        int Write();
        int Flush();
        int OnGC();

        static uint64_t Size();
    };
}

#endif