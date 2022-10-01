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
    #define addr_2_block(addr)    ((addr / S2FSBlock::Size()) % S2FSSegment::Size())
    #define addr_2_inseg_offset(addr)    (addr % S2FSSegment::Size())
    #define block_2_inseg_offset(bloc)    (bloc * S2FSBlock::Size())

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
        pthread_rwlock_t _rwlock = PTHREAD_RWLOCK_INITIALIZER;
    public:
        static S2FileSystem *_fs;
        S2FSObject(){}
        ~S2FSObject(){}

        inline int ReadLock() { return pthread_rwlock_rdlock(&_rwlock); }
        inline int WriteLock() { return pthread_rwlock_wrlock(&_rwlock); }
        inline int Unlock() { return pthread_rwlock_unlock(&_rwlock); }

        // Need to free the return value
        virtual char *Serialize() = 0;                  // for M5
        virtual void Deserialize(char *buffer) = 0;     // for M5
    };

    class S2FSFileAttr : public S2FSObject
    {
    private:
        std::string _name;
        uint64_t _size;
        uint64_t _create_time;
        bool _is_dir;
        uint64_t _offset;
        uint32_t _inode_id;
        // ...
    public:
        S2FSFileAttr(/* args */){}
        ~S2FSFileAttr(){}

        // Need to free the return value
        char *Serialize(){}                  // for M5
        void Deserialize(char *buffer){}     // for M5

        inline std::string Name() { return _name; }
        inline uint64_t Offset() { return _offset; }
    };

    class S2FSBlock : public S2FSObject
    {
    private:
        // Only valid for ITYPE_INODE types. Indicating next INode global offset
        uint64_t _next;
        // Only valid for ITYPE_INODE types. Indicating previous INode global offset
        uint64_t _prev;
        std::string _name;
        uint32_t _id;
        INodeType _type;
        // Only valid for ITYPE_INODE types.
        // Global offsets of data blocks
        std::list<uint64_t> _offsets;
        // Only valid for ITYPE_DIR_DATA types.
        std::list<S2FSFileAttr*> _file_attrs;
        // Only valid for ITYPE_FILE_DATA types.
        char *_content;
    public:
        S2FSBlock(INodeType type) : _id(id_alloc++), _type(type) {}
        S2FSBlock(){}
        ~S2FSBlock(){}

        // Need to free the return value
        char *Serialize(){}                  // for M5
        void Deserialize(char *buffer){}     // for M5

        inline void AddOffset(uint64_t offset) { _offsets.push_back(offset); }
        S2FSBlock *DirectoryLookUp(std::string &name);
        inline uint32_t ID() { return _id; }
        inline std::list<S2FSFileAttr*> &FileAttrs() { return _file_attrs; }

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

        // Need to free the return value
        char *Serialize();                  // for M5
        void Deserialize(char *buffer);     // for M5

        // return: in-segment offset
        uint64_t GetEmptyBlock();
        uint64_t GetEmptyBlockNum();
        
        // Get block by in-segment offset
        // No lock ops in this function, not thread safe
        // Make sure to use WriteLock() before calling this
        S2FSBlock *GetBlockByOffset(uint64_t offset);
        S2FSBlock *LookUp(const std::string &name);
        uint64_t Allocate(const std::string &name, INodeType type, uint64_t size, S2FSBlock **res);
        int Free();
        int Read();
        int Write();
        int Flush();
        int OnGC();
        inline uint64_t Addr() { return _addr_start; }

        static uint64_t Size();
    };
}

#endif