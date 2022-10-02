#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{
    S2FileSystem *S2FSObject::_fs;

    void S2FSFileAttr::Serialize(char *buffer)
    {
        strcpy(buffer, _name.c_str());
        uint64_t ptr = MAX_NAME_LENGTH;
        // Use the highest bit of size to indicate whether it is a directory or not
        *(uint64_t *)(buffer + ptr) = _is_dir ? (_size & 1 << 63) : _size;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _create_time;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _offset;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _inode_id;
    }

    void S2FSFileAttr::Deserialize(char *buffer)
    {
        uint64_t ptr = MAX_NAME_LENGTH;
        Name(std::string(buffer, MAX_NAME_LENGTH))
        ->IsDir(*(uint64_t *)(buffer + ptr) & (1 << 63))
        ->Size(*(uint64_t *)(buffer + ptr) | (1 << 63))
        ->CreateTime(*(uint64_t *)(buffer + (ptr += sizeof(uint64_t))))
        ->Offset(*(uint64_t *)(buffer + (ptr += sizeof(uint64_t))))
        ->InodeID(*(uint64_t *)(buffer + (ptr += sizeof(uint64_t))));
    }
}