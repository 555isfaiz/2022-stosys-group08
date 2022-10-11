#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{
    S2FileSystem *S2FSObject::_fs;

    uint64_t S2FSFileAttr::Serialize(char *buffer)
    {
        strcpy(buffer, _name.c_str());
        uint64_t ptr = MAX_NAME_LENGTH;
        // Use the highest bit of size to indicate whether it is a directory or not
        *(uint64_t *)(buffer + ptr) = _is_dir ? (_size & (uint64_t)1 << 63) : _size;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _create_time;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _offset;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _inode_id;
        return FILE_ATTR_SIZE;
    }

    uint64_t S2FSFileAttr::Deserialize(char *buffer)
    {
        uint64_t ptr = MAX_NAME_LENGTH;
        uint32_t str_len = strlen(buffer);
        Name(std::string(buffer, (str_len < MAX_NAME_LENGTH ? str_len : MAX_NAME_LENGTH)));
        if (Name().size() == 0)
            return 0;
        IsDir(*(uint64_t *)(buffer + ptr) & ((uint64_t)1 << 63))
        ->Size(*(uint64_t *)(buffer + ptr) | ((uint64_t)1 << 63))
        ->CreateTime(*(uint64_t *)(buffer + (ptr += sizeof(uint64_t))))
        ->Offset(*(uint64_t *)(buffer + (ptr += sizeof(uint64_t))))
        ->InodeID(*(uint64_t *)(buffer + (ptr += sizeof(uint64_t))));
        return FILE_ATTR_SIZE;
    }
}