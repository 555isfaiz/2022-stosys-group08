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

    uint64_t S2FSBlock::Size() { return _fs->_zns_dev->lba_size_bytes; }

    S2FSBlock *S2FSBlock::DirectoryLookUp(std::string &name)
    {
        if (_type != ITYPE_DIR_INODE)
        {
            std::cout << "Error: looking up dir in a non-dir inode type: " << _type << " name: " << name << " during S2FSBlock::DirectoryLookUp."
                          << "\n";
            return NULL;
        }

        ReadLock();
        for (auto off : _offsets)
        {
            S2FSBlock *data;
            bool need_lock = false;
            auto s = _fs->ReadSegmentFromCache(addr_2_segment(off));
            if (s)
            {
                s->WriteLock();
                need_lock = true;
                data = s->GetBlockByOffset(off);
            }
            else
            {
                data = new S2FSBlock;
                char *buf = (char *)calloc(S2FSBlock::Size(), sizeof(char));
                int ret = zns_udevice_read(_fs->_zns_dev, off, buf, S2FSBlock::Size());
                if (ret)
                {
                    std::cout << "Error: reading block at: " << off << " during S2FSBlock::DirectoryLookUp."
                          << "\n";
                }
                data->Deserialize(buf);
                free(buf);
            }

            if (need_lock)
            {
                data->ReadLock();
            }

            for (auto attr : data->FileAttrs())
            {
                if (attr->Name() == name)
                {
                    auto s = _fs->ReadSegment(addr_2_segment(attr->Offset()));
                    return s->LookUp(attr->Name());
                }
            }

            if (need_lock)
            {
                s->Unlock();
                data->Unlock();
            }
            delete data;
        }
        Unlock();
        return NULL;
    }

    void S2FSBlock::SerializeFileInode(char *buffer)
    {
        *buffer = _type << 4;
        uint64_t ptr = 0;
        *(uint64_t *)(buffer + (ptr += 1)) = _next;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _prev;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _id;
        ptr += sizeof(uint64_t);
        for (auto iter = _offsets.begin(); iter != _offsets.end(); iter++, ptr += sizeof(uint64_t))
        {
            *(uint64_t *)(buffer + ptr) = *iter;
        }
    }

    void S2FSBlock::DeserializeFileInode(char *buffer)
    {
        uint64_t ptr = 0;
        _next = *(uint64_t *)(buffer + (ptr += 1));
        _prev = *(uint64_t *)(buffer + (ptr += sizeof(uint64_t)));
        _id = *(uint64_t *)(buffer + (ptr += sizeof(uint64_t)));
        ptr += sizeof(uint64_t);
        while (*(uint64_t *)(buffer + ptr))
        {
            _offsets.push_back(*(uint64_t *)(buffer + ptr));
        }
    }

    void S2FSBlock::SerializeDirInode(char *buffer)
    {
        *buffer = _type << 4;
        uint64_t ptr = 0;
        *(uint64_t *)(buffer + (ptr += 1)) = _next;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _prev;
        *(uint64_t *)(buffer + (ptr += sizeof(uint64_t))) = _id;
        strcpy((buffer + (ptr += sizeof(uint64_t))), _name.c_str());
        ptr += MAX_NAME_LENGTH;
        for (auto iter = _offsets.begin(); iter != _offsets.end(); iter++, ptr += sizeof(uint64_t))
        {
            *(uint64_t *)(buffer + ptr) = *iter;
        }
    }

    void S2FSBlock::DeserializeDirInode(char *buffer)
    {
        uint64_t ptr = 0;
        _next = *(uint64_t *)(buffer + (ptr += 1));
        _prev = *(uint64_t *)(buffer + (ptr += sizeof(uint64_t)));
        _id = *(uint64_t *)(buffer + (ptr += sizeof(uint64_t)));
        _name = std::string(buffer + (ptr += sizeof(uint64_t)), MAX_NAME_LENGTH);
        ptr += MAX_NAME_LENGTH;
        while (*(uint64_t *)(buffer + ptr))
        {
            _offsets.push_back(*(uint64_t *)(buffer + ptr));
        }
    }

    void S2FSBlock::SerializeDirData(char *buffer)
    {
        *buffer = _type << 4;
        uint64_t ptr = 1;
        for (auto fa : _file_attrs)
        {
            fa->Serialize(buffer + ptr);
            ptr += FILE_ATTR_SIZE;
        }
    }

    void S2FSBlock::DeserializeDirData(char *buffer)
    {
        uint64_t ptr = 1;
        while (*(uint64_t *)(buffer + ptr))
        {
            S2FSFileAttr *fa = new S2FSFileAttr;
            fa->Deserialize(buffer + ptr);
            _file_attrs.push_back(fa);
        }
    }

    void S2FSBlock::Serialize(char *buffer)
    {
        switch (_type)
        {
        case ITYPE_DIR_INODE:
            SerializeDirInode(buffer);
            break;

        case ITYPE_DIR_DATA:
            SerializeDirData(buffer);
            break;

        case ITYPE_FILE_INODE:
            SerializeDirData(buffer);
            break;

        case ITYPE_FILE_DATA:
            *buffer = _type << 4;
            memcpy(buffer + 1, _content, Size() - 1);
            break;
        
        default:
            std::cout << "Error: unknown block type: " << _type << " during S2FSBlock::Serialize."<< "\n";
            break;
        }
    }

    void S2FSBlock::Deserialize(char *buffer)
    {
        char type = *buffer >> 4;
        switch (type)
        {
        case 1:
            _type = ITYPE_FILE_INODE;
            DeserializeFileInode(buffer);
            break;
        case 2:
            _type = ITYPE_FILE_DATA;
            _content = (char *)calloc(Size() - 1, sizeof(char));
            memcpy(_content, buffer + 1, Size() - 1);
            break;
        case 4:
            _type = ITYPE_DIR_INODE;
            DeserializeDirInode(buffer);
            break;
        case 8:
            _type = ITYPE_DIR_DATA;
            DeserializeDirData(buffer);
            break;
        case 0:
            _type = ITYPE_UNKNOWN;
            break;

        default:
            std::cout << "Error: dirty data, unknown block type: " << type << " during S2FSBlock::Deserialize."<< "\n";
            break;
        }
    }

    S2FSSegment::S2FSSegment(uint64_t addr)
    : _addr_start(addr)
    {
        _blocks.resize(_fs->_zns_dev_ex->blocks_per_zone);
        // hope this would always be 1...
        _reserve_for_inode = INODE_MAP_ENTRY_LENGTH * _fs->_zns_dev_ex->blocks_per_zone / 2 / S2FSBlock::Size() + 1;
    }

    uint64_t S2FSSegment::Size() { return _fs->_zns_dev->lba_size_bytes * _fs->_zns_dev_ex->blocks_per_zone; }

    uint64_t S2FSSegment::GetEmptyBlock()
    {
        // skip the first one. that's for inode map
        for (size_t i = _reserve_for_inode; i < _blocks.size(); i++)
        {
            if (_blocks.at(i) != NULL)
            {
                return block_2_inseg_offset(i);
            }
        }
        return 0;
    }

    uint64_t S2FSSegment::GetEmptyBlockNum()
    {
        uint64_t num = 0;
        // skip the first one. that's for inode map
        for (size_t i = _reserve_for_inode; i < _blocks.size(); i++)
        {
            if (_blocks.at(i) == NULL)
            {
                num++;
            }
        }
        return num;
    }

    S2FSBlock *S2FSSegment::GetBlockByOffset(uint64_t offset)
    {
        S2FSBlock *block = NULL;
        block = _blocks.at(addr_2_block(offset));
        // block is occupied, but not present in memory
        if ((uint64_t)block == 1)
        {
            // For now, this part seems will never be triggered
            char *buf = (char *)calloc(S2FSBlock::Size(), sizeof(char));
            int ret = zns_udevice_read(_fs->_zns_dev, offset + _addr_start, buf, S2FSBlock::Size());
            if (ret)
            {
                std::cout << "Error: reading block at: " << offset + _addr_start << " during S2FSSegment::LookUp."
                          << "\n";
            }
            else
            {
                S2FSBlock *block = new S2FSBlock;
                block->Deserialize(buf);
                _blocks[addr_2_block(offset)] = block;
            }
            free(buf);
        }
        return block;
    }

    S2FSBlock *S2FSSegment::LookUp(const std::string &name)
    {
        WriteLock();
        if (map_contains(_name_2_inode, name))
        {
            auto addr = _inode_map.at(_name_2_inode.at(name));
            return GetBlockByOffset(addr);
        }
        Unlock();
        return NULL;
    }

    uint64_t S2FSSegment::Allocate(const std::string &name, INodeType type, uint64_t size, S2FSBlock **res)
    {
        if (name.length() > MAX_NAME_LENGTH)
            return 0;

        WriteLock();
        uint64_t allocated = 0;
        S2FSBlock *inode;
        if (type == ITYPE_DIR_DATA || type == ITYPE_FILE_DATA)
        {
            if (!map_contains(_name_2_inode, name))
            {
                std::cout << "Error: allocating data block for not existing file: " << name << " during S2FSSegment::Allocate." << "\n";
                Unlock();
                return 0;
            }

            S2FSBlock *inode = _blocks.at(_inode_map.at(_name_2_inode.at(name)) / S2FSBlock::Size());
        }
        else if (type == ITYPE_DIR_INODE || type == ITYPE_FILE_INODE)
        {
            // need at least 2 blocks. one for inode, one for data.
            if (GetEmptyBlockNum() >= 2)
            {
                inode = new S2FSBlock(type, _addr_start);
                uint64_t empty = GetEmptyBlock();
                _blocks[addr_2_block(empty)] = inode;
                _name_2_inode[name] = inode->ID();
                _inode_map[inode->ID()] = empty;

                if (type == ITYPE_DIR_INODE)
                    inode->Name(name);
            }
        }
        else
        {
            std::cout << "Error: allocating unknown block type: " << type << " during S2FSSegment::Allocate." << "\n";
        }

        if (inode)
        {
            *res = inode;
            uint64_t empty = GetEmptyBlock();
            while (empty && allocated < size)
            {
                inode->AddOffset(empty + Addr());
                S2FSBlock *data = new S2FSBlock;
                _blocks[addr_2_block(empty)] = data;
                allocated += S2FSBlock::Size();
                empty = GetEmptyBlock();
            }
        }
        
        // Flush();

        Unlock();
        return allocated;
    }

    void S2FSSegment::Preload(char *buffer)
    {
        for (uint32_t c = 0; c < _reserve_for_inode * S2FSBlock::Size(); c += INODE_MAP_ENTRY_LENGTH)
        {
            uint64_t id = *(uint64_t *)(buffer + c);
            uint64_t offset = *(uint64_t *)(buffer + c + 8);
            if (!id && !offset)
                break;
            _inode_map[id] = offset;
        }
    }

    void S2FSSegment::Serialize(char *buffer)
    {
        ReadLock();
        uint32_t off = 0, size = _reserve_for_inode * S2FSBlock::Size();
        for (auto iter = _inode_map.begin(); iter != _inode_map.end() && off < size; off += INODE_MAP_ENTRY_LENGTH, iter++)
        {
            *(uint64_t *)(buffer + off) = iter->first;
            *(uint64_t *)(buffer + off + 8) = iter->second;
        }

        for (size_t i = _reserve_for_inode; i < _blocks.size(); i++)
        {
            _blocks.at(i)->Serialize(buffer + i * S2FSBlock::Size());
        }
        Unlock();
    }

    void S2FSSegment::Deserialize(char *buffer)
    {
        WriteLock();

        Preload(buffer);

        for (size_t i = _reserve_for_inode; i < _blocks.size(); i++)
        {
            S2FSBlock *block = new S2FSBlock;
            block->Deserialize(buffer + i * S2FSBlock::Size());

            if (block->Type() == 0)
                delete block;
            else
            {
                block->SegmentAddr(_addr_start);
                _blocks[i] = block;
                if (block->Type() == ITYPE_DIR_DATA)
                {
                    for (auto fa : block->FileAttrs())
                    {
                        if (fa->Offset() < _addr_start + Size() && fa->Offset() > _addr_start)
                        {
                            _name_2_inode[fa->Name()] = fa->InodeID();
                        }
                    }
                }
                else if (block->Type() == ITYPE_DIR_INODE)
                {
                    _name_2_inode[block->Name()] = block->ID();
                }
            }
        }
        Unlock();
    }
}