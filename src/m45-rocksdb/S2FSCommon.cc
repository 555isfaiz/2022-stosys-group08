#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{
    S2FileSystem *S2FSObject::_fs;

    uint64_t S2FSBlock::Size() { return _fs->_zns_dev->lba_size_bytes; }

    S2FSBlock *S2FSBlock::DirectoryLookUp(std::string &name)
    {
        if (_type != ITYPE_DIR_INODE)
        {
            std::cout << "Error: looking up dir in a non-dir inode type: " << _type << " name: " << name << " during S2FSBlock::DirectoryLookUp."
                          << "\n";
            return NULL;
        }

        for (auto off : _offsets)
        {
            S2FSBlock *data;
            auto s = _fs->ReadSegmentFromCache(addr_2_segment(off));
            if (s)
            {
                data = s->GetBlockByOffset(off);
            }
            else
            {
                data = new S2FSBlock(ITYPE_DIR_DATA);
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

            for (auto attr : _file_attrs)
            {
                if (attr->Name() == name)
                {
                    auto s = _fs->ReadSegment(addr_2_segment(attr->Offset()));
                    return s->GetBlockByOffset(addr_2_inseg_offset(attr->Offset()));
                }
            }
        }
        return NULL;
    }

    S2FSSegment::S2FSSegment(uint64_t addr)
    : _addr_start(addr)
    {
        _blocks.resize(_fs->_zns_dev_ex->blocks_per_zone);
    }

    uint64_t S2FSSegment::Size() { return _fs->_zns_dev->lba_size_bytes * _fs->_zns_dev_ex->blocks_per_zone; }

    uint64_t S2FSSegment::GetEmptyBlock()
    {
        // skip the first one. that's for inode map
        for (size_t i = 1; i < _blocks.size(); i++)
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
        for (size_t i = 1; i < _blocks.size(); i++)
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
        Lock();
        block = _blocks.at(addr_2_block(offset));
        // block is occupied, but not present in memory
        if ((uint64_t)block == 1)
        {
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
        Unlock();
        return block;
    }

    S2FSBlock *S2FSSegment::LookUp(const std::string &name)
    {
        if (map_contains(_name_2_inode, name))
        {
            auto addr = _inode_map.at(_name_2_inode.at(name));
            return GetBlockByOffset(addr);
        }
        return NULL;
    }

    uint64_t S2FSSegment::Allocate(const std::string &name, INodeType type, uint64_t size, S2FSBlock **res)
    {
        Lock();
        uint64_t allocated = 0;
        S2FSBlock *inode;
        if (type == ITYPE_DIR_DATA || type == ITYPE_FILE_DATA)
        {
            if (!map_contains(_name_2_inode, name))
            {
                std::cout << "Error: allocating data block for not existing file: " << name << " during S2FSSegment::Allocate." << "\n";
                Unlock();
                return NULL;
            }

            S2FSBlock *inode = _blocks.at(_inode_map.at(_name_2_inode.at(name)) / S2FSBlock::Size());
        }
        else if (type == ITYPE_DIR_INODE || type == ITYPE_FILE_INODE)
        {
            // need at least 2 blocks. one for inode, one for data.
            if (GetEmptyBlockNum() >= 2)
            {
                inode = new S2FSBlock(type);
                uint64_t empty = GetEmptyBlock();
                _blocks[addr_2_block(empty)] = inode;
                _name_2_inode[name] = inode->ID();
                _inode_map[inode->ID()] = empty;
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

    char *S2FSSegment::Serialize()                  
    {

    }

    void S2FSSegment::Deserialize(char *buffer)
    {

    }
}