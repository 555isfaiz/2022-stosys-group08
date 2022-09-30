#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{
    S2FileSystem *S2FSObject::_fs;

    uint64_t S2FSBlock::Size() { return _fs->_zns_dev->lba_size_bytes; }

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
                return block_2_addr(i);
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

    S2FSBlock *S2FSSegment::LookUp(const std::string &name)
    {
        if (map_contains(_name_2_inode, name))
        {
            auto addr = _inode_map.at(_name_2_inode.at(name));
            return _blocks.at(addr / _fs->_zns_dev->lba_size_bytes);
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
                std::cout << "Error: allocating data block for not existing file: " << name << "\n";
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
                inode = new S2FSBlock;
                uint64_t empty = GetEmptyBlock();
                _blocks[addr_2_block(empty)] = inode;
            }
        }
        else
        {
            std::cout << "Error: allocating unknown block type: " << type << "\n";
        }

        if (inode)
        {
            *res = inode;
            uint64_t empty = GetEmptyBlock();
            while (empty && allocated < size)
            {
                inode->AddOffset(empty);
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