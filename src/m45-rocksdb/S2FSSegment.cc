#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{
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
            char buf[S2FSBlock::Size()] = {0};
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

                if (block->Type() == ITYPE_DIR_INODE)
                    _name_2_inode[block->Name()] = block->ID();
            }
        }
        return block;
    }

    S2FSBlock *S2FSSegment::LookUp(const std::string &name)
    {
        WriteLock();
        if (map_contains(_name_2_inode, name))
        {
            auto addr = _inode_map.at(_name_2_inode.at(name));
            Unlock();
            return GetBlockByOffset(addr);
        }

        _fs->LoadSegmentFromDisk(_addr_start);
        if (map_contains(_name_2_inode, name))
        {
            auto addr = _inode_map.at(_name_2_inode.at(name));
            Unlock();
            return GetBlockByOffset(addr);
        }

        Unlock();
        return NULL;
    }

    uint64_t S2FSSegment::AllocateNew(const std::string &name, INodeType type, const char *data, uint64_t size, S2FSBlock **res, S2FSBlock *parent_dir)
    {
        if (name.length() > MAX_NAME_LENGTH)
            return 0;

        WriteLock();
        uint64_t allocated = 0;
        S2FSBlock *inode;
        INodeType data_type;
        if (type == ITYPE_DIR_INODE || type == ITYPE_FILE_INODE)
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
                
                data_type = (type == ITYPE_DIR_INODE ? ITYPE_DIR_DATA : ITYPE_FILE_DATA);
            }
        }
        else
        {
            std::cout << "Error: allocating new for unknown block type: " << type << " during S2FSSegment::Allocate." << "\n";
        }

        if (inode)
        {
            *res = inode;
            uint64_t to_allocate = round_up(size, S2FSBlock::MaxDataSize(inode->Type()));
            uint64_t empty = GetEmptyBlock();
            while (empty && allocated < to_allocate)
            {
                inode->AddOffset(empty + Addr());
                S2FSBlock *data = new S2FSBlock(type, _addr_start);
                _blocks[addr_2_block(empty)] = data;
                uint64_t to_copy = (S2FSBlock::MaxDataSize(inode->Type()) > size - allocated ? size - allocated : S2FSBlock::MaxDataSize(inode->Type()));
                if (data)
                {
                    memcpy(data->Content(), data + allocated, to_copy);
                }
                allocated += to_copy;
                empty = GetEmptyBlock();
            }
        }
        
        // Flush();

        Unlock();
        return allocated;
    }

    uint64_t S2FSSegment::AllocateData(uint64_t inode_id, INodeType type, const char *data, uint64_t size, S2FSBlock **res)
    {
        WriteLock();
        if (!map_contains(_inode_map, inode_id))
        {
            return 0;
        }

        uint64_t allocated = 0;
        auto inode = GetBlockByOffset(_inode_map[inode_id]);
        uint64_t to_allocate = round_up(size, S2FSBlock::MaxDataSize(inode->Type()));
        uint64_t empty = GetEmptyBlock();
        while (empty && allocated < to_allocate)
        {
            inode->AddOffset(empty + Addr());
            S2FSBlock *data = new S2FSBlock(type, _addr_start);
            _blocks[addr_2_block(empty)] = data;
            uint64_t to_copy = (S2FSBlock::MaxDataSize(inode->Type()) > size - allocated ? size - allocated : S2FSBlock::MaxDataSize(inode->Type()));
            if (data)
            {
                memcpy(data->Content(), data + allocated, to_copy);
            }
            allocated += to_copy;
            empty = GetEmptyBlock();
        }

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
            _blocks[addr_2_block(offset)] = (S2FSBlock *)1;
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
