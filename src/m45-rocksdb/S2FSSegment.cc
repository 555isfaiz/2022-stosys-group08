#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{
    S2FSSegment::S2FSSegment(uint64_t addr)
    : _addr_start(addr),
    _cur_size(S2FSBlock::Size())
    {
        // hope this would always be 1...
        _reserve_for_inode = INODE_MAP_ENTRY_LENGTH * _fs->_zns_dev_ex->blocks_per_zone / 2 / S2FSBlock::Size() + 1;
    }

    uint64_t S2FSSegment::Size() { return _fs->_zns_dev->lba_size_bytes * _fs->_zns_dev_ex->blocks_per_zone; }

    S2FSSegment::~S2FSSegment()
    {
        for (auto p : _blocks)
        {
            if (!p.second || p.second == (S2FSBlock *)1)
                continue;
            delete p.second;
        }
    }

    S2FSBlock *S2FSSegment::GetBlockByOffset(uint64_t offset)
    {
        if (!map_contains(_blocks, offset))
            return NULL;

        S2FSBlock *block = NULL;
        block = _blocks.at(offset);
        // block is occupied, but not present in memory
        if ((uint64_t)block == 1)
        {
            block = new S2FSBlock;
            _blocks[offset] = block;
        }

        if (block && !block->Loaded())
        {
            auto actual_size = round_up(block->ActualSize(), S2FSBlock::Size());
            char buf[actual_size] = {0};
            int ret = zns_udevice_read(_fs->_zns_dev, offset + _addr_start, buf, actual_size);
            if (ret)
            {
                std::cout << "Error: reading block at: " << offset + _addr_start << " during S2FSSegment::LookUp."
                          << "\n";
            }
            else
            {
                block->Deserialize(buf);
                if (block->Type() == ITYPE_DIR_INODE)
                    _name_2_inode[block->Name()] = block->ID();
            }
        }

        return block;
    }

    S2FSBlock *S2FSSegment::LookUp(const std::string &name)
    {
        if (map_contains(_name_2_inode, name))
        {
            auto addr = _inode_map.at(_name_2_inode.at(name));
            return GetBlockByOffset(addr);
        }

        _fs->LoadSegmentFromDisk(_addr_start);
        if (map_contains(_name_2_inode, name))
        {
            auto addr = _inode_map.at(_name_2_inode.at(name));
            return GetBlockByOffset(addr);
        }

        return NULL;
    }

    struct FlushArgs
    {
        S2FSSegment *s;
        uint64_t off = 0;
    };

    void *FlushWarpper(void *arg)
    {
        FlushArgs *aarg = (FlushArgs *)arg;
        aarg->s->ReadLock();
        aarg->s->Flush(aarg->off);
        aarg->s->Unlock();
        free(arg);
        return (void *)NULL;
    }

    int64_t S2FSSegment::AllocateNew(const std::string &name, INodeType type, S2FSBlock **res, S2FSBlock *parent_dir)
    {
        if (name.length() > MAX_NAME_LENGTH)
            return -1;

        if (type != ITYPE_DIR_INODE && type != ITYPE_FILE_INODE)
        {
            std::cout << "Error: allocating new for unknown block type: " << type << " during S2FSSegment::AllocateNew." << "\n";
            return -1;
        }

        if (_cur_size + S2FSBlock::Size() >= S2FSSegment::Size())
        {
            return -1;
        }

        WriteLock();
        uint64_t allocated = 0;
        S2FSBlock *inode;
        INodeType data_type;
        S2FSFileAttr fa;

        inode = new S2FSBlock(type, _addr_start, 0);
        inode->GlobalOffset(_addr_start + _cur_size);
        _blocks[_cur_size] = inode;
        _name_2_inode[name] = inode->ID();
        _inode_map[inode->ID()] = _cur_size;
        _cur_size += S2FSBlock::Size();

        if (type == ITYPE_DIR_INODE)
            inode->Name(name);

        if (parent_dir)
        {
            fa.Name(name)
                ->CreateTime(0)
                ->IsDir(type == ITYPE_DIR_INODE)
                ->InodeID(inode->ID())
                ->Offset(inode->GlobalOffset())
                ->Size(0);
        }

        *res = inode;

        Unlock();
        FlushArgs *args = (FlushArgs *)calloc(1, sizeof(struct FlushArgs));
        args->s = this;
        args->off = inode->GlobalOffset() - _addr_start;
        pool_exec(_fs->_thread_pool, FlushWarpper, args);
        // Flush(inode->GlobalOffset() - _addr_start);

        // Do this after releasing the lock, otherwise deadlock happens
        if (inode && parent_dir)
            parent_dir->DirectoryAppend(fa);
        return allocated;
    }

    int64_t S2FSSegment::AllocateData(uint64_t inode_id, INodeType type, const char *data, uint64_t size, S2FSBlock **res)
    {
        if (!map_contains(_inode_map, inode_id))
        {
            return -1;
        }

        if (_cur_size + 9>= S2FSSegment::Size())
        {
            return -1;
        }

        WriteLock();
        auto inode = GetBlockByOffset(_inode_map[inode_id]);
        uint64_t to_copy = (size > (S2FSSegment::Size() - _cur_size - 9) ? (S2FSSegment::Size() - _cur_size - 9) : size);
        inode->AddOffset(_cur_size + Addr());
        S2FSBlock *data_block = new S2FSBlock(type, _addr_start, to_copy);
        data_block->GlobalOffset(_addr_start + _cur_size);
        _blocks[_cur_size] = data_block;
        if (data)
        {
            memcpy(data_block->Content(), data, to_copy);
        }
        _cur_size += to_copy + 9;
        *res = data_block;

        Unlock();
        FlushArgs *args = (FlushArgs *)calloc(1, sizeof(struct FlushArgs));
        args->s = this;
        args->off = data_block->GlobalOffset() - _addr_start;
        pool_exec(_fs->_thread_pool, FlushWarpper, args);
        // Flush(data_block->GlobalOffset() - _addr_start);
        return to_copy;
    }

    int S2FSSegment::RemoveINode(uint64_t inode_id)
    {
        _blocks[_inode_map[inode_id]] = NULL;
        _inode_map.erase(inode_id);
        for (auto p : _name_2_inode)
        {
            if (p.second == inode_id)
            {
                _name_2_inode.erase(p.first);
                break;
            }
        }
        return 0;
    }

    void S2FSSegment::OnRename(const std::string &src, const std::string &target)
    {
        if (!map_contains(_name_2_inode, src))
            return;

        auto id = _name_2_inode.at(src);
        _name_2_inode.erase(src);
        _name_2_inode[target] = id;
    }

    int S2FSSegment::Free(uint64_t inode_id) 
    {
        if (!map_contains(_inode_map, inode_id))
        {
            std::cout << "Error: freeing non-existing inode id: " << inode_id << " during S2FSSegment::Free." << "\n";
            return -1;
        }
        WriteLock();
        std::list<S2FSBlock *> inodes;
        std::list<S2FSSegment *> segments;
        uint64_t off = _inode_map.at(inode_id);
        auto inode = GetBlockByOffset(off);
        auto segment = this;
        segments.push_back(this);
        inodes.push_back(inode);
        while (inode->Next())
        {
            segment = _fs->ReadSegment(addr_2_segment(inode->Next()));
            segment->WriteLock();
            inode = segment->GetBlockByOffset(addr_2_inseg_offset(inode->Next()));
            inode->WriteLock();
            segments.push_back(segment);
            inodes.push_back(inode);
        }

        while (!inodes.empty())
        {
            inode = inodes.back();
            segment = segments.back();
            segment->RemoveINode(inode->ID());
            segment->Flush();
            segment->Unlock();
            inode->Unlock();
            delete inode;
            inodes.pop_back();
            segments.pop_back();
        }

        return 0;
    }

    int S2FSSegment::Flush()
    {
        char buf[S2FSSegment::Size()] = {0};
        uint32_t off = 0, size = _reserve_for_inode * S2FSBlock::Size();
        for (auto iter = _inode_map.begin(); iter != _inode_map.end() && off < size; off += INODE_MAP_ENTRY_LENGTH, iter++)
        {
            *(uint64_t *)(buf + off) = iter->first;
            *(uint64_t *)(buf + off + 8) = iter->second;
        }

        for (auto p : _blocks)
        {
            if (!p.second || p.second == (S2FSBlock *)1)
                continue;
            
            size += p.second->Serialize(buf + size);
        }

        size = round_up(size, S2FSBlock::Size());
        for (size_t i = _addr_start; i < size; i += S2FSBlock::Size())
        {
            int ret = zns_udevice_write(_fs->_zns_dev, i, buf + i - _addr_start, S2FSBlock::Size());
            if (ret)
            {
                std::cout << "Error: nvme write error at: " << i << " ret:" << ret << " during S2FSSegment::Flush."
                        << "\n";
                return -1;
            }
        }
        return 0;
    }

    int S2FSSegment::Flush(uint64_t in_seg_off)
    {
        uint64_t write_start = in_seg_off / S2FSBlock::Size() * S2FSBlock::Size(), write_end = round_up(in_seg_off + _blocks[in_seg_off]->ActualSize(), S2FSBlock::Size());
        uint64_t ptr = 0, total_size = write_end - write_start;
        char buf[total_size] = {0};
        char tmp[S2FSSegment::Size()] = {0};
        auto iter = _blocks.find(in_seg_off);
        while (iter->first > write_start) 
            iter--;
        while (iter->first < write_end && iter != _blocks.end())
        {
            if (!iter->second)
            {
                auto itert = iter;
                itert++;
                ptr += (itert->first - iter->first);
                iter++;
                continue;
            }

            auto len = iter->second->Serialize(tmp);
            uint64_t skip = (write_start > iter->first ? write_start - iter->first : 0);
            uint64_t drop = (write_end < iter->first + len ? iter->first + len - write_end : 0);
            memcpy(buf + ptr, tmp + skip, len - skip - drop);
            ptr += len - skip - drop;
            iter++;
        }

        for (size_t i = write_start; i < write_end; i += S2FSBlock::Size())
        {
            int ret = zns_udevice_write(_fs->_zns_dev, i, buf + i - write_start, S2FSBlock::Size());
            if (ret)
            {
                std::cout << "Error: nvme write error at: " << i << " ret:" << ret << " during S2FSSegment::Flush."
                        << "\n";
                return -1;
            }
        }
        return write_end - write_start;
    }

    int S2FSSegment::Offload()
    {
        Flush();
        for (auto p : _blocks)
        {
            if (!p.second || p.second == (S2FSBlock *)1)
                continue;
            
            int ret = p.second->Offload();
            if (ret)
                return ret;
        }
        return 0;
    }

    int S2FSSegment::OnGC()
    {
        WriteLock();

//         _fs->LoadSegmentFromDisk(_addr_start);
//         std::unordered_map<uint64_t, S2FSBlock *> blocks;
//         uint64_t ptr = _reserve_for_inode;
//         bool changed = false;
//         for (size_t i = _reserve_for_inode; i < _blocks.size(); i++)
//         {
//             auto block = _blocks[i];
//             if (!block || block == (S2FSBlock *)1)
//                 continue;

//             blocks[ptr] = block;
//             if (ptr != i)
//                 changed = true;
            
//             uint64_t new_inseg_off = block_2_inseg_offset(ptr);
//             ptr++;
//             if (map_contains(_inode_map, block->ID()))
//             {
//                 _inode_map[block->ID()] = new_inseg_off;
//             }

//             block->WriteLock();
//             // Update _prev and _next for dir/file inodes if needed...
//             if (block->Type() == ITYPE_DIR_INODE || block->Type() == ITYPE_FILE_INODE)
//             {
//                 if (block->Next())
//                 {
//                     auto s = _fs->ReadSegment(addr_2_segment(block->Next()));
//                     s->WriteLock();
//                     auto next_block = s->GetBlockByOffset(addr_2_inseg_offset(block->Next()));
//                     next_block->WriteLock();
//                     next_block->Prev(_addr_start + new_inseg_off);
//                     next_block->Unlock();
//                     s->Unlock();
//                 }

//                 if (block->Prev())
//                 {
//                     auto s = _fs->ReadSegment(addr_2_segment(block->Next()));
//                     s->WriteLock();
//                     auto prev_block = s->GetBlockByOffset(addr_2_inseg_offset(block->Prev()));
//                     prev_block->WriteLock();
//                     prev_block->Next(_addr_start + new_inseg_off);
//                     prev_block->Unlock();
//                     s->Unlock();
//                 }
//             }

//             // change inode->_offsets if needed...
//             uint64_t old_off = _addr_start + block_2_inseg_offset(i);
//             for (auto p : _blocks)
//             {
//                 auto b = p.second;
//                 if (b == block)
//                     continue;
                
//                 b->WriteLock();
//                 auto offsets = b->Offsets();
//                 for (auto iter = offsets.begin(); iter != offsets.end(); iter++)
//                 {
//                     if (*iter == old_off)
//                     {
//                         offsets.erase(iter);
//                         offsets.insert(iter, _addr_start + new_inseg_off);
//                         b->Unlock();
//                         goto done;
//                     }
//                 }
//                 b->Unlock();
//             }
// done:
//             block->Unlock();
//         }

//         if (changed)
//         {
//             _blocks = blocks;
//             Flush();
//         }

        Offload();
        Unlock();
        return 0;
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

    uint64_t S2FSSegment::Serialize(char *buffer)
    {
        ReadLock();
        uint32_t off = 0, size = _reserve_for_inode * S2FSBlock::Size();
        for (auto iter = _inode_map.begin(); iter != _inode_map.end() && off < size; off += INODE_MAP_ENTRY_LENGTH, iter++)
        {
            *(uint64_t *)(buffer + off) = iter->first;
            *(uint64_t *)(buffer + off + 8) = iter->second;
        }

        uint64_t ptr = size;
        for (auto p : _blocks)
        {
            if (!p.second || p.second == (S2FSBlock *)1)
                continue;
            ptr += p.second->Serialize(buffer + ptr);
        }
        Unlock();
        return S2FSSegment::Size();
    }

    uint64_t S2FSSegment::Deserialize(char *buffer)
    {
        WriteLock();

        Preload(buffer);

        uint64_t ptr = _reserve_for_inode * S2FSBlock::Size();
        for (size_t i = _reserve_for_inode; i < _blocks.size(); i++)
        {
            S2FSBlock *block;
            if (_blocks.at(i))
                block = _blocks.at(i);
            else
                break;

            block->WriteLock();
            ptr += block->Deserialize(buffer + ptr);
            block->Unlock();

            if (block->Type() == 0)
                delete block;
            else
            {
                block->SegmentAddr(_addr_start);
                block->GlobalOffset(_addr_start + ptr);
                _blocks[ptr] = block;
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
        _cur_size = ptr;
        Unlock();
        return S2FSSegment::Size();
    }
}
