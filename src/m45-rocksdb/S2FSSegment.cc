#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{
    S2FSSegment::S2FSSegment(uint64_t addr)
    : _addr_start(addr),
    _buffer(0),
    _loaded(false),
    _cur_size(S2FSBlock::Size())
    {
        // hope this would always be 1...
        _reserve_for_inode = INODE_MAP_ENTRY_LENGTH * _fs->_zns_dev_ex->blocks_per_zone / 2 / S2FSBlock::Size() + 1;
    }

    uint64_t S2FSSegment::Size() { return _fs->_zns_dev->lba_size_bytes * _fs->_zns_dev_ex->blocks_per_zone; }

    S2FSSegment::~S2FSSegment()
    {
        Flush();
        for (auto p : _blocks)
        {
            if (!p.second || p.second == (S2FSBlock *)1)
                continue;
            delete p.second;
        }
        free(_buffer);
    }

    S2FSBlock *S2FSSegment::GetBlockByOffset(uint64_t offset)
    {
        if (!_loaded)
            _fs->LoadSegmentFromDisk(_addr_start);

        if (!map_contains(_blocks, offset))
            return NULL;

        S2FSBlock *block = NULL;
        block = _blocks.at(offset);

        if (block && !block->Loaded())
        {
            LastModify(microseconds_since_epoch());
            if (!_buffer)
                _buffer = (char *)calloc(S2FSSegment::Size(), sizeof(char));

            if (block->Type() == ITYPE_FILE_DATA)
                block->Content(_buffer + offset + 9);

            uint64_t read_start = offset / S2FSBlock::Size() * S2FSBlock::Size(), read_end = round_up(offset + _blocks[offset]->ActualSize(), S2FSBlock::Size());
            uint64_t skip = offset - read_start;
            auto actual_size = read_end - read_start;
            char buf[actual_size] = {0};
            int ret = zns_udevice_read(_fs->_zns_dev, read_start + _addr_start, buf, actual_size);
            if (ret)
            {
                std::cout << "Error: reading block at: " << offset + _addr_start << " during S2FSSegment::LookUp."
                          << "\n";
            }
            else
            {
                block->Deserialize(buf + skip);
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

    int64_t S2FSSegment::AllocateNew(const std::string &name, INodeType type, S2FSBlock **res, S2FSBlock *parent_dir)
    {
        if (name.length() > MAX_NAME_LENGTH)
            return -1;

        if (type != ITYPE_DIR_INODE && type != ITYPE_FILE_INODE)
        {
            std::cout << "Error: allocating new for unknown block type: " << type << " during S2FSSegment::AllocateNew." << "\n";
            return -1;
        }

        if (!_loaded)
            _fs->LoadSegmentFromDisk(_addr_start);

        WriteLock();
        if (_cur_size + S2FSBlock::Size() >= S2FSSegment::Size())
        {
            Unlock();
            return -1;
        }

        LastModify(microseconds_since_epoch());
        if (!_buffer)
            _buffer = (char *)calloc(S2FSSegment::Size(), sizeof(char));

        uint64_t allocated = 0;
        S2FSBlock *inode;
        INodeType data_type;
        S2FSFileAttr fa;

        inode = new S2FSBlock(type, _addr_start, 0, NULL);
        inode->GlobalOffset(_addr_start + _cur_size);
        _blocks[_cur_size] = inode;
        if (!name.empty())
            _name_2_inode[name] = inode->ID();
        _inode_map[inode->ID()] = _cur_size;
        _cur_size += S2FSBlock::Size();

        if (type == ITYPE_DIR_INODE)
            inode->Name(name);

        if (parent_dir)
        {
            fa.Name(name)
                ->CreateTime(microseconds_since_epoch())
                ->IsDir(type == ITYPE_DIR_INODE)
                ->InodeID(inode->ID())
                ->Offset(inode->GlobalOffset())
                ->Size(0);
        }

        *res = inode;
        inode->Serialize(_buffer + inode->GlobalOffset() - _addr_start);

        Flush(inode->GlobalOffset() - _addr_start);
        Unlock();

        // Do this after releasing the lock, otherwise deadlock happens
        if (inode && parent_dir)
            parent_dir->DirectoryAppend(fa);
        return allocated;
    }

    int64_t S2FSSegment::AllocateData(uint64_t inode_id, INodeType type, const char *data, uint64_t size, S2FSBlock **res)
    {
        if (!_loaded)
            _fs->LoadSegmentFromDisk(_addr_start);

        if (!map_contains(_inode_map, inode_id))
        {
            return -1;
        }

        WriteLock();
        if (_cur_size + 9>= S2FSSegment::Size())
        {
            Unlock();
            return -1;
        }

        LastModify(microseconds_since_epoch());
        if (!_buffer)
            _buffer = (char *)calloc(S2FSSegment::Size(), sizeof(char));

        auto inode = GetBlockByOffset(_inode_map[inode_id]);
        uint64_t to_copy = (size > (S2FSSegment::Size() - _cur_size - 9) ? (S2FSSegment::Size() - _cur_size - 9) : size);
        inode->AddOffset(_cur_size + Addr());
        S2FSBlock *data_block = new S2FSBlock(type, _addr_start, to_copy, _buffer + _cur_size + 9);
        data_block->GlobalOffset(_addr_start + _cur_size);
        _blocks[_cur_size] = data_block;
        if (data)
        {
            // std::cout<<"cur size: "<<_cur_size<<" to copy: "<<to_copy<<std::endl;
            memcpy(data_block->Content(), data, to_copy);
        }
        _cur_size += to_copy + 9;
        *res = data_block;

        inode->Serialize(_buffer + inode->GlobalOffset() - _addr_start);
        data_block->Serialize(_buffer + data_block->GlobalOffset() - _addr_start);

        Flush(data_block->GlobalOffset() - _addr_start);
        Unlock();
        return to_copy;
    }

    int S2FSSegment::RemoveINode(uint64_t inode_id)
    {
        LastModify(microseconds_since_epoch());
        if (_buffer)
        {
            auto inode = _blocks[_inode_map[inode_id]];
            memset(_buffer + inode->GlobalOffset() - _addr_start, 0, inode->ActualSize());
        }
        _blocks.erase(_inode_map[inode_id]);
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
        LastModify(microseconds_since_epoch());
        std::list<S2FSBlock *> inodes;
        std::list<S2FSSegment *> segments;
        std::set<S2FSSegment *> unlocked_segments;
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
            segment->Flush(0);
            if (unlocked_segments.find(segment) == unlocked_segments.end())
            {
                segment->Unlock();
                unlocked_segments.emplace(segment);
            }
            inode->Unlock();
            delete inode;
            inodes.pop_back();
            segments.pop_back();
        }

        // OnGC();

        return 0;
    }

    int S2FSSegment::Flush()
    {
        if (!_loaded)
            return 0;

        if (_cur_size == _reserve_for_inode * S2FSBlock::Size())
            return 0;

        uint32_t off = 0, size = _reserve_for_inode * S2FSBlock::Size();
        memset(_buffer, 0, size);
        if (!_addr_start)
        {
            *(uint64_t *)(_buffer) = id_alloc;
            off += sizeof(uint64_t);
        }

        *(uint64_t *)(_buffer + off) = _cur_size;
        off += sizeof(uint64_t);

        for (auto iter = _inode_map.begin(); iter != _inode_map.end() && off < size; off += INODE_MAP_ENTRY_LENGTH, iter++)
        {
            *(uint64_t *)(_buffer + off) = iter->first;
            *(uint64_t *)(_buffer + off + 8) = iter->second;
        }

        for (auto iter = _blocks.begin(); iter != _blocks.end(); iter++)
        {
            if (!iter->second||iter->second->Type()==0)
                continue;

            size = iter->second->GlobalOffset() - _addr_start;
            size += iter->second->Serialize(_buffer + size);
        }

        memset(_buffer + size, 0, S2FSSegment::Size() - size);
        size = round_up(size, S2FSBlock::Size());
        for (size_t i = _addr_start; i < _addr_start + size; i += S2FSBlock::Size())
        {
            int ret = zns_udevice_write(_fs->_zns_dev, i, _buffer + i - _addr_start, S2FSBlock::Size());
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
        uint64_t write_start, write_end;
        if (in_seg_off)
        {
            write_start = in_seg_off / S2FSBlock::Size() * S2FSBlock::Size();
            write_end = round_up(in_seg_off + _blocks[in_seg_off]->ActualSize(), S2FSBlock::Size());
        }
        else
        {
            write_start = 0;
            write_end = _reserve_for_inode * S2FSBlock::Size();
        }

        for (size_t i = write_start; i < write_end; i += S2FSBlock::Size())
        {
            int ret = zns_udevice_write(_fs->_zns_dev, i + _addr_start, _buffer + i - write_start, S2FSBlock::Size());
            if (ret)
            {
                std::cout << "Error: nvme write error at: " << i + _addr_start << " ret:" << ret << " during S2FSSegment::Flush."
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

        free(_buffer);
        _buffer = 0;
        _loaded = false;
        return 0;
    }

    int S2FSSegment::OnGC()
    {
        _fs->LoadSegmentFromDisk(_addr_start);
        WriteLock();
        LastModify(microseconds_since_epoch());
        uint64_t ptr = S2FSBlock::Size();
        std::map<uint64_t, S2FSBlock*> blocks;
        // Sort the INodes
        for (auto p : _inode_map)
        {
            if (!_blocks[p.second])
                continue;

            blocks[p.second] = _blocks[p.second];
            _blocks[p.second]->WriteLock();
        }

        // Reorder the blocks
        _inode_map.clear();
        std::map<uint64_t, S2FSBlock*> new_blocks;
        for (auto p : blocks)
        {
            _blocks.erase(p.first);
            auto block = p.second;
            new_blocks[ptr] = block;
            _inode_map[block->ID()] = ptr;
            block->GlobalOffset(_addr_start + ptr);
            if (block->Next())
            {
                auto s = _fs->ReadSegment(addr_2_segment(block->Next()));
                if (s != this)
                    s->WriteLock();
                auto next = s->GetBlockByOffset(addr_2_inseg_offset(block->Next()));
                next->WriteLock();
                next->Prev(block->GlobalOffset());
                next->Unlock();
                if (s != this)
                    s->Unlock();
            }

            if (block->Prev())
            {
                auto s = _fs->ReadSegment(addr_2_segment(block->Prev()));
                if (s != this)
                    s->WriteLock();
                auto prev = s->GetBlockByOffset(addr_2_inseg_offset(block->Prev()));
                if (!prev)
                    prev = new_blocks[addr_2_inseg_offset(block->Prev())];
                prev->WriteLock();
                prev->Next(block->GlobalOffset());
                prev->Unlock();
                if (s != this)
                    s->Unlock();
            }

            ptr += block->ActualSize();

            auto old_offsets = block->Offsets();
            block->Offsets().clear();
            for (auto off : old_offsets)
            {
                auto data_block = GetBlockByOffset(addr_2_inseg_offset(off));
                if (!data_block)
                    continue;       // This data block could be in other segments

                if (data_block->Type() == ITYPE_FILE_DATA)
                {
                    // in case of overlapping, copy to a tmp buffer
                    char tmp[data_block->ContentSize()] = {0};
                    memcpy(tmp, data_block->Content(), data_block->ContentSize());
                    memcpy(_buffer + ptr + 9, tmp, data_block->ContentSize());
                    data_block->Content(_buffer + ptr + 9);
                }
                block->AddOffset(ptr);
                new_blocks[ptr] = data_block;
                data_block->GlobalOffset(ptr + _addr_start);
                ptr += data_block->ActualSize();
                _blocks.erase(addr_2_inseg_offset(off));
            }

            p.second->Unlock();
        }

        // Update the offsets inside the dir data
        for (auto p : blocks)
        {
            if (p.second->Type() == ITYPE_DIR_DATA)
            {
                // p.second->WriteLock();
                for (auto fa : p.second->FileAttrs())
                {
                    fa->Offset(_inode_map[fa->InodeID()] + _addr_start);
                }
                // p.second->Unlock();
            }
        }

        // The remaining blocks are data blocks belonging to deleted file/dir
        for (auto p : _blocks)
        {
            delete p.second;
        }

        _blocks = new_blocks;
        _cur_size = ptr;
        
        Offload();
        Unlock();
        return 0;
    }

    void S2FSSegment::Preload(char *buffer)
    {
        if (!*(uint64_t *)(buffer))
            return;

        uint32_t c = 0;
        if (!_addr_start)
        {
            id_alloc = *(uint64_t *)(buffer);
            c += sizeof(uint64_t);
        }
        _cur_size = *(uint64_t *)(buffer + c);
        c += sizeof(uint64_t);

        for (c; c < _reserve_for_inode * S2FSBlock::Size(); c += INODE_MAP_ENTRY_LENGTH)
        {
            uint64_t id = *(uint64_t *)(buffer + c);
            uint64_t offset = *(uint64_t *)(buffer + c + 8);
            if (!id && !offset)
                break;
            _inode_map[id] = offset;
            // _blocks[addr_2_inseg_offset(offset)] = new S2FSBlock;
        }
    }

    uint64_t S2FSSegment::Serialize(char *buffer)
    {
        ReadLock();
        uint32_t off = 0, size = _reserve_for_inode * S2FSBlock::Size();
        if (!_addr_start)
        {
            *(uint64_t *)(buffer) = id_alloc;
            off += sizeof(uint64_t);
        }

        *(uint64_t *)(buffer + off) = _cur_size;
        off += sizeof(uint64_t);

        for (auto iter = _inode_map.begin(); iter != _inode_map.end() && off < size; off += INODE_MAP_ENTRY_LENGTH, iter++)
        {
            *(uint64_t *)(buffer + off) = iter->first;
            *(uint64_t *)(buffer + off + 8) = iter->second;
        }

        uint64_t ptr = size;
        for (auto iter = _blocks.begin(); iter != _blocks.end(); iter++)
        {
            if (!iter->second)
            {
                auto ii = iter;
                ii++;
                ptr += ii->first - iter->first;
                continue;
            }
            ptr += iter->second->Serialize(buffer + ptr);
        }
        Unlock();
        return S2FSSegment::Size();
    }

    uint64_t S2FSSegment::Deserialize(char *buffer)
    {
        // WriteLock();
        Preload(buffer);
        uint64_t ptr = _reserve_for_inode * S2FSBlock::Size(), last = 0;

        if (!_buffer)
            _buffer = (char *)calloc(S2FSSegment::Size(), sizeof(char));

        while (ptr < _cur_size)
        {      
            S2FSBlock *block; 
            if (!map_contains(_blocks, ptr))
            {
                block = new S2FSBlock;
                _blocks[ptr] = block;
            }
            else
                block = _blocks.at(ptr);

            block->Content(_buffer + ptr + 9);
            //block->WriteLock();
            auto ssize = block->Deserialize(buffer + ptr);
            //block->Unlock();
            if (block->Type() == 0)
            {
                _blocks.erase(ptr);
                delete block;
                while (ptr < _cur_size && !buffer[ptr]) { ptr++; }
                continue;
            }
            else
            {
                last = ptr;
                block->SegmentAddr(_addr_start);
                block->GlobalOffset(_addr_start + ptr);
                block->Loaded(true);
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
            ptr += ssize;
        }
        // _cur_size = last + _blocks[last]->ActualSize();
        // Unlock();
        return S2FSSegment::Size();
    }
}
