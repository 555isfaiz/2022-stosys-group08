#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{

    uint64_t S2FSBlock::Size() { return _fs->_zns_dev->lba_size_bytes; }

    S2FSBlock::~S2FSBlock()
    {
        switch (_type)
        {
        case 2:
        {
            free(_content);
            _content = 0; _content_size = 0;
            break;
        }
        case 8:
        {
            while (!_file_attrs.empty())
            {
                delete _file_attrs.front();
                _file_attrs.pop_front();
            }
            break;
        }

        default:
            break;
        }
    }

    uint64_t S2FSBlock::SerializeFileInode(char *buffer)
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
        return S2FSBlock::Size();
    }

    uint64_t S2FSBlock::DeserializeFileInode(char *buffer)
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
        return S2FSBlock::Size();
    }

    uint64_t S2FSBlock::SerializeDirInode(char *buffer)
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
        return S2FSBlock::Size();
    }

    uint64_t S2FSBlock::DeserializeDirInode(char *buffer)
    {
        uint64_t ptr = 0;
        _next = *(uint64_t *)(buffer + (ptr += 1));
        _prev = *(uint64_t *)(buffer + (ptr += sizeof(uint64_t)));
        _id = *(uint64_t *)(buffer + (ptr += sizeof(uint64_t)));
        uint32_t str_len = strlen(buffer + (ptr += sizeof(uint64_t)));
        _name = std::string(buffer + (ptr += sizeof(uint64_t)), (str_len < MAX_NAME_LENGTH ? str_len : MAX_NAME_LENGTH));
        ptr += MAX_NAME_LENGTH;
        while (*(uint64_t *)(buffer + ptr))
        {
            _offsets.push_back(*(uint64_t *)(buffer + ptr));
        }
        return S2FSBlock::Size();
    }

    uint64_t S2FSBlock::SerializeDirData(char *buffer)
    {
        *buffer = _type << 4;
        *(uint64_t *)(buffer + 1) = _content_size;
        uint64_t ptr = 9;
        for (auto fa : _file_attrs)
        {
            ptr += fa->Serialize(buffer + ptr);
        }
        return _content_size;
    }

    uint64_t S2FSBlock::DeserializeDirData(char *buffer)
    {
        uint64_t ptr = 9;
        _content_size = *(uint64_t *)(buffer + 1);
        for (size_t i = 0; i < _content_size; i++)
        {
            S2FSFileAttr *fa = new S2FSFileAttr;
            auto size = fa->Deserialize(buffer + ptr);
            if (size == 0)
            {
                delete fa;
                break;
            }
            ptr += size;
            _file_attrs.push_back(fa);
        }
        return _content_size;
    }

    uint64_t S2FSBlock::Serialize(char *buffer)
    {
        switch (_type)
        {
        case ITYPE_DIR_INODE:
            return SerializeDirInode(buffer);

        case ITYPE_DIR_DATA:
            return SerializeDirData(buffer);

        case ITYPE_FILE_INODE:
            return SerializeFileInode(buffer);

        case ITYPE_FILE_DATA:
            *buffer = _type << 4;
            *(uint64_t *)(buffer + 1) = _content_size;
            memcpy(buffer + 9, _content, _content_size);
            return _content_size + 9;

        default:
            // std::cout << "Error: unknown block type: " << _type << " during S2FSBlock::Serialize."
            //           << "\n";
            break;
        }
        return 0;
    }

    uint64_t S2FSBlock::Deserialize(char *buffer)
    {
        uint8_t type = *buffer >> 4;
        _loaded = true;
        switch (type)
        {
        case 1:
            _type = ITYPE_FILE_INODE;
            return DeserializeFileInode(buffer);
        case 2:
            _type = ITYPE_FILE_DATA;
            _content = (char *)calloc(Size() - 1, sizeof(char));
            _content_size = *(uint64_t *)(buffer + 1);
            memcpy(_content, buffer + 9, _content_size);
            return _content_size + 9;
        case 4:
            _type = ITYPE_DIR_INODE;
            return DeserializeDirInode(buffer);
        case 8:
            _type = ITYPE_DIR_DATA;
            return DeserializeDirData(buffer);
        case 0:
            // _type = ITYPE_UNKNOWN;
            break;

        default:
            std::cout << "Error: dirty data, unknown block type: " << type << " during S2FSBlock::Deserialize."
                      << "\n";
            break;
        }
        return 0;
    }

    void S2FSBlock::LivenessCheck()
    {
        if (_loaded)
            return;

        _fs->ReadSegment(_segment_addr)->GetBlockByOffset(addr_2_inseg_offset(GlobalOffset()));
    }

    int S2FSBlock::ChainReadLock()
    {
        return ReadLock();
    }

    int S2FSBlock::ChainWriteLock()
    {
        return WriteLock();
    }

    int S2FSBlock::ChainUnlock()
    {
        return Unlock();
    }

    S2FSBlock *S2FSBlock::DirectoryLookUp(std::string &name)
    {
        if (_type != ITYPE_DIR_INODE)
        {
            std::cout << "Error: looking up dir in a non-dir inode type: " << _type << " name: " << name << " during S2FSBlock::DirectoryLookUp."
                      << "\n";
            return NULL;
        }

        WriteLock();
        LivenessCheck();
        S2FSBlock *res = NULL;
        for (auto off : _offsets)
        {
            S2FSBlock *data;
            auto s = _fs->ReadSegment(addr_2_segment(off));
            s->WriteLock();
            data = s->GetBlockByOffset(off);

            data->ReadLock();
            for (auto attr : data->FileAttrs())
            {
                if (attr->Name() == name)
                {
                    auto ss = _fs->ReadSegment(addr_2_segment(attr->Offset()));
                    if (ss != s)
                    {
                        ss->WriteLock();
                        res = ss->GetBlockByOffset(addr_2_inseg_offset(attr->Offset()));
                        ss->Unlock();
                    }
                    else
                        res = ss->GetBlockByOffset(addr_2_inseg_offset(attr->Offset()));

                    s->Unlock();
                    data->Unlock();
                    Unlock();
                    return res;
                }
            }

            s->Unlock();
            data->Unlock();
        }

        if (_next)
        {
            auto s = _fs->ReadSegment(addr_2_segment(_next));
            s->WriteLock();
            auto in = s->GetBlockByOffset(addr_2_inseg_offset(_next));
            s->Unlock();
            res = in->DirectoryLookUp(name);
        }
        Unlock();
        return res;
    }

    int S2FSBlock::DirectoryAppend(S2FSFileAttr &fa)
    {
        WriteLock();
        LivenessCheck();
        auto inode = this;
        S2FSSegment *segment = _fs->ReadSegment(inode->SegmentAddr());
        std::list<S2FSBlock *> inodes;
        inodes.push_back(inode);
        while (inode->Next())
        {
            segment = _fs->ReadSegment(addr_2_segment(inode->Next()));
            segment->WriteLock();
            inode = segment->GetBlockByOffset(addr_2_inseg_offset(inode->Next()));
            inode->WriteLock();
            segment->Unlock();
            inodes.push_back(inode);
        }

        segment->WriteLock();
        auto data_block = segment->GetBlockByOffset(addr_2_inseg_offset(inode->Offsets().back()));
        segment->Unlock();

        if (!data_block || (data_block->FileAttrs().size() + 1) * FILE_ATTR_SIZE > data_block->ContentSize())
        {
            uint64_t to_allocate = S2FSBlock::MaxDataSize(ITYPE_DIR_INODE);
            do
            {
                int64_t tmp = segment->AllocateData(inode->ID(), ITYPE_DIR_DATA, NULL, to_allocate, &data_block);
                // this segment is full, allocate new in next segment
                if (tmp < 0)
                {
                    segment = _fs->FindNonFullSegment();
                    if (!segment)
                        return -1;

                    S2FSBlock *res;
                    tmp = segment->AllocateNew("", ITYPE_DIR_INODE, &res, NULL);
                    if (tmp < 0)
                        continue;

                    inode->Next(res->GlobalOffset());
                    res->Prev(inode->GlobalOffset());
                    res->WriteLock();
                    inodes.push_back(res);
                    inode = res;
                }
                else
                    break;

            } while (true);

            segment->GetBlockByOffset(inode->Offsets().back())->AddFileAttr(fa);
        }
        else
        {
            data_block->AddFileAttr(fa);
        }

        while (!inodes.empty())
        {
            inodes.back()->Unlock();
            inodes.pop_back();
        }
        return 0;
    }

    int S2FSBlock::DataAppend(const char *data, uint64_t len)
    {
        WriteLock();
        LivenessCheck();
        auto inode = this;
        S2FSSegment *segment = _fs->ReadSegment(inode->SegmentAddr());
        std::list<S2FSBlock *> inodes;
        inodes.push_back(inode);
        while (inode->Next())
        {
            segment = _fs->ReadSegment(addr_2_segment(inode->Next()));
            segment->WriteLock();
            inode = segment->GetBlockByOffset(addr_2_inseg_offset(inode->Next()));
            inode->WriteLock();
            segment->Unlock();
            inodes.push_back(inode);
        }

        S2FSBlock *data_block;
        uint64_t io_num = 0;
        while (io_num < len)
        {
            int64_t tmp = segment->AllocateData(inode->ID(), ITYPE_FILE_DATA, data + io_num, len - io_num, &data_block);
            // this segment is full, allocate new in next segment
            if (tmp < 0)
            {
                segment = _fs->FindNonFullSegment();
                if (!segment)
                {
                    return -1;
                }

                S2FSBlock *res;
                tmp = segment->AllocateNew("", ITYPE_FILE_INODE, &res, NULL);
                if (tmp < 0)
                {
                    return -1;
                }
                inode->Next(res->GlobalOffset());
                res->Prev(inode->GlobalOffset());
                res->WriteLock();
                inodes.push_back(res);
                inode = res;
            }
            else
                io_num += tmp;
        }

        while (!inodes.empty())
        {
            inodes.back()->Unlock();
            inodes.pop_back();
        }
        return 0;
    }

    int S2FSBlock::Read(char *buf, uint64_t n, uint64_t offset, uint64_t buf_offset)
    {
        WriteLock();
        LivenessCheck();
        uint64_t cur_off = 0, read_num = 0, buf_off = buf_offset, total_read = 0;
        for (auto off : _offsets)
        {
            if (total_read >= n)
                break;

            auto s = _fs->ReadSegment(addr_2_segment(off));
            auto data_block = s->GetBlockByOffset(addr_2_inseg_offset(off));

            if (cur_off + data_block->ContentSize() > offset)
            {
                uint64_t in_block_off = (cur_off > offset ? 0 : offset - cur_off), max_readable = data_block->ContentSize() - in_block_off;
                
                uint64_t to_read = max_readable >= n ? n : max_readable;
                memcpy(buf + buf_off, data_block->Content() + in_block_off, to_read);
                buf_off += to_read;
                read_num += to_read;
                total_read += to_read;
                cur_off += to_read;
            }
            else
                cur_off += data_block->ContentSize();
        }

        if (read_num < n && _next)
        {
            auto s = _fs->ReadSegment(addr_2_segment(_next));
            s->WriteLock();
            auto in = s->GetBlockByOffset(addr_2_inseg_offset(_next));
            s->Unlock();
            total_read += in->Read(buf, n - read_num, offset - cur_off, buf_off);
        }

        Unlock();
        return total_read;
    }

    int S2FSBlock::ReadChildren(std::vector<std::string> *list)
    {
        if (_type != ITYPE_DIR_INODE)
        {
            std::cout << "Error: I am not dir inode, can't do this. My type: " << _type << " during S2FSBlock::ReadChildren."
                      << "\n";
            return -1;
        }

        WriteLock();
        LivenessCheck();

        for (auto off : _offsets)
        {
            S2FSBlock *data;
            auto s = _fs->ReadSegment(addr_2_segment(off));
            s->WriteLock();
            data = s->GetBlockByOffset(off);

            data->ReadLock();
            for (auto attr : data->FileAttrs())
            {
                list->push_back(attr->Name());
            }

            s->Unlock();
            data->Unlock();
        }

        if (_next)
        {
            auto s = _fs->ReadSegment(addr_2_segment(_next));
            s->WriteLock();
            auto in = s->GetBlockByOffset(addr_2_inseg_offset(_next));
            s->Unlock();
            in->ReadChildren(list);
        }

        Unlock();
        return 0;
    }

    void S2FSBlock::RenameChild(const std::string &src, const std::string &target)
    {
        if (_type != ITYPE_DIR_INODE)
        {
            std::cout << "Error: I am not dir inode, can't do this. My type: " << _type << " during S2FSBlock::RenameChild."
                      << "\n";
            return;
        }
        WriteLock();
        LivenessCheck();

        for (auto off : _offsets)
        {
            S2FSBlock *data;
            auto s = _fs->ReadSegment(addr_2_segment(off));
            s->WriteLock();
            data = s->GetBlockByOffset(off);

            data->ReadLock();
            for (auto attr : data->FileAttrs())
            {
                if (attr->Name() == src)
                {
                    attr->Name(target);
                    s->OnRename(src, target);
                    s->Flush(addr_2_inseg_offset(data->GlobalOffset()));
                    data->Unlock();
                    Unlock();
                    s->Unlock();
                    return;
                }
            }

            s->Unlock();
            data->Unlock();
        }

        if (_next)
        {
            auto s = _fs->ReadSegment(addr_2_segment(_next));
            s->WriteLock();
            auto in = s->GetBlockByOffset(addr_2_inseg_offset(_next));
            s->Unlock();
            in->RenameChild(src, target);
        }

        Unlock();
    }

    int S2FSBlock::Offload()
    {
        if (!_loaded)
            return 0;

        _loaded = false;
        switch (_type)
        {
        case ITYPE_DIR_DATA:
        {
            for (size_t size = _file_attrs.size(); size > 0; size--)
            {
                free(_file_attrs.front());
                _file_attrs.pop_front();
            }
            break;
        }

        case ITYPE_DIR_INODE:
        case ITYPE_FILE_INODE:
            _offsets.clear();
            break;

        case ITYPE_FILE_DATA:
            free(_content);
            _content = 0;
            break;

        default:
            break;
        }

        return 0;
    }

    uint64_t S2FSBlock::ActualSize()
    {
        switch (_type)
        {
        case ITYPE_DIR_DATA:
            return _content_size + 9;

        case ITYPE_DIR_INODE:
        case ITYPE_FILE_INODE:
            return S2FSBlock::Size();

        case ITYPE_FILE_DATA:
            return _content_size + 9;

        default:
            break;
        }

        return 0;
    }

    uint64_t S2FSBlock::MaxDataSize(INodeType type)
    {
        if (type == ITYPE_DIR_INODE)
        {
            return S2FSBlock::Size() - 9;
        }
        else if (type == ITYPE_FILE_INODE)
        {
            return S2FSBlock::Size() - 9;
        }
        else
            return 0;
    }
}