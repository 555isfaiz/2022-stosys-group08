#include "S2FSCommon.h"

namespace ROCKSDB_NAMESPACE
{

    uint64_t S2FSBlock::Size() { return _fs->_zns_dev->lba_size_bytes; }

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
            *(uint64_t *)(buffer + 1) = _content_size;
            memcpy(buffer + 9, _content, Size() - 1);
            break;

        default:
            std::cout << "Error: unknown block type: " << _type << " during S2FSBlock::Serialize."
                      << "\n";
            break;
        }
    }

    void S2FSBlock::Deserialize(char *buffer)
    {
        char type = *buffer >> 4;
        _loaded = true;
        switch (type)
        {
        case 1:
            _type = ITYPE_FILE_INODE;
            DeserializeFileInode(buffer);
            break;
        case 2:
            _type = ITYPE_FILE_DATA;
            _content = (char *)calloc(Size() - 1, sizeof(char));
            _content_size = *(uint64_t *)(buffer + 1);
            memcpy(_content, buffer + 9, Size() - 1);
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
            // _type = ITYPE_UNKNOWN;
            break;

        default:
            std::cout << "Error: dirty data, unknown block type: " << type << " during S2FSBlock::Deserialize."
                      << "\n";
            break;
        }
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

    S2FSBlock* S2FSBlock::DirectoryLookUp(std::string &name)
    {
        if (_type != ITYPE_DIR_INODE)
        {
            std::cout << "Error: looking up dir in a non-dir inode type: " << _type << " name: " << name << " during S2FSBlock::DirectoryLookUp."
                      << "\n";
            return NULL;
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
                if (attr->Name() == name)
                {
                    auto ss = _fs->ReadSegment(addr_2_segment(attr->Offset()));
                    S2FSBlock *res;
                    if (ss != s)
                    {
                        ss->WriteLock();
                        res = ss->LookUp(attr->Name());
                        ss->Unlock();
                    }
                    else
                        res = ss->LookUp(attr->Name());

                    return res;
                }
            }

            s->Unlock();
            data->Unlock();
        }
        Unlock();
        return NULL;
    }

    int S2FSBlock::DirectoryAppend(S2FSFileAttr& fa)
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

        if (!data_block || (data_block->FileAttrs().size() + 1) * FILE_ATTR_SIZE > S2FSBlock::MaxDataSize(ITYPE_DIR_INODE))
        {
            uint64_t allocated = 0, to_allocate = S2FSBlock::MaxDataSize(ITYPE_DIR_INODE);
            do
            {
                uint64_t tmp = segment->AllocateData(inode->ID(), ITYPE_DIR_DATA, NULL, to_allocate - allocated, &data_block);
                allocated += tmp;
                // this segment is full, allocate new in next segment
                if (tmp < 0)
                {
                    segment = _fs->FindNonFullSegment();
                    if (!segment)
                    {
                        return -1;
                    }

                    S2FSBlock *res;
                    tmp = segment->AllocateNew("", ITYPE_DIR_INODE, NULL, to_allocate - allocated, &res, NULL);
                    if (tmp < 0)
                    {
                        return -1;
                    }
                    inode->Next(res->GlobalOffset());
                    res->Prev(inode->GlobalOffset());
                    res->WriteLock();
                    inodes.push_back(res);
                    inode = res;
                    allocated += tmp;
                }

            } while (allocated < to_allocate);

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

        auto data_block = segment->GetBlockByOffset(addr_2_inseg_offset(inode->Offsets().back()));
        if (data_block->ContentSize() + len > S2FSBlock::MaxDataSize(ITYPE_FILE_INODE))
        {
            uint64_t allocated = 0;
            do
            {
                uint64_t tmp = segment->AllocateData(inode->ID(), ITYPE_FILE_DATA, data + allocated, len - allocated, &data_block);
                allocated += tmp;
                // this segment is full, allocate new in next segment
                if (tmp < 0)
                {
                    segment = _fs->FindNonFullSegment();
                    if (!segment)
                    {
                        return -1;
                    }

                    S2FSBlock *res;
                    tmp = segment->AllocateNew("", ITYPE_FILE_DATA, data + allocated, len - allocated, &res, NULL);
                    if (tmp < 0)
                    {
                        return -1;
                    }
                    inode->Next(res->GlobalOffset());
                    res->Prev(inode->GlobalOffset());
                    res->WriteLock();
                    inodes.push_back(res);
                    inode = res;
                    allocated += tmp;
                }

            } while (allocated < len);
        }
        else
        {
            memcpy(data_block->Content() + data_block->ContentSize(), data, len);
        }

        while (!inodes.empty())
        {
            inodes.back()->Unlock();
            inodes.pop_back();
        }
        return 0;
    }

    uint64_t S2FSBlock::MaxDataSize(INodeType type)
    {
        if (type == ITYPE_DIR_INODE)
        {
            return S2FSBlock::Size() - 1;
        }
        else if (type == ITYPE_FILE_INODE)
        {
            return S2FSBlock::Size() - 9;
        }
        else
            return 0;
    }
}