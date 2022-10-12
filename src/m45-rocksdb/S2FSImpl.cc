#include "S2FSImpl.h"

namespace ROCKSDB_NAMESPACE
{
    S2FSSequentialFile::S2FSSequentialFile(S2FSBlock *inode, S2FSBlock *parent)
        : _inode(inode)
    {
        _size = parent->GetFileSize(_inode->ID());
        _buffer = (char *)calloc(_size, sizeof(char));
        _inode->Read(_buffer, _size, 0, 0);
    }

    S2FSRandomAccessFile::S2FSRandomAccessFile(S2FSBlock *inode, S2FSBlock *parent)
        : _inode(inode)
    {
        _size = parent->GetFileSize(_inode->ID());
        _buffer = (char *)calloc(_size, sizeof(char));
        _inode->Read(_buffer, _size, 0, 0);
    }

    int S2FSFileLock::Lock()
    {
        pthread_mutex_lock(&_inner_mutex);
        if (_locked)
        {
            pthread_mutex_unlock(&_inner_mutex);
            return -1;
        }
        // _inode->WriteLock();
        _locked = pthread_self();
        pthread_mutex_unlock(&_inner_mutex);
        return 0;
    }

    int S2FSFileLock::Unlock()
    {
        pthread_mutex_lock(&_inner_mutex);
        if (_locked != pthread_self())
        {
            pthread_mutex_unlock(&_inner_mutex);
            return -1;
        }
        // _inode->Unlock();
        _locked = 0;
        pthread_mutex_unlock(&_inner_mutex);
        return 0;
    }

    IOStatus S2FSWritableFile::Append(const Slice &data, const IOOptions &options,
                                      IODebugContext *dbg)
    {
        if (_inode->DataAppend(data.data(), data.size()))
        {
            return IOStatus::IOError();
        }
        _parent->AddFileSize(_inode->ID(), data.size());
        return IOStatus::OK();
    }

    IOStatus S2FSRandomAccessFile::Read(uint64_t offset, size_t n, const IOOptions &options,
                                        Slice *result, char *scratch,
                                        IODebugContext *dbg) const
    {
        if (offset >= _size)
        {
            *result = Slice(scratch, 0);
            return IOStatus::OK();
        }

        auto read_num = (_size - offset) > n ? n : (_size - offset);

        memcpy(scratch, _buffer + offset, read_num);
        *result = Slice(scratch, read_num);

        return IOStatus::OK();
    }

    IOStatus S2FSSequentialFile::Read(size_t n, const IOOptions &options,
                                      Slice *result, char *scratch,
                                      IODebugContext *dbg)
    {
        if (_offset_pointer >= _size)
        {
            *result = Slice(scratch, 0);
            return IOStatus::OK();
        }
        
        auto read_num = (_size - _offset_pointer) > n ? n : (_size - _offset_pointer);

        memcpy(scratch, _buffer + _offset_pointer, read_num);
        *result = Slice(scratch, read_num);

        Skip(read_num);

        return IOStatus::OK();
    }

    IOStatus S2FSSequentialFile::Skip(uint64_t n)
    {
        OffsetSkip(n);
        return IOStatus::OK();
    }
}