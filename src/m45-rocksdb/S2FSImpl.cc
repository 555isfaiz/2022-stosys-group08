#include "S2FSImpl.h"

namespace ROCKSDB_NAMESPACE
{
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
        return IOStatus::OK();
    }

    IOStatus S2FSRandomAccessFile::Read(uint64_t offset, size_t n, const IOOptions &options,
                                        Slice *result, char *scratch,
                                        IODebugContext *dbg) const
    {
        auto read_num = _inode->Read(scratch, n, offset, 0);
        if (!read_num)
        {
            *result = Slice(scratch, 0);
            return IOStatus::OK();
        }

        *result = Slice(scratch, read_num);
        return IOStatus::OK();
    }

    IOStatus S2FSSequentialFile::Read(size_t n, const IOOptions &options,
                                      Slice *result, char *scratch,
                                      IODebugContext *dbg)
    {
        auto read_num = _inode->Read(scratch, n, _offset, 0);
        
        if (!read_num)
        {
            *result = Slice(scratch, 0);
            return IOStatus::OK();
        }

        Skip(read_num);

        *result = Slice(scratch, read_num);
        return IOStatus::OK();
    }

    IOStatus S2FSSequentialFile::Skip(uint64_t n)
    {
        OffsetSkip(n);
        return IOStatus::OK();
    }
}