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

    IOStatus S2FSRandomAccessFile::Read(uint64_t offset, size_t n, const IOOptions& options,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const
    {
        if (_inode->Read(scratch, n, offset, 0))
        {
            *result = Slice(scratch, 0);
            return IOStatus::IOError();
        }
        
        *result = Slice(scratch, n);
        return IOStatus::OK();
    }
}