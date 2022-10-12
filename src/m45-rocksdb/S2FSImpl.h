#ifndef STOSYS_PROJECT_S2FILESYSTEM_WRITABLE_FILE_H
#define STOSYS_PROJECT_S2FILESYSTEM_WRITABLE_FILE_H

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "S2FSCommon.h"

#include <zns_device.h>
#include <pthread.h>

namespace ROCKSDB_NAMESPACE
{
    class S2FSBlock;

    class S2FSFileLock : public FileLock
    {
    private:
        S2FSBlock *_inode;
        pthread_t _locked;
        pthread_mutex_t _inner_mutex = PTHREAD_MUTEX_INITIALIZER;
    public:
        S2FSFileLock(S2FSBlock *inode)
        : _inode(inode)
        , _locked(0)
        {}
        ~S2FSFileLock() {}

        int Lock();
        int Unlock();
    };

    class S2FSDirectory : public FSDirectory
    {
    private:
        S2FSBlock *_inode;
    public:
        S2FSDirectory(S2FSBlock *inode)
        : _inode(inode)
        {}
        ~S2FSDirectory() {}

        virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) 
        {
            return IOStatus::OK();
        }

        virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
            return 0;
        }
    };
    
    class S2FSWritableFile : public FSWritableFile
    {
    private:
        S2FSBlock *_inode;
        S2FSBlock *_parent;
    public:
        S2FSWritableFile(S2FSBlock *inode, S2FSBlock *parent) 
        : _inode(inode),
        _parent(parent)
        {}
        ~S2FSWritableFile() {}
        // Append data to the end of the file
        // Note: A WriteableFile object must support either Append or
        // PositionedAppend, so the users cannot mix the two.
        virtual IOStatus Append(const Slice &data, const IOOptions &options,
                                IODebugContext *dbg);

        // Append data with verification information.
        // Note that this API change is experimental and it might be changed in
        // the future. Currently, RocksDB only generates crc32c based checksum for
        // the file writes when the checksum handoff option is set.
        // Expected behavior: if the handoff_checksum_type in FileOptions (currently,
        // ChecksumType::kCRC32C is set as default) is not supported by this
        // FSWritableFile, the information in DataVerificationInfo can be ignored
        // (i.e. does not perform checksum verification).
        virtual IOStatus Append(const Slice &data, const IOOptions &options,
                                const DataVerificationInfo & /* verification_info */,
                                IODebugContext *dbg)
        {
            return Append(data, options, dbg);
        }

        virtual IOStatus Close(const IOOptions &options, IODebugContext *dbg) 
        {
            return IOStatus::OK();
        }

        virtual IOStatus Flush(const IOOptions &options, IODebugContext *dbg) 
        {
            return IOStatus::OK();
        }

        virtual IOStatus Sync(const IOOptions &options,
                              IODebugContext *dbg)
        {
            return IOStatus::OK();
        } // sync data
    };

    class S2FSRandomAccessFile : public FSRandomAccessFile
    {
    private:
        S2FSBlock *_inode;

    public:
        S2FSRandomAccessFile(S2FSBlock *inode)
            : _inode(inode)
        {
        }
        ~S2FSRandomAccessFile() {}

        virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions &options,
                              Slice *result, char *scratch,
                              IODebugContext *dbg) const;
    };

    class S2FSSequentialFile : public FSSequentialFile
    {
    private:
        S2FSBlock *_inode;
        int64_t _offset_pointer = 0;
        int64_t _eof = 0;
        // TODO: lock for offset
    public:
        S2FSSequentialFile(S2FSBlock *inode)
            : _inode(inode)
        {
        }
        ~S2FSSequentialFile() {}

        virtual IOStatus Read(size_t n, const IOOptions &options,
                              Slice *result, char *scratch,
                              IODebugContext *dbg);

        virtual IOStatus Skip(uint64_t n);
        // Not make any sense to have the addoffset function
        // Still need modify
        void OffsetSkip(uint64_t n)
        {
            _offset_pointer += n;
        }
    };
}

#endif