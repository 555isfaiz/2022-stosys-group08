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

        virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) {};

        virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
            return 0;
        }
    };
    
    class S2FSWritableFile : public FSWritableFile
    {
    private:
        S2FSBlock *_inode;
    public:
        S2FSWritableFile(S2FSBlock *inode) 
        : _inode(inode)
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

        virtual IOStatus Close(const IOOptions &options, IODebugContext *dbg) {}
        virtual IOStatus Flush(const IOOptions &options, IODebugContext *dbg) {}
        virtual IOStatus Sync(const IOOptions &options,
                              IODebugContext *dbg){} // sync data

    };
}

#endif