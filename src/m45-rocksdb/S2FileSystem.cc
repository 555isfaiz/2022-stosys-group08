/*
 * MIT License
Copyright (c) 2021 - current
Authors:  Animesh Trivedi
This code is part of the Storage System Course at VU Amsterdam
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#include "S2FileSystem.h"
#include <string>
#include <iostream>
#include <sys/mman.h>

#include <stosys_debug.h>
#include <utils.h>

namespace ROCKSDB_NAMESPACE {
    S2FileSystem::S2FileSystem(std::string uri_db_path, bool debug) {
        FileSystem::Default();
        std::string sdelimiter = ":";
        std::string edelimiter = "://";
        this->_uri = uri_db_path;
        struct zdev_init_params params;
        std::string device = uri_db_path.substr(uri_db_path.find(sdelimiter) + sdelimiter.size(),
                                                uri_db_path.find(edelimiter) -
                                                (uri_db_path.find(sdelimiter) + sdelimiter.size()));
        //make sure to setup these parameters properly and check the forced reset flag for M5
        params.name = strdup(device.c_str());
        params.log_zones = 3;
        params.gc_wmark = 1;
        params.force_reset = true;
        int ret = init_ss_zns_device(&params, &this->_zns_dev);
        if(ret != 0){
            std::cout << "Error: " << uri_db_path << " failed to open the device " << device.c_str() << "\n";
            std::cout << "Error: ret " << ret << "\n";
        }
        this->_zns_dev_ex = (struct zns_device_extra_info *)this->_zns_dev->_private;
        assert (ret == 0);
        assert(this->_zns_dev->lba_size_bytes != 0);
        assert(this->_zns_dev->capacity_bytes != 0);
        ss_dprintf(DBG_FS_1, "device %s is opened and initialized, reported LBA size is %u and capacity %lu \n",
                   device.c_str(), this->_zns_dev->lba_size_bytes, this->_zns_dev->capacity_bytes);

        S2FSObject::_fs = this;
    }

    S2FileSystem::~S2FileSystem() {
    }

    S2FSSegment *S2FileSystem::ReadSegmentFromCache(uint64_t from)
    {
        Lock();
        for (auto s : _cache)
        {
            if (s->Addr() == from)
            {
                Unlock();
                return s;
            }
        }
        Unlock();
        return NULL;
    }

    S2FSSegment *S2FileSystem::ReadSegmentFromDisk()
    {
        return ReadSegmentFromDisk(_wp_end);
    }


    S2FSSegment *S2FileSystem::ReadSegmentFromDisk(uint64_t from)
    {
        uint64_t segm_start = segment_2_addr(addr_2_segment(from));
        S2FSSegment *s = new S2FSSegment(segm_start);
        char *buf = (char *)calloc(S2FSSegment::Size(), sizeof(char));

        int ret = zns_udevice_read(_zns_dev, segm_start, buf, S2FSSegment::Size());
        if (ret)
        {
            std::cout << "Error: reading segment from WP, ret: " << ret << "\n";
            return NULL;
        }

        s->Deserialize(buf);
        // First segment is empty. So we have a brand new flash here.
        // set up root directory
        if (!segm_start && s->GetEmptyBlockNum() == _zns_dev_ex->blocks_per_zone - 1)
        {
            S2FSBlock *b;
            s->Allocate("/", ITYPE_DIR_INODE, S2FSBlock::Size(), &b);
        }

        Lock();
        if (_cache.size() >= CACHE_SEG_THRESHOLD)
        {
            auto ptr = _cache.front();
            ptr->WriteLock();
            _cache.pop_front();
            ptr->Unlock();
            delete ptr;
        }
        _cache.push_back(s);
        Unlock();

        return s;
    }

    S2FSSegment *S2FileSystem::ReadSegment(uint64_t from)
    {
        auto s = ReadSegmentFromCache(from);
        if (s)
            return s;

        return ReadSegmentFromDisk(from);
    }

    S2FSSegment *S2FileSystem::FindNonFullSegment()
    {
        Lock();
        for (auto s : _cache)
        {
            s->ReadLock();
            if (s->GetEmptyBlockNum() >= 2)
            {
                s->Unlock();
                return s;
            }
            s->Unlock();
        }

        for (uint64_t i = 0; i < _zns_dev->capacity_bytes; i += S2FSSegment::Size())
        {
            // should save some time here
            for (auto s : _cache)
            {
                if (s->Addr() == i)
                    continue;
            }

            auto seg = ReadSegmentFromDisk(i);
            if (seg->GetEmptyBlockNum() >= 2)
            {
                return seg;
            }
        }
        
        Unlock();
        return NULL;
    }

    bool S2FileSystem::DirectoryLookUp(std::string &name, S2FSBlock *parent, S2FSBlock **res)
    {
        auto del_pos = name.find('/');
        S2FSBlock *block = NULL;
        std::string next;
        if (del_pos == 0)
        {
            S2FSSegment *segment = ReadSegment(0);
            block = segment->LookUp("/");
            next = name.substr(del_pos + 1, name.length() - del_pos - 1);
        }
        else if (name.length() != 0)
        {
            std::string n;
            if (del_pos != name.npos)
            {
                n = name.substr(0, del_pos);
                next = name.substr(del_pos + 1, name.length() - del_pos - 1);
            }
            else
            {
                n = name;
            }
            block = parent->DirectoryLookUp(n);
        }

        if (block)
        {
            if (!next.empty())
            {
                auto ret = DirectoryLookUp(next, block, res);
                if (*res != block)
                    delete block;
                return ret;
            }
            else
            {
                *res = block;
                return true;
            }
        }
        else
        {
            *res = parent;
            return false;
        }
    }

    // Create a brand new sequentially-readable file with the specified name.
    // On success, stores a pointer to the new file in *result and returns OK.
    // On failure stores nullptr in *result and returns non-OK.  If the file does
    // not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    IOStatus S2FileSystem::NewSequentialFile(const std::string &fname, const FileOptions &file_opts,
                                             std::unique_ptr<FSSequentialFile> *result, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::IsDirectory(const std::string &, const IOOptions &options, bool *is_dir, IODebugContext *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Create a brand new random access read-only file with the
    // specified name.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores nullptr in *result and
    // returns non-OK.  If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    IOStatus S2FileSystem::NewRandomAccessFile(const std::string &fname, const FileOptions &file_opts,
                                               std::unique_ptr<FSRandomAccessFile> *result, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    const char *S2FileSystem::Name() const {
        return "S2FileSytem";
    }

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a
    // new file.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores nullptr in *result and
    // returns non-OK.
    //
    // The returned file will only be accessed by one thread at a time.
    IOStatus S2FileSystem::NewWritableFile(const std::string &fname, const FileOptions &file_opts,
                                           std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::ReopenWritableFile(const std::string &, const FileOptions &, std::unique_ptr<FSWritableFile> *,
                                              IODebugContext *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NewRandomRWFile(const std::string &, const FileOptions &, std::unique_ptr<FSRandomRWFile> *,
                                           IODebugContext *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NewMemoryMappedFileBuffer(const std::string &, std::unique_ptr<MemoryMappedFileBuffer> *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Create an object that represents a directory. Will fail if directory
    // doesn't exist. If the directory exists, it will open the directory
    // and create a new Directory object.
    //
    // On success, stores a pointer to the new Directory in
    // *result and returns OK. On failure stores nullptr in *result and
    // returns non-OK.
    IOStatus
    S2FileSystem::NewDirectory(const std::string &name, const IOOptions &io_opts, std::unique_ptr<FSDirectory> *result,
                               IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetFreeSpace(const std::string &, const IOOptions &, uint64_t *, IODebugContext *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::Truncate(const std::string &, size_t, const IOOptions &, IODebugContext *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Create the specified directory. Returns error if directory exists.
    IOStatus S2FileSystem::CreateDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Creates directory if missing. Return Ok if it exists, or successful in
    // Creating.
    IOStatus S2FileSystem::CreateDirIfMissing(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) {
        std::string name = dirname;
        S2FSBlock *inode;
        auto exist = DirectoryLookUp(name, NULL, &inode);
        if (exist)
            return IOStatus::OK();

        auto segment = FindNonFullSegment();
        // segment->Allocate(dir);
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus
    S2FileSystem::GetFileSize(const std::string &fname, const IOOptions &options, uint64_t *file_size, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::DeleteDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetFileModificationTime(const std::string &fname, const IOOptions &options, uint64_t *file_mtime,
                                                   IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetAbsolutePath(const std::string &db_path, const IOOptions &options, std::string *output_path,
                                           IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::DeleteFile(const std::string &fname, const IOOptions &options, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NewLogger(const std::string &fname, const IOOptions &io_opts, std::shared_ptr<Logger> *result,
                                     IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetTestDirectory(const IOOptions &options, std::string *path, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Release the lock acquired by a previous successful call to LockFile.
    // REQUIRES: lock was returned by a successful LockFile() call
    // REQUIRES: lock has not already been unlocked.
    IOStatus S2FileSystem::UnlockFile(FileLock *lock, const IOOptions &options, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Lock the specified file.  Used to prevent concurrent access to
    // the same db by multiple processes.  On failure, stores nullptr in
    // *lock and returns non-OK.
    //
    // On success, stores a pointer to the object that represents the
    // acquired lock in *lock and returns OK.  The caller should call
    // UnlockFile(*lock) to release the lock.  If the process exits,
    // the lock will be automatically released.
    //
    // If somebody else already holds the lock, finishes immediately
    // with a failure.  I.e., this call does not wait for existing locks
    // to go away.
    //
    // May create the named file if it does not already exist.
    IOStatus S2FileSystem::LockFile(const std::string &fname, const IOOptions &options, FileLock **lock, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus
    S2FileSystem::AreFilesSame(const std::string &, const std::string &, const IOOptions &, bool *, IODebugContext *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NumFileLinks(const std::string &, const IOOptions &, uint64_t *, IODebugContext *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::LinkFile(const std::string &, const std::string &, const IOOptions &, IODebugContext *) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::RenameFile(const std::string &src, const std::string &target, const IOOptions &options,
                                      IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetChildrenFileAttributes(const std::string &dir, const IOOptions &options,
                                                     std::vector<FileAttributes> *result, IODebugContext *dbg) {
        return FileSystem::GetChildrenFileAttributes(dir, options, result, dbg);
    }

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    // Returns OK if "dir" exists and "*result" contains its children.
    //         NotFound if "dir" does not exist, the calling process does not have
    //                  permission to access "dir", or if "dir" is invalid.
    //         IOError if an IO Error was encountered
    IOStatus S2FileSystem::GetChildren(const std::string &dir, const IOOptions &options, std::vector<std::string> *result,
                                       IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Returns OK if the named file exists.
    //         NotFound if the named file does not exist,
    //                  the calling process does not have permission to determine
    //                  whether this file exists, or if the path is invalid.
    //         IOError if an IO Error was encountered
    IOStatus S2FileSystem::FileExists(const std::string &fname, const IOOptions &options, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus
    S2FileSystem::ReuseWritableFile(const std::string &fname, const std::string &old_fname, const FileOptions &file_opts,
                                    std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) {
        return IOStatus::IOError(__FUNCTION__);
    }
}