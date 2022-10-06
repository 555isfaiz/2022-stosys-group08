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
#include <unistd.h>
#include <thread>

#include <stosys_debug.h>
#include <utils.h>

namespace ROCKSDB_NAMESPACE
{
    void *GCWrapper(void *args)
    {
        struct GCWrapperArg *gc_arg = (struct GCWrapperArg *)args;
        if (gc_arg->fs == NULL)
            return (void *)0;

        auto now = microseconds_since_epoch();
        for (uint32_t i = 0; i < gc_arg->seg_num; i++)
        {
            auto s = gc_arg->fs->ReadSegment(i * S2FSSegment::Size());
            if (now - s->LastModify() > 300000000)  // 5 min
            {
                s->OnGC();
            }
        }
        return (void *)1;
    }

    S2FileSystem::S2FileSystem(std::string uri_db_path, bool debug)
    {
        FileSystem::Default();

        this->_seq_id = 0;
        this->_name = "S2FileSystem forward to ";
        // Do not know the meaning of Filesystem default
        // this->_name = this->_name.append(FileSystem::Default()->Name());
        this->_ss.str("");
        this->_ss.clear();

        std::string sdelimiter = ":";
        std::string edelimiter = "://";
        this->_uri = uri_db_path;
        struct zdev_init_params params;
        std::string device = uri_db_path.substr(uri_db_path.find(sdelimiter) + sdelimiter.size(),
                                                uri_db_path.find(edelimiter) -
                                                    (uri_db_path.find(sdelimiter) + sdelimiter.size()));
        // make sure to setup these parameters properly and check the forced reset flag for M5
        params.name = strdup(device.c_str());
        params.log_zones = 3;
        params.gc_wmark = 1;
        params.force_reset = true;
        int ret = init_ss_zns_device(&params, &this->_zns_dev);
        if (ret != 0)
        {
            std::cout << "Error: " << uri_db_path << " failed to open the device " << device.c_str() << "\n";
            std::cout << "Error: ret " << ret << "\n";
        }
        free(params.name);
        pool_init(&_thread_pool, 4);
        this->_zns_dev_ex = (struct zns_device_extra_info *)this->_zns_dev->_private;
        assert(ret == 0);
        assert(this->_zns_dev->lba_size_bytes != 0);
        assert(this->_zns_dev->capacity_bytes != 0);
        ss_dprintf(DBG_FS_1, "device %s is opened and initialized, reported LBA size is %u and capacity %lu \n",
                   device.c_str(), this->_zns_dev->lba_size_bytes, this->_zns_dev->capacity_bytes);

        S2FSObject::_fs = this;
        size_t segments = _zns_dev->capacity_bytes / S2FSSegment::Size();
        for (size_t i = 0; i < segments; i++)
        {
            char buf[S2FSBlock::Size()] = {0};
            uint64_t segm_start = i * S2FSSegment::Size();
            S2FSSegment *s = new S2FSSegment(segm_start);

            int ret = zns_udevice_read(_zns_dev, segm_start, buf, S2FSBlock::Size());
            if (ret)
            {
                std::cout << "Error: reading first block, ret: " << ret << " addr: " << segm_start << "\n";
                continue;
            }

            s->Preload(buf);
            _cache[segm_start] = s;

            // First segment is empty. So we have a brand new flash here.
            // set up root directory
            if (!segm_start && s->IsEmpty())
            {
                S2FSBlock *b;
                s->AllocateNew("/", ITYPE_DIR_INODE, &b, NULL);
            }

            if (!s->IsEmpty())
                _wp_end = segm_start;

            s->LastModify(microseconds_since_epoch());
        }

        for (int i = 0; i < 4; i++)
        {
            struct GCWrapperArg *arg = (struct GCWrapperArg *)calloc(1, sizeof(struct GCWrapperArg));
            _gc_args[i] = arg;
            arg->fs = this;
            auto each = _cache.size() / 4;
            if (i == 3)
                arg->seg_num = each;
            else
                arg->seg_num = _cache.size() - each * 3;
            
            arg->seg_start = i * each * S2FSSegment::Size();

            pool_exec(_thread_pool, GCWrapper, arg);
        }
    }

    S2FileSystem::~S2FileSystem()
    {
        for (int i = 0; i < 4; i++)
        {
            struct GCWrapperArg *arg = _gc_args[i];
            arg->fs = NULL;
        }

        for (auto p : _cache)
        {
            delete p.second;
        }
        deinit_ss_zns_device(_zns_dev);
    }

    std::string S2FileSystem::get_seq_id()
    {
        this->_ss.str("");
        this->_ss.clear();
        this->_ss << " call_seq: " << this->_seq_id++ << " tid: " << std::hash<std::thread::id>{}(std::this_thread::get_id()) << " ";
        return this->_ss.str();
    }

    S2FSSegment *S2FileSystem::ReadSegment(uint64_t from)
    {
        if (!map_contains(_cache, from))
        {
            return NULL;
        }
        
        return _cache[from];
    }

    S2FSSegment *S2FileSystem::LoadSegmentFromDisk()
    {
        return LoadSegmentFromDisk(_wp_end);
    }

    S2FSSegment *S2FileSystem::LoadSegmentFromDisk(uint64_t from)
    {
        uint64_t segm_start = segment_2_addr(addr_2_segment(from));
        S2FSSegment *s = _cache[segm_start];
        char buf[S2FSSegment::Size()] = {0};

        int ret = zns_udevice_read(_zns_dev, segm_start, buf, S2FSSegment::Size());
        if (ret)
        {
            std::cout << "Error: reading segment from WP, ret: " << ret << "\n";
            return NULL;
        }

        s->Deserialize(buf);

        return s;
    }

    S2FSSegment *S2FileSystem::FindNonFullSegment()
    {
    start:
        S2FSSegment *seg = ReadSegment(_wp_end);
        seg->ReadLock();
        if (seg->CurSize() < S2FSSegment::Size() - S2FSBlock::Size())
        {
            seg->Unlock();
            return seg;
        }
        seg->Unlock();

        if (_wp_end < _zns_dev->capacity_bytes - S2FSSegment::Size())
        {
            _wp_end += S2FSSegment::Size();
            goto start;
        }

        for (uint64_t i = 0; i < _zns_dev->capacity_bytes; i += S2FSSegment::Size())
        {
            seg = ReadSegment(i);
            seg->ReadLock();
            if (seg->CurSize() < S2FSSegment::Size() - S2FSBlock::Size())
            {
                seg->Unlock();
                return seg;
            }
            seg->Unlock();
            seg->OnGC();
        }

        std::cout << "Disk full"
                  << "\n";
        return NULL;
    }

    bool S2FileSystem::DirectoryLookUp(std::string &name, S2FSBlock *parent, bool set_parent, S2FSBlock **res)
    {
        auto del_pos = name.find(_fs_delimiter);
        S2FSBlock *block = NULL;
        std::string next;
        if (del_pos == 0)
        {
            S2FSSegment *segment = ReadSegment(0);
            segment->WriteLock();
            block = segment->LookUp("/");
            segment->Unlock();
            next = name.substr(del_pos + 1, name.length() - del_pos - 1);
            if (!next.empty() && next.at(0) == '/')
            {
                next = next.substr(1, name.length() - 1);
            }
        }
        else if (name.length() != 0)
        {
            std::string n;
            if (del_pos != name.npos)
            {
                n = name.substr(0, del_pos);
                next = name.substr(del_pos + 1, name.length() - del_pos - 1);
                if (!next.empty() && next.at(0) == '/')
                {
                    next = next.substr(1, name.length() - 1);
                }
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
                return DirectoryLookUp(next, block, set_parent, res);
            }
            else
            {
                if (set_parent)
                    *res = parent;
                else
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

    std::string strip_name(const std::string &fname, const std::string &delimiter)
    {
        size_t pos;
        std::string name = fname;
        while ((pos = name.find(delimiter)) != name.npos)
        {
            if (pos == 0)
                name = name.substr(1, name.length() - 1);
            else
                name = name.substr(pos + 1, name.length() - pos - 1);
        }
        return name;
    }

    // Look up the file indicated by fname, and set res to the inode of that file
    // If the file does not exist, res will be set to the inode of the deepest existing directory of fname
    IOStatus S2FileSystem::_FileExists(const std::string &fname, bool set_parent, S2FSBlock **res)
    {
        std::string name = fname;
        auto exist = DirectoryLookUp(name, NULL, set_parent, res);
        if (exist)
            return IOStatus::OK();
        else
            return IOStatus::NotFound();
    }

    // Create a brand new sequentially-readable file with the specified name.
    // On success, stores a pointer to the new file in *result and returns OK.
    // On failure stores nullptr in *result and returns non-OK.  If the file does
    // not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    // Needed
    IOStatus S2FileSystem::NewSequentialFile(const std::string &fname, const FileOptions &file_opts,
                                             std::unique_ptr<FSSequentialFile> *result, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
         auto r = _FileExists(fname, false, &inode);
        if (!r.ok())
            return r;
        result->reset(new S2FSSequentialFile(inode));
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::IsDirectory(const std::string &, const IOOptions &options, bool *is_dir, IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Create a brand new random access read-only file with the
    // specified name.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores nullptr in *result and
    // returns non-OK.  If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    // Needed
    IOStatus S2FileSystem::NewRandomAccessFile(const std::string &fname, const FileOptions &file_opts,
                                               std::unique_ptr<FSRandomAccessFile> *result, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        auto r = _FileExists(fname, false, &inode);
        if (!r.ok())
            return r;
        result->reset(new S2FSRandomAccessFile(inode));
        return IOStatus::OK();
    }

    const char *S2FileSystem::Name() const
    {
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
                                           std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        S2FSSegment *s;
        if (_FileExists(fname, false, &inode).ok())
        {
            s = ReadSegment(inode->SegmentAddr());
            s->Free(inode->ID());
            _FileExists(fname, false, &inode); // set inode to the parent dir
        }

        bool allocated = false;
        S2FSBlock *new_inode;
        while (s = FindNonFullSegment())
        {
            if (s->AllocateNew(strip_name(fname, _fs_delimiter), ITYPE_FILE_INODE, &new_inode, inode) >= 0)
            {
                allocated = true;
                break;
            }
        }

        if (allocated)
        {
            result->reset(new S2FSWritableFile(new_inode));
            return IOStatus::OK();
        }
        else
        {
            return IOStatus::IOError(__FUNCTION__);
        }
    }

    IOStatus S2FileSystem::ReopenWritableFile(const std::string &, const FileOptions &, std::unique_ptr<FSWritableFile> *,
                                              IODebugContext *)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NewRandomRWFile(const std::string &, const FileOptions &, std::unique_ptr<FSRandomRWFile> *,
                                           IODebugContext *)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NewMemoryMappedFileBuffer(const std::string &, std::unique_ptr<MemoryMappedFileBuffer> *)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
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
                               IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        auto r = _FileExists(name, false, &inode);
        if (!r.ok())
            return r;
        result->reset(new S2FSDirectory(inode));
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::GetFreeSpace(const std::string &, const IOOptions &, uint64_t *, IODebugContext *)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::Truncate(const std::string &, size_t, const IOOptions &, IODebugContext *)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    // Create the specified directory. Returns error if directory exists.
    IOStatus S2FileSystem::CreateDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        if (_FileExists(dirname, false, &inode).ok())
            return IOStatus::IOError(__FUNCTION__);

        auto to_create = dirname.substr(inode->Name().length(), dirname.length() - inode->Name().length());
        while (!to_create.empty())
        {
            S2FSBlock *res;
            uint64_t pos = to_create.find(_fs_delimiter);
            auto name = to_create.substr(0, pos == to_create.npos ? to_create.length() : pos);
            S2FSSegment *s;
            bool allocated = false;
            while (s = FindNonFullSegment())
            {
                if (s->AllocateNew(name, ITYPE_DIR_INODE, &res, inode) >= 0)
                {
                    allocated = true;
                    break;
                }
            }

            if (!allocated)
            {
                return IOStatus::IOError(__FUNCTION__);
            }
            inode = res;
            to_create = to_create.substr(name.length() + 1, to_create.length() - name.length());
            if (to_create == "/")
                break;
        }
        return IOStatus::OK();
    }

    // Creates directory if missing. Return Ok if it exists, or successful in
    // Creating.
    IOStatus S2FileSystem::CreateDirIfMissing(const std::string &dirname, const IOOptions &options, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        if (_FileExists(dirname, false, &inode).ok())
            return IOStatus::OK();

        return CreateDir(dirname, options, dbg);
    }

    IOStatus
    S2FileSystem::GetFileSize(const std::string &fname, const IOOptions &options, uint64_t *file_size, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::DeleteDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetFileModificationTime(const std::string &fname, const IOOptions &options, uint64_t *file_mtime,
                                                   IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    // Needed
    IOStatus S2FileSystem::GetAbsolutePath(const std::string &db_path, const IOOptions &options, std::string *output_path,
                                           IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        if (!db_path.empty() && db_path[0] == '/')
        {
            *output_path = db_path;
            return IOStatus::OK();
        }

        char the_path[4096];
        char *ret = getcwd(the_path, 4096);
        if (ret == nullptr)
        {
            return IOStatus::IOError(__FUNCTION__);
        }

        *output_path = ret;
        return IOStatus::OK();
    }

    // Needed
    IOStatus S2FileSystem::DeleteFile(const std::string &fname, const IOOptions &options, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        S2FSSegment *s;
        if (!_FileExists(fname, false, &inode).ok())
        {
            return IOStatus::NotFound(__FUNCTION__);
        }

        s = ReadSegment(inode->SegmentAddr());
        s->Free(inode->ID());
        return IOStatus::OK();
    }

    // Needed
    IOStatus S2FileSystem::NewLogger(const std::string &fname, const IOOptions &io_opts, std::shared_ptr<Logger> *result,
                                     IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetTestDirectory(const IOOptions &options, std::string *path, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    // Release the lock acquired by a previous successful call to LockFile.
    // REQUIRES: lock was returned by a successful LockFile() call
    // REQUIRES: lock has not already been unlocked.
    IOStatus S2FileSystem::UnlockFile(FileLock *lock, const IOOptions &options, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSFileLock *fl = dynamic_cast<S2FSFileLock *>(lock);
        if (fl->Unlock())
        {
            return IOStatus::IOError(__FUNCTION__);
        }
        else
        {
            delete fl;
            return IOStatus::OK();
        }
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
    IOStatus S2FileSystem::LockFile(const std::string &fname, const IOOptions &options, FileLock **lock, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        S2FSSegment *s;
        S2FSBlock *new_inode;
        if (!_FileExists(fname, false, &inode).ok())
        {
            bool allocated = false;
            while (s = FindNonFullSegment())
            {
                if (s->AllocateNew(strip_name(fname, _fs_delimiter), ITYPE_FILE_INODE, &new_inode, inode) >= 0)
                {
                    allocated = true;
                    break;
                }
            }

            if (!allocated)
                return IOStatus::IOError(__FUNCTION__);
        }

        auto fl = new S2FSFileLock(new_inode);
        if (fl->Lock())
        {
            delete fl;
            *lock = NULL;
            return IOStatus::IOError(__FUNCTION__);
        }
        else
        {
            *lock = fl;
            return IOStatus::OK();
        }
    }

    IOStatus
    S2FileSystem::AreFilesSame(const std::string &, const std::string &, const IOOptions &, bool *, IODebugContext *)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NumFileLinks(const std::string &, const IOOptions &, uint64_t *, IODebugContext *)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::LinkFile(const std::string &, const std::string &, const IOOptions &, IODebugContext *)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::RenameFile(const std::string &src, const std::string &target, const IOOptions &options,
                                      IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        if (_FileExists(src, true, &inode).IsNotFound())
            return IOStatus::NotFound();

        inode->RenameChild(strip_name(src, _fs_delimiter), strip_name(target, _fs_delimiter));
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::GetChildrenFileAttributes(const std::string &dir, const IOOptions &options,
                                                     std::vector<FileAttributes> *result, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return FileSystem::GetChildrenFileAttributes(dir, options, result, dbg);
    }

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    // Returns OK if "dir" exists and "*result" contains its children.
    //         NotFound if "dir" does not exist, the calling process does not have
    //                  permission to access "dir", or if "dir" is invalid.
    //         IOError if an IO Error was encountered
    // Needed
    IOStatus S2FileSystem::GetChildren(const std::string &dir, const IOOptions &options, std::vector<std::string> *result,
                                       IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        auto r = _FileExists(dir, true, &inode);
        if (!r.ok())
            return r;

        if (inode->ReadChildren(result))
            return IOStatus::IOError();
        return IOStatus::OK();
    }

    // Returns OK if the named file exists.
    //         NotFound if the named file does not exist,
    //                  the calling process does not have permission to determine
    //                  whether this file exists, or if the path is invalid.
    //         IOError if an IO Error was encountered
    IOStatus S2FileSystem::FileExists(const std::string &fname, const IOOptions &options, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        S2FSBlock *inode;
        return _FileExists(fname, false, &inode);
    }

    IOStatus
    S2FileSystem::ReuseWritableFile(const std::string &fname, const std::string &old_fname, const FileOptions &file_opts,
                                    std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg)
    {
        // std::cout << get_seq_id() << " func: " << __FUNCTION__ << " line: " << __LINE__ << " " << std::endl;
        return IOStatus::IOError(__FUNCTION__);
    }
}