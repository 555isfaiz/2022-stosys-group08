#include "S2FSImpl.h"

namespace ROCKSDB_NAMESPACE
{
    IOStatus S2FSWritableFile::Append(const Slice &data, const IOOptions &options,
                                IODebugContext *dbg)
    {
        if (_inode->DataAppend(data.data(), data.size()))
        {
            return IOStatus::IOError();
        }
        return IOStatus::OK();
    }
}