#include <common/branch_prediction.h>
#include <io/file_channel.h>

#include <cassert>


namespace tyrtech::io {


uint32_t file_channel::pread(uint64_t offset, char* data, uint32_t size) const
{
    auto res = m_file->pread(offset, data, size);

    if (res != static_cast<int32_t>(size))
    {
        throw file::exception("{}: unable to read");
    }

    return size;
}

uint32_t file_channel::write(const char* data, uint32_t size)
{
    auto res = m_file->pwrite(m_offset, data, size);

    if (res != static_cast<int32_t>(size))
    {
        throw file::exception("{}: unable to write");
    }

    m_offset += size;

    return size;
}

uint64_t file_channel::offset() const
{
    return m_offset;
}

file_channel::file_channel(file* file)
  : m_file(file)
{
}

}
