#include <common/branch_prediction.h>
#include <io/file_channel.h>

#include <cassert>


namespace tyrtech::io {


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

uint32_t file_channel::read(char* data, uint32_t size)
{
    auto res = m_file->pread(m_offset, data, size);

    if (res != static_cast<int32_t>(size))
    {
        throw file::exception("{}: unable to read");
    }

    m_offset += size;

    return size;
}

uint64_t file_channel::offset() const
{
    return m_offset;
}

void file_channel::set_offset(uint64_t offset)
{
    m_offset = offset;
}

file_channel::file_channel(file* file)
  : m_file(file)
{
}

}
