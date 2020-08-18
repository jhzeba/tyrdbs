#include <tyrdbs/meta_node/file_writer.h>


namespace tyrtech::tyrdbs::meta_node {


uint32_t file_writer::write(const char* data, uint32_t size)
{
    m_writer.write(data, size);
    return size;
}

void file_writer::flush()
{
    m_writer.flush();
}

uint64_t file_writer::offset() const
{
    return m_writer.offset();
}

void file_writer::unlink()
{
    m_file.unlink();
}

std::string_view file_writer::path() const
{
    return m_file.path();
}

uint32_t file_writer::stream_writer::write(const char* data, uint32_t size)
{
    auto orig_size = size;

    while (size != 0)
    {
        auto res = m_file->pwrite(m_offset, data, size);
        assert(res != 0);

        data += res;
        size -= res;

        m_offset += res;
    }

    return orig_size;
}

uint64_t file_writer::stream_writer::offset() const
{
    return m_offset;
}

file_writer::stream_writer::stream_writer(io::file* file)
  : m_file(file)
{
}

}
