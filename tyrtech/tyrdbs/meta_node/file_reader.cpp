#include <tyrdbs/meta_node/file_reader.h>


namespace tyrtech::tyrdbs::meta_node {


uint32_t file_reader::pread(uint64_t offset, char* data, uint32_t size) const
{
    auto orig_size = size;

    while (size != 0)
    {
        auto res = m_file.pread(offset, data, size);

        if (res == 0)
        {
            throw io::file::exception("{}: unexpected end of file", m_file.path());
        }

        data += res;
        size -= res;

        offset += res;
    }

    return orig_size;
}

void file_reader::unlink()
{
    m_file.unlink();
}

}
