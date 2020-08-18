#pragma once


#include <io/file.h>
#include <tyrdbs/reader.h>


namespace tyrtech::tyrdbs::meta_node {


class file_reader : public tyrdbs::reader
{
public:
    uint32_t pread(uint64_t offset, char* data, uint32_t size) const override;
    void unlink() override;

public:
    template<typename... Arguments>
    file_reader(Arguments&&... arguments)
      : m_file(io::file::open(io::file::access::read_only, std::forward<Arguments>(arguments)...))
    {
    }

private:
    io::file m_file;
};

}
