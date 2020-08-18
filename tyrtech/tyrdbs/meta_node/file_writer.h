#pragma once


#include <common/buffered_writer.h>
#include <io/file.h>
#include <tyrdbs/writer.h>


namespace tyrtech::tyrdbs::meta_node {


class file_writer : public tyrdbs::writer
{
public:
    uint32_t write(const char* data, uint32_t size) override;
    void flush() override;

    uint64_t offset() const override;

    void unlink() override;
    std::string_view path() const;

public:
    template<typename... Arguments>
    file_writer(Arguments&&... arguments)
      : m_file(io::file::create(std::forward<Arguments>(arguments)...))
    {
    }

private:
    class stream_writer
    {
    public:
        uint32_t write(const char* data, uint32_t size);
        uint64_t offset() const;

    public:
        stream_writer(io::file* file);

    private:
        io::file* m_file{nullptr};
        uint64_t m_offset{0};
    };

private:
    using buffer_t =
            std::array<char, 8192>;

    using writer_t =
            buffered_writer<buffer_t, stream_writer>;

private:
    io::file m_file;

    buffer_t m_stream_buffer;
    stream_writer m_stream_writer{&m_file};

    writer_t m_writer{&m_stream_buffer, &m_stream_writer};
};

}
