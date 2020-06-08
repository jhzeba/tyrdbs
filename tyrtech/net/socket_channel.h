#pragma once


#include <common/buffered_reader.h>
#include <common/buffered_writer.h>
#include <net/socket_reader.h>
#include <net/socket_writer.h>


namespace tyrtech::net {


class socket_channel
{
public:
    static constexpr uint32_t buffer_size{8192};

public:
    uint32_t write(const char* data, uint32_t size);
    uint32_t read(char* data, uint32_t size);

    void flush();

    template<typename T>
    void read(T* data)
    {
        read(reinterpret_cast<char*>(data), sizeof(T));
    }

    char read()
    {
        return m_reader.read();
    }

    const std::shared_ptr<io::socket>& socket();

public:
    socket_channel(std::shared_ptr<io::socket> socket, uint64_t timeout);

private:
    using socket_ptr =
            std::shared_ptr<io::socket>;

    using buffer_t =
            std::array<char, buffer_size>;

    using reader_t =
            buffered_reader<buffer_t, socket_reader>;

    using writer_t =
            buffered_writer<buffer_t, socket_writer>;

private:
    socket_ptr m_socket;

    socket_reader m_socket_reader;
    buffer_t m_reader_buffer;
    reader_t m_reader{&m_reader_buffer, &m_socket_reader};

    socket_writer m_socket_writer;
    buffer_t m_writer_buffer;
    writer_t m_writer{&m_writer_buffer, &m_socket_writer};
};

}
