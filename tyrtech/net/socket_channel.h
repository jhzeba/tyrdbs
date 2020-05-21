#pragma once


#include <common/buffered_reader.h>
#include <common/buffered_writer.h>
#include <io/socket_channel.h>


namespace tyrtech::net {


class socket_channel : public io::channel
{
public:
    static constexpr uint32_t buffer_size{8192};

public:
    uint32_t write(const char* data, uint32_t size) override;
    uint32_t read(char* data, uint32_t size) override;

    uint64_t offset() const override;

    void flush();

    template<typename T>
    void read(T* data)
    {
        read(reinterpret_cast<char*>(data), sizeof(T));
    }

    std::string_view uri() const;
    const std::shared_ptr<io::socket>& remote();

public:
    socket_channel(std::shared_ptr<io::socket> socket, uint64_t timeout);

private:
    using socket_ptr =
            std::shared_ptr<io::socket>;

    using buffer_t =
            std::array<char, buffer_size>;

    using reader_t =
            buffered_reader<buffer_t, io::socket_channel>;

    using writer_t =
            buffered_writer<buffer_t, io::socket_channel>;

private:
    socket_ptr m_socket;

    io::socket_channel m_channel;

    buffer_t m_reader_buffer;
    reader_t m_reader{&m_reader_buffer, &m_channel};

    buffer_t m_writer_buffer;
    writer_t m_writer{&m_writer_buffer, &m_channel};
};

}
