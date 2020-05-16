#include <net/socket_channel.h>


namespace tyrtech::net {


uint32_t socket_channel::write(const char* data, uint32_t size)
{
    m_channel.write(data, size);

    return size;
}

uint32_t socket_channel::read(char* data, uint32_t size)
{
    m_reader.read(data, size);

    return size;
}

uint64_t socket_channel::offset() const
{
    return m_reader.offset();
}

void socket_channel::set_offset(uint64_t offset)
{
    m_channel.set_offset(offset);
}

std::string_view socket_channel::uri() const
{
    return m_socket->uri();
}

const std::shared_ptr<io::socket>& socket_channel::remote()
{
    return m_socket;
}

socket_channel::socket_channel(std::shared_ptr<io::socket> socket, uint64_t timeout)
  : m_socket(std::move(socket))
  , m_channel(m_socket.get(), timeout)
{
}

}
