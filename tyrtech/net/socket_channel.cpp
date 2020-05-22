#include <net/socket_channel.h>


namespace tyrtech::net {


uint32_t socket_channel::write(const char* data, uint32_t size)
{
    m_writer.write(data, size);

    return size;
}

uint32_t socket_channel::read(char* data, uint32_t size)
{
    m_reader.read(data, size);

    return size;
}

void socket_channel::flush()
{
    m_writer.flush();
}

const std::shared_ptr<io::socket>& socket_channel::socket()
{
    return m_socket;
}

socket_channel::socket_channel(std::shared_ptr<io::socket> socket, uint64_t timeout)
  : m_socket(std::move(socket))
  , m_channel(m_socket.get(), timeout)
{
}

}
