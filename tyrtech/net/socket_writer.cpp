#include <net/socket_writer.h>


namespace tyrtech::net {


uint32_t socket_writer::write(const char* data, uint32_t size)
{
    uint32_t _size{size};

    while (size != 0)
    {
        auto res = m_socket->send(data, size, m_timeout);

        data += res;
        size -= res;
    }

    return _size;
}

socket_writer::socket_writer(io::socket* socket, uint64_t timeout)
  : m_socket(socket)
  , m_timeout(timeout)
{
}

}
