#include <net/socket_reader.h>


namespace tyrtech::net {


uint32_t socket_reader::read(char* data, uint32_t size)
{
    return m_socket->recv(data, size, m_timeout);
}

socket_reader::socket_reader(io::socket* socket, uint64_t timeout)
  : m_socket(socket)
  , m_timeout(timeout)
{
}

}
