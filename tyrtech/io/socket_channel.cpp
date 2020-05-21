#include <common/branch_prediction.h>
#include <common/clock.h>
#include <io/socket_channel.h>

#include <cassert>


namespace tyrtech::io {


uint32_t socket_channel::write(const char* data, uint32_t size)
{
    uint32_t orig_size = size;
    uint64_t timeout = m_timeout;

    while (size != 0)
    {
        uint64_t t1, t2;

        t1 = clock::now();

        uint32_t res = m_socket->send(data, size, timeout);

        t2 = clock::now();

        uint64_t send_took = (t2 - t1) / 1000000;

        if (likely(timeout > send_took))
        {
            timeout -= send_took;
        }
        else
        {
            timeout = 1;
        }

        data += res;
        size -= res;
    }

    m_offset += orig_size;

    return orig_size;
}

uint32_t socket_channel::read(char* data, uint32_t size)
{
    size = m_socket->recv(data, size, m_timeout);
    m_offset += size;

    return size;
}

uint64_t socket_channel::offset() const
{
    return m_offset;
}

socket_channel::socket_channel(socket* socket, uint64_t timeout)
  : m_socket(socket)
  , m_timeout(timeout)
{
}

}
