#pragma once


#include <io/socket.h>


namespace tyrtech::net {


class socket_writer
{
public:
    uint32_t write(const char* data, uint32_t size);

public:
    socket_writer(io::socket* socket, uint64_t timeout);

private:
    io::socket* m_socket{nullptr};
    uint64_t m_timeout{0};
};

}
