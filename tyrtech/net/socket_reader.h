#pragma once


#include <io/socket.h>


namespace tyrtech::net {


class socket_reader
{
public:
    uint32_t read(char* data, uint32_t size);

public:
    socket_reader(io::socket* socket, uint64_t timeout);

private:
    io::socket* m_socket{nullptr};
    uint64_t m_timeout{0};
};

}
