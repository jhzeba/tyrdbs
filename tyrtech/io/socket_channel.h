#pragma once


#include <io/socket.h>


namespace tyrtech::io {


class socket_channel
{
public:
    uint32_t write(const char* data, uint32_t size);
    uint32_t read(char* data, uint32_t size);

public:
    socket_channel(socket* socket, uint64_t timeout);

private:
    socket* m_socket{nullptr};
    uint64_t m_timeout{0};
};

}
