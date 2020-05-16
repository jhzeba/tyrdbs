#pragma once


#include <io/channel.h>
#include <io/socket.h>


namespace tyrtech::io {


class socket_channel : public channel
{
public:
    uint32_t write(const char* data, uint32_t size) override;
    uint32_t read(char* data, uint32_t size) override;

    uint64_t offset() const override;
    void set_offset(uint64_t offset) override;

public:
    socket_channel(socket* socket, uint64_t timeout);

private:
    socket* m_socket{nullptr};

    uint64_t m_timeout{0};
    uint64_t m_offset{0};
};

}
