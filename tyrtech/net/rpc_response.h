#pragma once


#include <common/disallow_copy.h>
#include <net/socket_channel.h>
#include <net/service.json.h>


namespace tyrtech::net {


template<typename Function>
class rpc_response : private disallow_copy
{
public:
    decltype(auto) add_error()
    {
        return m_response.add_error();
    }

    decltype(auto) add_message()
    {
        return typename Function::response_builder_t(m_response.add_message());
    }

    void send()
    {
        m_response.finalize();

        m_channel->write(m_buffer.data(), m_builder.size());
    }

public:
    rpc_response(socket_channel* channel)
      : m_channel(channel)
    {
    }

private:
    using buffer_t =
            std::array<char, socket_channel::buffer_size>;

private:
    socket_channel* m_channel{nullptr};

    buffer_t m_buffer;

    message::builder m_builder{m_buffer.data(),
                               static_cast<uint16_t>(m_buffer.size())};
    service::response_builder m_response{&m_builder};
};

}
