#pragma once


#include <common/buffered_reader.h>
#include <net/socket_channel.h>
#include <net/service.json.h>


namespace tyrtech::net {


template<typename Function>
class rpc_request : private disallow_copy
{
public:
    decltype(auto) add_message()
    {
        return typename Function::request_builder_t(m_request.add_message());
    }

    void execute()
    {
        m_request.finalize();

        m_channel->write(m_buffer.data(), m_builder.size());
    }

    decltype(auto) wait()
    {
        uint16_t* size = reinterpret_cast<uint16_t*>(m_buffer.data());

        m_channel->read(size);

        if (unlikely(*size > socket_channel::buffer_size - sizeof(uint16_t)))
        {
            throw runtime_error_exception("{}: response message too big",
                                          m_channel->uri());
        }

        m_channel->read(m_buffer.data() + sizeof(uint16_t), *size);

        m_parser = message::parser(m_buffer.data(), *size + sizeof(uint16_t));
        m_response = service::response_parser(&m_parser, 0);

        if (unlikely(m_response.has_error() == true))
        {
            Function::throw_exception(m_response.error());
        }

        return typename Function::response_parser_t(m_response.get_parser(),
                                                    m_response.message());
    }

public:
    rpc_request(socket_channel* channel)
      : m_channel(channel)
    {
        m_request.set_module(Function::module_id);
        m_request.set_function(Function::id);
    }

private:
    using buffer_t =
            std::array<char, socket_channel::buffer_size>;

private:
    socket_channel* m_channel{nullptr};

    buffer_t m_buffer;

    message::builder m_builder{m_buffer.data(),
                               static_cast<uint16_t>(m_buffer.size())};
    service::request_builder m_request{&m_builder};

    message::parser m_parser;
    service::response_parser m_response;
};

}
