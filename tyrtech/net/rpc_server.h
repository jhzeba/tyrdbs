#pragma once


#include <common/disallow_copy.h>
#include <common/disallow_move.h>
#include <common/buffered_reader.h>
#include <common/logger.h>
#include <net/rpc_response.h>
#include <net/server_exception.h>

#include <unordered_set>
#include <array>


namespace tyrtech::net {


template<typename T>
class rpc_server : private disallow_copy, disallow_move
{
public:
    void terminate()
    {
        m_socket->disconnect();

        for (auto&& remote : m_remotes)
        {
            remote->socket()->disconnect();
        }
    }

    std::string_view uri() const
    {
        return m_socket->uri();
    }

public:
    rpc_server(std::shared_ptr<io::socket> socket, T* service)
      : m_socket(std::move(socket))
      , m_service(service)
    {
        gt::create_thread(&rpc_server::server_thread, this);
    }

private:
    using socket_ptr =
            std::shared_ptr<io::socket>;

    using remotes_t =
            std::unordered_set<socket_channel*>;

    using buffer_t =
            std::array<char, socket_channel::buffer_size>;

private:
    socket_ptr m_socket;
    remotes_t m_remotes;

    T* m_service{nullptr};

private:
    void server_thread()
    {
        logger::debug("{}: server started", m_socket->uri());

        while (true)
        {
            socket_ptr remote;

            try
            {
                remote = m_socket->accept();
            }
            catch (io::socket::disconnected_exception&)
            {
                logger::debug("{}: server shutdown", m_socket->uri());

                break;
            }

            gt::create_thread(&rpc_server::worker_thread, this, std::move(remote));
        }

        m_socket.reset();
    }

    void worker_thread(socket_ptr remote)
    {
        socket_channel channel(std::move(remote), 0);

        logger::debug("{}: connected", channel.socket()->uri());

        m_remotes.insert(&channel);

        try
        {
            auto ctx = m_service->create_context(&channel);

            while (true)
            {
                buffer_t request_buffer;
                uint16_t* request_size = reinterpret_cast<uint16_t*>(request_buffer.data());

                channel.read(request_size);

                if (unlikely(*request_size > socket_channel::buffer_size - sizeof(uint16_t)))
                {
                    logger::error("{}: request too big, disconnecting...",
                                  channel.socket()->uri());

                    break;
                }

                channel.read(request_buffer.data() + sizeof(uint16_t), *request_size);

                message::parser parser(request_buffer.data(),
                                       *request_size + sizeof(uint16_t));
                service::request_parser request(&parser, 0);

                try
                {
                    m_service->process_message(request, &ctx);
                }
                catch (server_error_exception& e)
                {
                    logger::error("{}: {}", channel.socket()->uri(), e.what());

                    rpc_response<std::void_t<>> response(&channel);

                    auto error = response.add_error();

                    error.add_code(e.code());
                    error.add_message(e.what());

                    response.send();
                }

                channel.flush();
            }
        }
        catch (io::socket::disconnected_exception&)
        {
            logger::debug("{}: disconnected", channel.socket()->uri());
        }
        catch (message::malformed_message_exception&)
        {
            logger::error("{}: invalid message, disconnecting...", channel.socket()->uri());
        }

        m_remotes.erase(&channel);
    }
};

}
