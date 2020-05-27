#pragma once


#include <common/disallow_copy.h>
#include <common/disallow_move.h>
#include <common/logger.h>
#include <net/socket_channel.h>
#include <net/server_exception.h>
#include <net/http_request.h>

#include <unordered_set>
#include <array>


namespace tyrtech::net {


template<uint16_t buffer_size, typename T>
class http_server : private disallow_copy, disallow_move
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
    http_server(std::shared_ptr<io::socket> socket, T* service)
      : m_socket(std::move(socket))
      , m_service(service)
    {
        gt::create_thread(&http_server::server_thread, this);
    }

private:
    using socket_ptr =
            std::shared_ptr<io::socket>;

    using remotes_t =
            std::unordered_set<socket_channel*>;

    using buffer_t =
            std::array<char, buffer_size>;

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

            gt::create_thread(&http_server::worker_thread, this, std::move(remote));
        }

        m_socket.reset();
    }

    void worker_thread(socket_ptr remote)
    {
        socket_channel channel(std::move(remote), 0);

        logger::debug("{}: connected", remote->uri());

        m_remotes.insert(&channel);

        try
        {
            auto ctx = m_service->create_context(&channel);

            while (true)
            {
                buffer_t http_buffer;

                try
                {
                    auto request = http::request::parse(&http_buffer, &channel);
                    m_service->process_request(request, &ctx);
                }
                catch (http::exception& e)
                {
                    logger::error("{}: {}, disconnecting...", channel.socket()->uri(), e.what());

                    char buff[512];
                    auto response = format_to(buff, sizeof(buff),
                                              "HTTP/1.1 {}\r\n"
                                              "Content-Length: 0\r\n"
                                              "Connection-Type: Close\r\n"
                                              "\r\n", e.what());

                    channel.write(response.data(), response.size());
                    channel.flush();
                }

                channel.flush();
            }
        }
        catch (io::socket::disconnected_exception&)
        {
            logger::debug("{}: disconnected", channel.socket()->uri());
        }

        m_remotes.erase(&channel);
    }
};

}
