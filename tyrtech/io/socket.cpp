#include <common/branch_prediction.h>
#include <common/system_error.h>
#include <common/clock.h>
#include <io/engine.h>
#include <io/socket.h>
#include <io/queue_flow.h>

#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <cassert>


namespace tyrtech::io {


static thread_local std::unique_ptr<queue_flow> __queue_flow;


void socket::initialize(uint32_t queue_size)
{
    __queue_flow = std::make_unique<queue_flow>(queue_size);
}

uint32_t socket::recv(char* data, uint32_t size, uint64_t timeout)
{
    queue_flow::resource r(*__queue_flow);

    auto res = io::recv(m_fd, data, size, 0, timeout);

    if (likely(res > 0))
    {
        return static_cast<uint32_t>(res);
    }

    if (unlikely(res == 0))
    {
        throw disconnected_exception("{}", uri());
    }

    auto e = system_error();

    switch (e.code)
    {
        case ECONNRESET:
        {
            throw disconnected_exception("{}", uri());
        }
        case ECANCELED:
        {
            throw timeout_exception("{}", uri());
        }
        default:
        {
            throw runtime_error_exception("{}: {}", uri(), e.message);
        }
    }
}

uint32_t socket::send(const char* data, uint32_t size, uint64_t timeout)
{
    queue_flow::resource r(*__queue_flow);

    auto res = io::send(m_fd, data, size, 0, timeout);

    if (likely(res > 0))
    {
        return static_cast<uint32_t>(res);
    }

    if (unlikely(res == 0))
    {
        throw disconnected_exception("{}", uri());
    }

    auto e = system_error();

    switch (e.code)
    {
        case ECONNRESET:
        {
            throw disconnected_exception("{}", uri());
        }
        case ECANCELED:
        {
            throw timeout_exception("{}", uri());
        }
        default:
        {
            throw runtime_error_exception("{}: {}", uri(), e.message);
        }
    }
}

void socket::disconnect()
{
    ::shutdown(m_fd, SHUT_RDWR);
}

std::string_view socket::uri() const
{
    return m_uri_view;
}

socket::socket(int32_t fd)
  : m_fd(fd)
{
}

socket::~socket()
{
    if (unlikely(m_fd == -1))
    {
        return;
    }

    io::close(m_fd);
    m_fd = -1;
}

void socket::connect(const void* address, uint32_t address_size, uint64_t timeout)
{
    queue_flow::resource r(*__queue_flow);

    auto res = io::connect(m_fd,
                           reinterpret_cast<const sockaddr*>(address),
                           address_size,
                           timeout);

    if (res == 0)
    {
        return;
    }

    assert(likely(res == -1));

    auto e = system_error();

    switch (e.code)
    {
        case EADDRNOTAVAIL:
        case ECONNREFUSED:
        case ECONNRESET:
        case ENOENT:
        {
            throw unable_to_connect_exception("{}", uri());
        }
        case ECANCELED:
        {
            throw timeout_exception("{}", uri());
        }
        default:
        {
            throw runtime_error_exception("{}: {}", uri(), e.message);
        }
    }
}

void socket::listen(const void* address, uint32_t address_size)
{
    if (unlikely(::bind(m_fd,
                        reinterpret_cast<const sockaddr*>(address),
                        address_size) == -1))
    {
        auto error = system_error();

        if (error.code == EADDRINUSE)
        {
            throw address_in_use_exception("{}: {}", uri(), error.message);
        }
        else
        {
            throw runtime_error_exception("{}: {}", uri(), error.message);
        }
    }

    if (unlikely(::listen(m_fd, 1024) == -1))
    {
        throw runtime_error_exception("{}: {}", uri(), system_error().message);
    }
}

void socket::accept(int32_t* fd, void* address, uint32_t address_size)
{
    queue_flow::resource r(*__queue_flow);

    socklen_t addr_size = address_size;
    std::memset(address, 0, addr_size);

    *fd = io::accept(m_fd, reinterpret_cast<sockaddr*>(address), &addr_size, 0);

    if (likely(*fd != -1))
    {
        return;
    }

    auto e = system_error();

    switch (e.code)
    {
        case EINVAL:
        {
            throw disconnected_exception("{}", uri());
        }
        default:
        {
            throw runtime_error_exception("{}: {}", uri(), e.message);
        }
    }
}

}
