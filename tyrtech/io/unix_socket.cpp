#include <common/branch_prediction.h>
#include <common/system_error.h>
#include <io/unix_socket.h>

#include <cassert>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>


namespace tyrtech::io::unix {


class unix_socket : public io::socket
{
public:
    static std::shared_ptr<io::socket> connect(const std::string_view& path, uint64_t timeout);
    static std::shared_ptr<io::socket> listen(const std::string_view& path);

public:
    std::shared_ptr<io::socket> accept() override;

public:
    unix_socket(int32_t fd, const sockaddr_un& addr);

private:
    void to_uri(const sockaddr_un& addr);
};


int32_t create_socket()
{
    int32_t s = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);

    if (unlikely(s == -1))
    {
        throw runtime_error_exception("socket(): {}", system_error().message);
    }

    return s;
}

sockaddr_un resolve(const std::string_view& path)
{
    sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;

    format_to(addr.sun_path, sizeof(addr.sun_path), "{}", path);

    if (addr.sun_path[0] == '@')
    {
        addr.sun_path[0] = '\0';
    }

    return addr;
}

std::shared_ptr<io::socket> unix_socket::connect(const std::string_view& path, uint64_t timeout)
{
    auto addr = resolve(path);

    auto c = std::make_shared<unix_socket>(create_socket(), addr);

    c->io::socket::connect(&addr, sizeof(addr), timeout);

    return c;
}

std::shared_ptr<io::socket> unix_socket::listen(const std::string_view& path)
{
    auto addr = resolve(path);

    auto c = std::make_shared<unix_socket>(create_socket(), addr);

    c->io::socket::listen(&addr, sizeof(addr));

    return c;
}

std::shared_ptr<io::socket> unix_socket::accept()
{
    int32_t fd{-1};
    sockaddr_un addr;

    io::socket::accept(&fd, &addr, sizeof(addr));

    return std::make_shared<unix_socket>(fd, addr);
}

unix_socket::unix_socket(int32_t fd, const sockaddr_un& addr)
  : io::socket(fd)
{
    to_uri(addr);
}

void unix_socket::to_uri(const sockaddr_un& addr)
{
    bool abstract = true;

    abstract &= addr.sun_path[0] == '\0';
    abstract &= addr.sun_path[1] != '\0';

    const char* path = addr.sun_path;

    if (abstract == true)
    {
        path++;
    }

    m_uri_view = format_to(m_uri, sizeof(m_uri),
                           "unix://{}{}",
                           (abstract == true) ? "@" : "",
                           path);
}

std::shared_ptr<io::socket> connect(const std::string_view& path, uint64_t timeout)
{
    return unix_socket::connect(path, timeout);
}

std::shared_ptr<io::socket> listen(const std::string_view& path)
{
    return unix_socket::listen(path);
}

}
