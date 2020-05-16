#pragma once


#include <io/socket.h>


namespace tyrtech::io::tcp {


std::shared_ptr<socket> connect(const std::string_view& host, const std::string_view& service, uint64_t timeout);
std::shared_ptr<socket> listen(const std::string_view& host, const std::string_view& service);

}
