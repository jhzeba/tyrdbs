#pragma once


#include <io/socket.h>


namespace tyrtech::net::uri {


std::shared_ptr<io::socket> connect(const std::string_view& uri, uint64_t timeout);
std::shared_ptr<io::socket> listen(const std::string_view& uri);

}
