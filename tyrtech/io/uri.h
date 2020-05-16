#pragma once


#include <io/socket.h>


namespace tyrtech::io::uri {


std::shared_ptr<socket> connect(const std::string_view& uri, uint64_t timeout);
std::shared_ptr<socket> listen(const std::string_view& uri);

}
