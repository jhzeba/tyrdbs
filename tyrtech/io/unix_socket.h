#pragma once


#include <io/socket.h>


namespace tyrtech::io::unix {


std::shared_ptr<socket> connect(const std::string_view& path, uint64_t timeout);
std::shared_ptr<socket> listen(const std::string_view& path);

}
