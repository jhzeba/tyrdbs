#pragma once


#include <cstdint>


namespace tyrtech::tyrdbs::meta_node {


void service_thread(const std::string_view& uri,
                    uint32_t max_slices,
                    uint16_t ushards);

}
