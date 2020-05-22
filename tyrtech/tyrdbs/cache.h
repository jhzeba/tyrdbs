#pragma once


#include <tyrdbs/node.h>
#include <io/file_channel.h>


namespace tyrtech::tyrdbs::cache {


using node_ptr =
        std::shared_ptr<node>;


void initialize(uint32_t cache_bits);

node_ptr get(io::file_channel* channel, uint64_t chunk_ndx, uint64_t location);
void set(uint64_t chunk_ndx, uint64_t location, node_ptr node);

}
