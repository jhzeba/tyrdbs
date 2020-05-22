#pragma once


#include <tyrdbs/node.h>
#include <tyrdbs/reader.h>


namespace tyrtech::tyrdbs::cache {


using node_ptr =
        std::shared_ptr<node>;


void initialize(uint32_t cache_bits);

node_ptr get(reader* reader, uint64_t chunk_ndx, uint64_t location);
void set(uint64_t chunk_ndx, uint64_t location, node_ptr node);

}
