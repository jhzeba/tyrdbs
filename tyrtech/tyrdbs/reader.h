#pragma once


#include <cstdint>


namespace tyrtech::tyrdbs {


class reader
{
public:
    virtual uint32_t pread(uint64_t offset, char* data, uint32_t size) const = 0;

public:
    virtual void unlink() = 0;

public:
    virtual ~reader() = default;
};

}
