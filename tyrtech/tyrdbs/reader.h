#pragma once


#include <string>
#include <cstdint>


namespace tyrtech::tyrdbs {


class reader
{
public:
    virtual uint32_t pread(uint64_t offset, char* data, uint32_t size) const = 0;

public:
    virtual void unlink() = 0;
    virtual const std::string_view& path() const = 0;

public:
    virtual ~reader() = default;
};

}
