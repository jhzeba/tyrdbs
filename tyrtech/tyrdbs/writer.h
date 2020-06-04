#pragma once


#include <cstdint>


namespace tyrtech::tyrdbs {


class writer
{
public:
    virtual uint32_t write(const char* data, uint32_t size) = 0;
    virtual uint64_t offset() const = 0;

    virtual void flush() = 0;
    virtual void unlink() = 0;

    template<typename T>
    void write(const T& data)
    {
        write(reinterpret_cast<const char*>(&data), sizeof(T));
    }

public:
    virtual ~writer() = default;
};

}
