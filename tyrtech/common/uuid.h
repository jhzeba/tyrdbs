#pragma once


#include <common/disallow_copy.h>

#include <string>
#include <cstdint>


namespace tyrtech {


using uuid_t =
        uint8_t[16];

class uuid : private disallow_copy
{
public:
    std::string_view str(char* buffer, uint32_t size) const;
    std::string_view bytes() const;

public:
    uuid();

private:
    uuid_t m_uuid;
};

}
