#pragma once


#include <common/disallow_copy.h>

#include <string>
#include <cstdint>


namespace tyrtech {


class uuid : private disallow_copy
{
public:
    std::string_view str(char* buffer, uint32_t size);

public:
    uuid();

private:
    using uuid_t =
            uint8_t[16];

    uuid_t m_uuid;
};

}
