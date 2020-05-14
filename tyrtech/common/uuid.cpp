#include <common/branch_prediction.h>
#include <common/uuid.h>

#include <uuid/uuid.h>
#include <cassert>


namespace tyrtech {


std::string_view uuid::str(char* buffer, uint32_t size)
{
    assert(likely(size >= 37));

    uuid_unparse_lower(m_uuid, buffer);
    buffer[36] = 0;

    return std::string_view(buffer, 36);
}

uuid::uuid()
{
    uuid_generate_random(m_uuid);
}

}
