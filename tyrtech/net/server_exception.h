#pragma once


#include <common/exception.h>


namespace tyrtech::net {


class server_error_exception : public runtime_error_exception
{
public:
    int16_t code() const noexcept
    {
        return m_code;
    }

public:
    template<typename... Arguments>
    server_error_exception(int16_t code, Arguments&&... arguments)
      : runtime_error_exception(std::forward<Arguments>(arguments)...)
      , m_code(code)
    {
    }

private:
    int16_t m_code;
};


#define DEFINE_SERVER_EXCEPTION(code, base_name, name)                \
    class name : public base_name                                     \
    {                                                                 \
    public:                                                           \
        template<typename... Arguments>                               \
        name(Arguments&&... arguments)                                \
          : base_name(code, std::forward<Arguments>(arguments)...)    \
        {                                                             \
        }                                                             \
    }


DEFINE_SERVER_EXCEPTION(-1, server_error_exception, unknown_module_exception);
DEFINE_SERVER_EXCEPTION(-2, server_error_exception, unknown_function_exception);
DEFINE_SERVER_EXCEPTION(-3, server_error_exception, unknown_exception);

}
