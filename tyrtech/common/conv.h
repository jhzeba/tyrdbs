#pragma once


#include <common/exception.h>


namespace tyrtech::conv {


DEFINE_EXCEPTION(runtime_error_exception, format_error_exception);


template<typename T>
T parse(const std::string_view& value);

}
