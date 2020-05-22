#include <common/exception.h>
#include <common/rdrnd.h>


namespace tyrtech {


uint64_t rdrnd()
{
    unsigned long long rnd;

    if (__builtin_ia32_rdrand64_step(&rnd) != 1)
    {
        throw runtime_error_exception("unable to generate rnd");
    }

    return rnd;
}

}
