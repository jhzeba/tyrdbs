#include <common/uuid.h>
#include <gt/engine.h>
#include <io/engine.h>
#include <tyrdbs/slice_writer.h>

#include <crc32c.h>


using namespace tyrtech;


void test()
{
    uuid name;
    uint64_t size{0};

    {
        char buff[37];
        tyrdbs::slice_writer w("{}", name.str(buff, sizeof(buff)));

        w.add("01", "01", true, false, 0);
        w.add("02", "02", true, false, 0);
        w.add("03", "03", true, false, 0);
        w.add("04", "04", true, false, 0);

        w.flush();
        size = w.commit();
    }

    char buff[37];
    fmt::print("{}\n", name.str(buff, sizeof(buff)));

    {
        char buff[37];
        tyrdbs::slice r(size, "{}", name.str(buff, sizeof(buff)));

        auto it = r.begin();

        while (it->next() == true)
        {
            fmt::print("key:{}, value:{}, eor:{}, deleted:{}, idx:{}\n",
                       it->key(), it->value(), it->eor(), it->deleted(), it->idx());
        }
    }
}

int main(int argc, const char* argv[])
{
    assert(crc32c_initialize() == true);

    gt::initialize();
    io::initialize(4096);
    io::file::initialize(128);

    gt::create_thread(test);

    gt::run();

    return 0;
}
