#include <common/cmd_line.h>
#include <common/cpu_sched.h>
#include <common/logger.h>
#include <gt/engine.h>
#include <gt/async.h>
#include <io/engine.h>
#include <io/file.h>
#include <io/channel.h>
#include <tyrdbs/cache.h>

#include <crc32c.h>


using namespace tyrtech;


int main(int argc, const char* argv[])
{
    cmd_line cmd(argv[0], "tyrdbs meta node.", nullptr);

    cmd.add_param("iouring-queue-depth",
                  nullptr,
                  "iouring-queue-depth",
                  "num",
                  "4096",
                  {"storage queue depth to use (default is 4096)"});

    cmd.add_param("storage-queue-depth",
                  nullptr,
                  "storage-queue-depth",
                  "num",
                  "32",
                  {"storage queue depth to use (default is 32)"});

    cmd.add_param("network-queue-depth",
                  nullptr,
                  "network-queue-depth",
                  "num",
                  "128",
                  {"network queue depth to use (default is 128)"});

    cmd.add_param("cpu",
                  nullptr,
                  "cpu",
                  "index",
                  "0",
                  {"cpu index to run the program on (default is 0)"});

    cmd.add_param("merge-threads",
                  nullptr,
                  "merge-threads",
                  "num",
                  "2",
                  {"number of merge threads to use (default is 2)"});

    cmd.add_param("ushards",
                  nullptr,
                  "ushards",
                  "num",
                  "16",
                  {"number of ushards to use (default is 16)"});

    cmd.add_param("cache-bits",
                  nullptr,
                  "cache-bits",
                  "bits",
                  "12",
                  {"cache size expressed as 2^bits (default is 12)"});

    try
    {
        cmd.parse(argc, argv);

        set_cpu(cmd.get<uint32_t>("cpu"));

        if (crc32c_initialize() == false)
        {
            throw runtime_error_exception("crc32c instruction not supported!");
        }

        gt::initialize();
        gt::async::initialize();
        io::initialize(4096);
        io::file::initialize(cmd.get<uint32_t>("storage-queue-depth"));
        io::channel::initialize(cmd.get<uint32_t>("network-queue-depth"));

        tyrdbs::cache::initialize(cmd.get<uint32_t>("cache-bits"));

        gt::run();
    }
    catch (cmd_line::format_error_exception& e)
    {
        logger::error("{}", e.what());
    }
    catch (std::exception& e)
    {
        logger::critical("{}", e.what());
    }

    return 0;
}
