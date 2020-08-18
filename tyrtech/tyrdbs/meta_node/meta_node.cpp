#include <common/cmd_line.h>
#include <common/cpu_sched.h>
#include <common/logger.h>
#include <gt/engine.h>
#include <gt/async.h>
#include <io/engine.h>
#include <io/file.h>
#include <io/socket.h>
#include <net/rpc_server.h>
#include <net/uri.h>
#include <tyrdbs/cache.h>
#include <tyrdbs/meta_node/service.json.h>
#include <tyrdbs/meta_node/log.h>
#include <crc32c.h>


using namespace tyrtech;


using service_t =
        tyrdbs::meta_node::service<tyrdbs::meta_node::log::impl>;

using server_t =
        net::rpc_server<service_t>;


void service_thread(const cmd_line& cmd)
{
    auto ch = net::uri::listen(cmd.get<std::string_view>("uri"));

    tyrdbs::meta_node::log::impl log_impl(cmd.get<std::string_view>("data"),
                                          cmd.get<uint32_t>("merge-threads"),
                                          cmd.get<uint32_t>("ushards"),
                                          cmd.get<uint32_t>("max-slices"));

    service_t svc(&log_impl);
    server_t srv(ch, &svc);

    while (gt::terminated() == false)
    {
        gt::sleep(100);
    }
}


int main(int argc, const char* argv[])
{
    cmd_line cmd(argv[0], "tyrdbs meta node.", nullptr);

    cmd.add_param("iouring-queue-depth",
                  nullptr,
                  "iouring-queue-depth",
                  "num",
                  "1024",
                  {"storage queue depth to use (default is 1024)"});

    cmd.add_param("storage-queue-depth",
                  nullptr,
                  "storage-queue-depth",
                  "num",
                  "128",
                  {"storage queue depth to use (default is 128)"});

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
                  "-1",
                  {"cpu index to run the program on (default is -1)"});

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
                  "64",
                  {"number of ushards to use (default is 64)"});

    cmd.add_param("max-slices",
                  nullptr,
                  "max-slices",
                  "num",
                  "3072",
                  {"maximum number of slices allowed (default is 3072)"});

    cmd.add_param("cache-bits",
                  nullptr,
                  "cache-bits",
                  "bits",
                  "12",
                  {"cache size expressed as 2^bits (default is 12)"});

    cmd.add_param("data",
                  nullptr,
                  "data",
                  "path",
                  "data",
                  {"data path to use (default is data)"});

    cmd.add_param("uri",
                  "<uri>",
                  {"uri to listen on"});

    try
    {
        cmd.parse(argc, argv);

        if (crc32c_initialize() == false)
        {
            throw runtime_error_exception("cpu doesn't support crc32c instruction");
        }

        set_cpu(cmd.get<int32_t>("cpu"));

        gt::initialize();
        gt::async::initialize();

        io::initialize(cmd.get<uint32_t>("iouring-queue-depth"));
        io::file::initialize(cmd.get<uint32_t>("storage-queue-depth"));
        io::socket::initialize(cmd.get<uint32_t>("network-queue-depth"));

        tyrdbs::cache::initialize(cmd.get<uint32_t>("cache-bits"));

        gt::create_thread(service_thread, std::cref(cmd));

        gt::run();
    }
    catch (std::exception& e)
    {
        fmt::print(stderr, "{}\n", e.what());

        return 1;
    }

    return 0;
}
