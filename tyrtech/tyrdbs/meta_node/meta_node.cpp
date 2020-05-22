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
#include <tyrdbs/meta_node/service.json.h>
#include <tyrdbs/meta_node/log_module.h>
#include <crc32c.h>


using namespace tyrtech;


using service_t =
        tyrdbs::meta_node::service<tyrdbs::meta_node::log::impl>;

using server_t =
        net::rpc_server<service_t>;


void service_thread(const std::string_view& uri,
                    uint32_t merge_threads,
                    uint32_t ushards,
                    uint32_t max_slices)
{
    auto ch = net::uri::listen(uri);

    tyrdbs::meta_node::log::impl impl("data",
                                      merge_threads,
                                      ushards,
                                      max_slices);

    service_t service(&impl);

    server_t server(ch, &service);

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
                  "64",
                  {"number of ushards to use (default is 64)"});

    cmd.add_param("max-slices",
                  nullptr,
                  "max-slices",
                  "num",
                  "8192",
                  {"maximum number of slices allowed (default is 8192)"});

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

        set_cpu(cmd.get<uint32_t>("cpu"));

        gt::initialize();
        gt::async::initialize();

        io::initialize(cmd.get<uint32_t>("iouring-queue-depth"));
        io::file::initialize(cmd.get<uint32_t>("storage-queue-depth"));
        io::socket::initialize(cmd.get<uint32_t>("network-queue-depth"));

        gt::create_thread(service_thread,
                          cmd.get<std::string_view>("uri"),
                          cmd.get<uint32_t>("merge-threads"),
                          cmd.get<uint32_t>("ushards"),
                          cmd.get<uint32_t>("max-slices"));

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
