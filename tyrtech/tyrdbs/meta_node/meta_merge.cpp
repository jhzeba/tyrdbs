#include <common/logger.h>
#include <common/cmd_line.h>
#include <common/cpu_sched.h>
#include <gt/engine.h>
#include <io/engine.h>
#include <io/file.h>
#include <io/socket.h>
#include <net/rpc_request.h>
#include <net/uri.h>
#include <tyrdbs/meta_node/service.json.h>
#include <crc32c.h>


using namespace tyrtech;


void merge_thread(const std::string_view& uri)
{
    net::socket_channel channel(net::uri::connect(uri, 0), 0);

    net::rpc_request<tyrdbs::meta_node::log::merge> request(&channel);
    auto message = request.add_message();

    request.execute();

    while (true)
    {
        auto response = request.wait();

        if (response.has_terminate() == true)
        {
            break;
        }

        char c;
        channel.write(&c, 1);
    }
}


int main(int argc, const char* argv[])
{
    cmd_line cmd(argv[0], "tyrdbs meta merge helper.", nullptr);

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
                  {"merge threads to run (default is 2)"});

    cmd.add_param("uri",
                  "<uri>",
                  {"uri to connect to"});

    try
    {
        cmd.parse(argc, argv);

        set_cpu(cmd.get<uint32_t>("cpu"));

        if (crc32c_initialize() == false)
        {
            throw runtime_error_exception("crc32c instruction not supported!");
        }

        gt::initialize();

        io::initialize(cmd.get<uint32_t>("iouring-queue-depth"));
        io::file::initialize(cmd.get<uint32_t>("storage-queue-depth"));
        io::socket::initialize(cmd.get<uint32_t>("network-queue-depth"));

        gt::create_thread(merge_thread, cmd.get<std::string_view>("uri"));

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
