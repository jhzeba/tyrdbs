#include <common/cmd_line.h>
#include <common/cpu_sched.h>
#include <common/logger.h>
#include <gt/engine.h>
#include <io/engine.h>
#include <io/socket.h>
#include <tyrdbs/meta_node/service.h>


using namespace tyrtech;


int main(int argc, const char* argv[])
{
    cmd_line cmd(argv[0], "tyrdbs meta node.", nullptr);

    cmd.add_param("iouring-queue-depth",
                  nullptr,
                  "iouring-queue-depth",
                  "num",
                  "1024",
                  {"storage queue depth to use (default is 1024)"});

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

    cmd.add_param("max-slices",
                  nullptr,
                  "max-slices",
                  "num",
                  "512",
                  {"maximum number of slices allowed (default is 512)"});

    cmd.add_param("uri",
                  "<uri>",
                  {"uri to listen on"});

    try
    {
        cmd.parse(argc, argv);

        set_cpu(cmd.get<uint32_t>("cpu"));

        gt::initialize();

        io::initialize(cmd.get<uint32_t>("iouring-queue-depth"));
        io::socket::initialize(cmd.get<uint32_t>("network-queue-depth"));

        gt::create_thread(tyrdbs::meta_node::service_thread,
                          cmd.get<std::string_view>("uri"),
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
