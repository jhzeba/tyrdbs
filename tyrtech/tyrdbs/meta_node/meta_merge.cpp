#include <common/logger.h>
#include <common/cmd_line.h>
#include <common/cpu_sched.h>
#include <common/clock.h>
#include <gt/engine.h>
#include <io/engine.h>
#include <io/file_channel.h>
#include <io/socket_channel.h>
#include <net/rpc_request.h>
#include <net/uri.h>
#include <tyrdbs/slice_writer.h>
#include <tyrdbs/overwrite_iterator.h>
#include <tyrdbs/meta_node/log_module.h>
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

        auto tier = tyrdbs::meta_node::log::impl::recv_tier(&channel);

        tyrdbs::slices_t slices;

        for (auto& s : tier)
        {
            auto slice = std::make_shared<tyrdbs::slice>(s.size, "data/{:016x}.dat", s.id);
            slice->set_tid(s.tid);

            slices.emplace_back(std::move(slice));
        }

        auto t1 = clock::now();

        auto id = tyrdbs::slice::new_id();

        auto file = io::file::create("data/{:016x}.dat", id);
        auto file_channel = io::file_channel(&file);
        auto slice_writer = tyrdbs::slice_writer(&file_channel);

        tyrdbs::overwrite_iterator it(slices);

        slice_writer.add(&it, false);
        slice_writer.flush();

        auto t2 = clock::now();

        decltype(tier)::value_type merged_slice;

        merged_slice.id = id;
        merged_slice.tid = 0;
        merged_slice.size = slice_writer.commit();
        merged_slice.key_count = slice_writer.key_count();

        float total_t = (t2 - t1) / 1000000.;

        fmt::print("merge done in {:.2f} ms, size: {} ({:.2f} MB/s), key_count: {} ({:.2f} keys/s)\n",
                   total_t,
                   merged_slice.size,
                   merged_slice.size / (total_t * 1024 * 1024 * 1000),
                   merged_slice.key_count,
                   merged_slice.key_count / (total_t * 1000));

        decltype(tier) merged_tier;
        merged_tier.emplace_back(merged_slice);

        tyrdbs::meta_node::log::impl::send_tier(merged_tier, true, &channel);

        for (auto& s : slices)
        {
            s->unlink();
        }
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
