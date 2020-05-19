#include <common/cmd_line.h>
#include <common/cpu_sched.h>
#include <common/clock.h>
#include <common/logger.h>
#include <gt/engine.h>
#include <io/engine.h>
#include <net/rpc_request.h>
#include <net/uri.h>
#include <tyrdbs/meta_node/service.json.h>

#include <random>
#include <set>


using namespace tyrtech;


void update_iteration(const std::set<uint64_t>& keys, uint32_t iteration, net::socket_channel* channel)
{
    net::rpc_request<tyrdbs::meta_node::log::update> request(channel);
    auto message = request.add_message();

    message.set_id(iteration);
    message.set_size(100);
    message.set_key_count(1000);

    request.execute();

    /*
    std::array<char, 8192> buffer;
    message::builder builder(buffer.data(), buffer.size());
    tyrdbs::meta_node::block_builder block(&builder);

    auto entries = block.add_entries();

    uint32_t ushard_id = 0;

    for (auto& key : keys)
    {
        uint64_t _key = __builtin_bswap64(key + iteration);
        std::string_view key_str(reinterpret_cast<const char*>(&_key), sizeof(_key));

        uint16_t bytes_required = 0;

        bytes_required += tyrdbs::meta_node::block_builder::entries_bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::key_bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::key_bytes_required();
        bytes_required += key_str.size();
        bytes_required += key_str.size();

        if (builder.available_space() <= bytes_required)
        {
            entries.finalize();
            block.finalize();

            channel->write(buffer.data(), builder.size());

            builder = message::builder(buffer.data(), buffer.size());
            block = tyrdbs::meta_node::block_builder(&builder);

            entries = block.add_entries();
        }

        auto entry = entries.add_value();

        entry.set_flags(0x01);
        entry.set_ushard_id(ushard_id++);
        entry.add_key(key_str);
        entry.add_value(key_str);
    }

    block.set_flags(1);

    entries.finalize();
    block.finalize();

    channel->write(buffer.data(), builder.size());
    */

    request.wait();
}

void update_thread(const std::string_view& uri,
                   uint32_t seed,
                   uint32_t iterations,
                   uint32_t slices,
                   uint32_t target_rate,
                   FILE* stats_fd)
{
    net::socket_channel channel(net::uri::connect(uri, 0), 0);

    std::mt19937 generator(seed);
    std::uniform_int_distribution<uint32_t> distribution(0, static_cast<uint32_t>(-1));

    uint64_t last_print = 0;
    auto t0 = clock::now();

    std::set<uint64_t> keys;

    while (keys.size() != slices)
    {
        keys.insert(distribution(generator));
    }

    for (uint32_t i = 0; i < iterations; i++)
    {
        auto t1 = clock::now();

        update_iteration(keys, i, &channel);

        auto t2 = clock::now();

        uint64_t iter_t = t2 - t1;
        uint64_t total_t = t2 - t0;

        if ((i % target_rate) == 0)
        {
            last_print = total_t;

            logger::notice("#{}: transaction commited in {:.2f} ms, {:.2f} transactions/s, {:.2f} keys/s",
                           i,
                           iter_t / 1000000.,
                           (i + 1) * 1000000000. / total_t,
                           (i + 1) * slices * 1000000000. / total_t);
        }

        if (stats_fd != nullptr)
        {
            fmt::print(stats_fd,
                       "{},{},{}\n",
                       t2 / 1000,
                       i,
                       iter_t);

            fflush(stats_fd);
        }

        if (target_rate != 0)
        {
            uint64_t target_t = (i + 1) * 1000000000UL / target_rate;

            if (target_t > total_t)
            {
                uint64_t delay = target_t - total_t;
                gt::sleep(delay / 1000000);
            }
        }
    }
}


int main(int argc, const char* argv[])
{
    cmd_line cmd(argv[0], "Network update client demo.", nullptr);

    cmd.add_param("seed",
                  nullptr,
                  "seed",
                  "num",
                  "0",
                  {"pseudo random seed to use (default is 0)"});

    cmd.add_param("network-queue-depth",
                  nullptr,
                  "network-queue-depth",
                  "num",
                  "512",
                  {"network queue depth to use (default is 512)"});

    cmd.add_param("cpu",
                  nullptr,
                  "cpu",
                  "index",
                  "0",
                  {"cpu index to run the program on (default is 0)"});

    cmd.add_param("iterations",
                  nullptr,
                  "iterations",
                  "num",
                  "10000",
                  {"iterations to run (default is 10000)"});

    cmd.add_param("slices",
                  nullptr,
                  "slices",
                  "num",
                  "1000",
                  {"slices to update in one iteration (default: 1000)"});

    cmd.add_param("update-rate",
                  nullptr,
                  "update-rate",
                  "num",
                  "0",
                  {"update rate limit in iterations per second (default: unlimited)"});

    cmd.add_param("stats-output",
                  nullptr,
                  "stats-output",
                  "file",
                  nullptr,
                  {"output csv formatted stats to file"});

    cmd.add_param("uri",
                  "<uri>",
                  {"uri to connect to"});

    cmd.parse(argc, argv);

    set_cpu(cmd.get<uint32_t>("cpu"));

    gt::initialize();
    io::initialize(4096);
    io::socket::initialize(cmd.get<uint32_t>("network-queue-depth"));

    FILE* stats_fd = nullptr;

    if (cmd.has("stats-output") == true)
    {
        stats_fd = fopen(cmd.get<std::string_view>("stats-output").data(), "w");
        assert(stats_fd != nullptr);

        fmt::print(stats_fd, "#ts[us],keys,duration[ns]\n");
    }

    gt::create_thread(update_thread,
                      cmd.get<std::string_view>("uri"),
                      cmd.get<uint32_t>("seed"),
                      cmd.get<uint32_t>("iterations"),
                      cmd.get<uint32_t>("slices"),
                      cmd.get<uint32_t>("update-rate"),
                      stats_fd);

    gt::run();

    if (stats_fd != nullptr)
    {
        fclose(stats_fd);
    }

    return 0;
}
