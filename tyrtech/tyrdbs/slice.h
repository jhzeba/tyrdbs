#pragma once


#include <io/file.h>
#include <tyrdbs/node.h>
#include <tyrdbs/attributes.h>
#include <tyrdbs/iterator.h>

#include <memory>
#include <vector>


namespace tyrtech::tyrdbs {


class slice : private disallow_copy, disallow_move
{
public:
    struct stats
    {
        uint64_t key_count{0};
        uint64_t compressed_size{0};
        uint64_t uncompressed_size{0};
        uint64_t total_nodes{0};
        uint64_t leaf_nodes{0};
    } __attribute__ ((packed));

public:
    std::unique_ptr<iterator> range(const std::string_view& min_key, const std::string_view& max_key);
    std::unique_ptr<iterator> begin();

    void unlink();

    uint64_t key_count() const;

    void set_tid(uint64_t tid);

public:
    static uint64_t new_id();

public:
    template<typename... Arguments>
    slice(uint64_t size, Arguments&&... arguments)
      : slice(size, io::file::open(io::file::access::read_only,
                                   std::forward<Arguments>(arguments)...))
    {
    }

    slice() = default;
    ~slice();

private:
    static constexpr uint64_t signature{0x3130306264727974UL};

public:
    struct header
    {
        uint64_t signature{slice::signature};
        uint64_t root{static_cast<uint64_t>(-1)};
        uint16_t first_node_size{static_cast<uint16_t>(-1)};
        stats stats;
    } __attribute__ ((packed));

private:
    uint64_t m_cache_id{static_cast<uint64_t>(-1)};

    io::file m_file;

    uint64_t m_key_count{0};

    uint64_t m_root{static_cast<uint64_t>(-1)};
    uint64_t m_first_node_size{0};

    uint64_t m_tid{0};

    bool m_unlink{false};

private:
    slice(uint64_t size, io::file&& file);

private:
    uint64_t find_node_for(uint64_t location,
                           const std::string_view& min_key,
                           const std::string_view& max_key) const;

    std::shared_ptr<node> load(uint64_t location) const;

private:
    friend class slice_iterator;
    friend class slice_writer;
};


using slice_ptr =
        std::shared_ptr<slice>;

using slices_t =
        std::vector<slice_ptr>;

}
