#include <cstdint>

char table_name[16];

struct size_pair {
  size_t data_size;      // size of final data (table)
  size_t metadata_size;  // size of metadata
} __attribute__ ((packed));