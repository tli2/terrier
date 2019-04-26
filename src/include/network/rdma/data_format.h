#pragma once
#include <cstdint>
#include <cstdlib>

struct size_pair {
  size_t data_size;      // size of final data (table)
  size_t metadata_size;  // size of metadata
} __attribute__ ((packed));