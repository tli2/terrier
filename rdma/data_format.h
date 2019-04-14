char table_name[16];

struct size_pair {
  int data_size;      // size of final data (table)
  int metadata_size;  // size of metadata
} __attribute__ ((packed));