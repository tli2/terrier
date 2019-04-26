#include <string.h>

#define FAKEDB_METADATA_SIZE 128
#define FAKEDB_BLOCK_SIZE 10485760
#define NUM_BLOCKS 100

char fakedb_md[FAKEDB_METADATA_SIZE];

typedef char** blocklist;

// given a table name, provides info of table
int process_query(char *table_name,
  char **metadata, int *md_size, blocklist *data, int *num_blocks) {
  *metadata = fakedb_md;
  *md_size = sizeof(fakedb_md);
  *data = (blocklist)calloc(NUM_BLOCKS, sizeof(char *));
  for (int i = 0; i < NUM_BLOCKS; i++) {
    char *block_addr = (char*)malloc(FAKEDB_BLOCK_SIZE);
    (*data)[i] = block_addr;
    memset(block_addr, 0xdb, FAKEDB_BLOCK_SIZE);
  }
  *num_blocks = NUM_BLOCKS;
  return 0;
}