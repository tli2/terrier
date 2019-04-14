#include <string.h>

#define FAKEDB_METADATA_SIZE 128
#define FAKEDB_DATA_SIZE 1048576

char fakedb_md[FAKEDB_METADATA_SIZE];
char fakedb_data[FAKEDB_DATA_SIZE];

// given a table name, provides info of table
int process_query(char *table_name,
  char **metadata, int *md_size, char **data, int *data_size) {
  *metadata = fakedb_md;
  *md_size = sizeof(fakedb_md);
  *data = fakedb_data;
  *data_size = sizeof(fakedb_data);
  memset(fakedb_data, 0xdb, FAKEDB_DATA_SIZE);
  return 0;
}