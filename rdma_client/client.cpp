#include <chrono>
#include <iostream>

#include "data_format.h"
#include "rdma.h"

#define METADATA_SIZE 128

struct config_t config = {
    NULL,                         /* device_name */
    NULL,                         /* server_name */
    15712,                        /* tcp_port */
    1,                            /* ib_port */
    1                             /* gid_idx */
};

struct size_pair sizes = {0, 0};

int main(int argc, char *argv[]) {
  if (argc != 3) {
    fprintf(stderr,
            "Usage (client): %s <hostname> <hot_ratio>\n",
            argv[0]);
    return 1;
  }

  // setup local config
  config.server_name = argv[1];

  // connect to server
  struct resources res;
  resources_init(&res);

  res.sock = sock_connect (config.server_name, config.tcp_port);
  if (res.sock < 0) {
    fprintf(stderr,
            "failed to establish TCP connection to server %s, port %d\n",
            config.server_name, config.tcp_port);
    return 1;
  }
  fprintf (stdout, "TCP connection was established\n");

  // send table data from client to server
  memset(table_name, 0, sizeof(table_name));
  strncpy(table_name, argv[2], sizeof(table_name));
  int wc = sock_write_data(res.sock, sizeof(table_name), table_name);
  if (wc < 0) {
    fprintf(stderr, "failed to send data to server\n");
    return 1;
  }
  fprintf(stderr, "table name sent to server\n");

  // get metadata and data size from server to client
  sock_read_data(res.sock, sizeof(sizes), (char *)&sizes);
  // ***** force overwrite data_size *****
  sizes.data_size = 10737418240;
  fprintf(stdout, "Metadata size: %zd, data size: %zd\n", sizes.metadata_size, sizes.data_size);
  char *data = (char*)malloc(sizes.data_size);
  char *metadata = (char *)malloc(sizes.metadata_size);
  memset(data, 0, sizes.data_size);
  memset(metadata, 0, sizes.metadata_size);

  int total_read = sock_read_data(res.sock, sizes.metadata_size, metadata);
  fprintf(stderr, "got metadata, total_read: %d\n", total_read);

  // sync resources between client and server
  res.buf = data;
  res.size = sizes.data_size;
  // std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  /* create resources before using them */
  if (resources_create (&res, config))
  {
    fprintf (stderr, "failed to create resources\n");
    return 1;
  }
  /* connect the QPs */
  std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  if (connect_qp (&res, config)) {
    fprintf (stderr, "failed to connect QPs\n");
    return 1;
  }

  size_t num_polls = 0;
  while (true) {
    if (poll_completion (&res))
    {
        fprintf (stderr, "poll completion failed\n");
        break;
    }
    num_polls++;
  }
  fprintf (stdout, "num_polls: %zu\n", num_polls);

  // wait for server to tell us it's done
  /* Sync so server will know that client is done mucking with its memory */
  char temp_char = 'W';
  if (sock_sync_data (res.sock, 1, &temp_char, &temp_char))    /* just send a dummy char back and forth */
  {
    fprintf (stderr, "sync error after RDMA ops\n");
    return 1;
  }
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  fprintf(stdout, "Client side TOTAL duration: %ld\n", std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  fprintf (stderr, "final sync done\n");

  // report what we got

  // cleanup
  resources_destroy (&res);

  return 0;
}