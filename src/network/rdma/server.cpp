#include <chrono>
#include <iostream>

#include "data_format.h"
#include "fake_db.h"
#include "rdma.h"

struct config_t config = {
    NULL,                         /* device_name */
    NULL,                         /* server_name */
    19875,                        /* tcp_port */
    1,                            /* ib_port */
    1                             /* gid_idx */
};

struct size_pair sizes = {-1, -1};

int do_rdma(int sockfd) {
  struct resources res;
  resources_init(&res);
  res.sock = sockfd;

  // get table data from client to server
  int rc = sock_read_data(res.sock, sizeof(table_name), table_name);
  if (rc < 0) {
    fprintf(stderr, "failed to receive data from client\n");
    return 1;
  }
  fprintf(stdout, "received table name: %s\n", table_name);

  // send metadata and data size from server to client
  char *metadata;
  blocklist data;
  int metadata_size;
  int num_blocks;
  process_query(table_name, &metadata, &metadata_size, &data, &num_blocks);
  sizes.metadata_size = metadata_size;
  sizes.data_size = FAKEDB_BLOCK_SIZE * num_blocks;
  sock_write_data(res.sock, sizeof(sizes), (char *)&sizes);
  fprintf(stderr, "sizes sent to client\n");

  sock_write_data(res.sock, metadata_size, metadata);
  fprintf(stderr, "metadata sent to client\n");

  // sync resources between client and server
  res.buf = data[0];
  res.size = FAKEDB_BLOCK_SIZE;
  /* create resources before using them */
  if (resources_create (&res, config)) {
    fprintf (stderr, "failed to create resources\n");
    return 1;
  }
  /* connect the QPs */
  if (connect_qp (&res, config)) {
    fprintf (stderr, "failed to connect QPs\n");
    return 1;
  }

  // initiate rdma write
  fprintf (stdout, "Now initiating RDMA write\n");
  std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  for (int i = 0; i < num_blocks; i++) {
    // fprintf (stdout, "Sending address 0x%x to 0x%x\n", res.buf, res.remote_props.addr);
    if (post_send (&res, IBV_WR_RDMA_WRITE))
    {
        fprintf (stderr, "failed to post SR\n");
        return 1;
    }
    if (poll_completion (&res))
    {
        fprintf (stderr, "poll completion failed\n");
        return 1;
    }

    // clone resource for next iteration
    if (i + 1 < num_blocks) {
      int mr_flags = IBV_ACCESS_LOCAL_WRITE;
      res.buf = data[i];
      res.mr = ibv_reg_mr (res.pd, res.buf, FAKEDB_BLOCK_SIZE, mr_flags);
      res.remote_props.addr += FAKEDB_BLOCK_SIZE;
    }
  }
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  fprintf(stdout, "Server side RDMA duration: %ld\n", std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  fprintf (stderr, "RDMA Write completed\n");

  // tell client we're done
  /* Sync so server will know that client is done mucking with its memory */
  char temp_char = 'W';
  if (sock_sync_data (res.sock, 1, &temp_char, &temp_char))    /* just send a dummy char back and forth */
  {
    fprintf (stderr, "sync error after RDMA ops\n");
    return 1;
  }
  fprintf (stderr, "final sync done\n");

  // cleanup
  resources_destroy (&res);

  return 0;
}


// int main(int argc, char *argv[]) {
//   if (argc != 1) {
//     fprintf(stderr,
//             "Usage (server): run with no arguments\n");
//     return 1;
//   }

//   // wait for client connection
//   fprintf(stdout, "waiting on port %d for TCP connection\n",
//           config.tcp_port);
//   int sockfd = sock_connect (NULL, config.tcp_port);
//   if (sockfd < 0) {
//     fprintf(stderr,
//             "failed to establish TCP connection to server %s, port %d\n",
//             config.server_name, config.tcp_port);
//     return 1;
//   }
//   fprintf (stdout, "TCP connection was established\n");

//   return do_rdma(sockfd)
// }