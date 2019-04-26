#include <random>
#include <vector>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "storage/garbage_collector.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "tpcc/builder.h"
#include "tpcc/database.h"
#include "tpcc/delivery.h"
#include "tpcc/loader.h"
#include "tpcc/new_order.h"
#include "tpcc/order_status.h"
#include "tpcc/payment.h"
#include "tpcc/stock_level.h"
#include "tpcc/worker.h"
#include "tpcc/workload.h"
#include "storage/block_compactor.h"
#include "transaction/transaction_manager.h"
#include "storage/dirty_globals.h"
#include "storage/arrow_util.h"
#include "storage/tuple_access_strategy.h"
#include "storage/data_table.h"
#include "arrow/table.h"
#include "arrow/type.h"

#include "data_format.h"
#include "fake_db.h"
#include "rdma.h"

#define ONE_MEGABYTE 1048576
namespace terrier {

struct config_t config = {
    NULL,                         /* device_name */
    NULL,                         /* server_name */
    19875,                        /* tcp_port */
    1,                            /* ib_port */
    1                             /* gid_idx */
};

struct size_pair sizes = {0, 0};

std::bernoulli_distribution treat_as_hot{0.1};

int do_send(struct resources *res, char *buf, size_t buf_size, uint64_t remote_addr) {
  int mr_flags = IBV_ACCESS_LOCAL_WRITE;
  res->buf = buf;
  res->mr = ibv_reg_mr (res->pd, res->buf, buf_size, mr_flags);
  res->remote_props.addr = remote_addr;

  // fprintf (stdout, "Sending address 0x%x to 0x%x\n", block, res.remote_props.addr);
  if (post_send (res, IBV_WR_RDMA_WRITE))
  {
      fprintf (stderr, "failed to post SR\n");
      return 1;
  }
  if (poll_completion (res))
  {
      fprintf (stderr, "poll completion failed\n");
      return 1;
  }
  return 0;
}

int do_rdma(int sockfd, storage::DataTable *datatable) {
  std::list<storage::RawBlock *> blocks = datatable->blocks_;
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
  char metadata[] = "FAKE METADATA";
  size_t metadata_size = 8;
  size_t num_blocks = blocks.size();
  sizes.metadata_size = metadata_size;
  sizes.data_size = ONE_MEGABYTE * num_blocks;
  sock_write_data(res.sock, sizeof(sizes), (char *)&sizes);
  fprintf(stderr, "sizes sent to client\n");

  sock_write_data(res.sock, metadata_size, metadata);
  fprintf(stderr, "metadata sent to client\n");

  // sync resources between client and server
  /* create resources before using them */
  res.buf = metadata; // use metadata for now
  res.size = metadata_size;
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
  uint64_t remote_addr_start = res.remote_props.addr;
  uint64_t remote_curr_addr = remote_addr_start;
  const storage::TupleAccessStrategy &accessor = datatable->accessor_;
  fprintf (stdout, "Now initiating RDMA write\n");
  std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  for (storage::RawBlock *block : blocks) {
    std::shared_ptr<arrow::Table> table UNUSED_ATTRIBUTE;
    if (block->controller_.CurrentBlockState() != storage::BlockState::FROZEN || treat_as_hot(generator_)) {
      // table = MaterializeHotBlock(tpcc_db, block);
      continue;
    } else {
      table = storage::ArrowUtil::AssembleToArrowTable(accessor, block);
    }

    int num_cols = table->num_columns();
    fprintf (stdout, "num columns: %d\n", num_cols);
    for (int ci = 0; ci < num_cols; ci++) {
      printf ("index: %d\n", ci);
      auto col = table->column(ci);
      std::cout << "---- column name: " << col->field()->type()->id() << ", should not be " << arrow::Type::type::STRING << std::endl;
      // if (col->field()->type()->id() == arrow::Type::type::STRING) continue;
      // fprintf (stdout, "--- column name: %s\n", col->field()->name());
      auto array_data = col->data()->chunk(0)->data();
      int64_t length = array_data->buffers.size();
      std::cout << "  array_data length: " << length << std::endl;
      for (int64_t bi = 0; bi < length; bi++) {
        auto buffer = array_data->buffers[bi];
        int64_t buf_size = buffer->size();
        std::cout << "  size: " << buf_size << std::endl;
        if (buf_size == 0) break;
        uint8_t *data = (uint8_t *)buffer->data();
        

        if (0 != do_send(&res, reinterpret_cast<char *>(data), buf_size, remote_curr_addr)) return 1;
        remote_curr_addr += buf_size;
      }
    }
  }
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  fprintf (stdout, "Server side RDMA duration: %ld\n", std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  fprintf (stdout, "RDMA Write completed\n");
  fprintf (stdout, "Num blocks written: %zd\n", remote_curr_addr - remote_addr_start);

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