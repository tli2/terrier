#include <random>
#include <vector>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <chrono>

#include "../../pybind11/include/pybind11/pybind11.h"
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

namespace terrier {
#define READBUF_SIZE (1024 * 8)
#define SUCCESS 0
#define NEED_MORE 1
#define DONE 2

struct ArrowBufferBuilder {
  std::shared_ptr<arrow::Table> Build() {
    std::shared_ptr<arrow::Array> o_id, o_d_id, o_w_id, ol_number,
        ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info;
    auto status UNUSED_ATTRIBUTE = o_id_builder.Finish(&o_id);
    auto status1 UNUSED_ATTRIBUTE = o_d_id_builder.Finish(&o_d_id);
    auto status2 UNUSED_ATTRIBUTE = o_w_id_builder.Finish(&o_w_id);
    auto status3 UNUSED_ATTRIBUTE = ol_number_builder.Finish(&ol_number);
    auto status4 UNUSED_ATTRIBUTE = ol_i_id_builder.Finish(&ol_i_id);
    auto status5 UNUSED_ATTRIBUTE = ol_supply_w_id_builder.Finish(&ol_supply_w_id);
    auto status6 UNUSED_ATTRIBUTE = ol_delivery_d_builder.Finish(&ol_delivery_d);
    auto status7 UNUSED_ATTRIBUTE = ol_quantity_builder.Finish(&ol_quantity);
    auto status8 UNUSED_ATTRIBUTE = ol_amount_builder.Finish(&ol_amount);
    auto status9 UNUSED_ATTRIBUTE = ol_dist_info_builder.Finish(&ol_dist_info);
    std::vector<std::shared_ptr<arrow::Field>> schema_vector{
        arrow::field("ol_dist_info", arrow::utf8()),
        arrow::field("ol_amount", arrow::float64()),
        arrow::field("ol_delivery_d", arrow::uint64()),
        arrow::field("o_id", arrow::uint32()),
        arrow::field("ol_i_id", arrow::uint32()),
        arrow::field("o_d_id", arrow::uint8()),
        arrow::field("o_w_id", arrow::uint8()),
        arrow::field("ol_number", arrow::uint8()),
        arrow::field("ol_supply_w_id", arrow::uint8()),
        arrow::field("ol_quantity", arrow::uint8())
    };

    std::vector<std::shared_ptr<arrow::Array>> table_vector{ol_dist_info, ol_amount, ol_delivery_d, o_id, ol_i_id,
                                                            o_d_id, o_w_id, ol_number, ol_supply_w_id, ol_quantity};
    return arrow::Table::Make(std::make_shared<arrow::Schema>(schema_vector), table_vector);
  }

  arrow::Int32Builder o_id_builder;
  arrow::Int8Builder o_d_id_builder;
  arrow::Int8Builder o_w_id_builder;
  arrow::Int8Builder ol_number_builder;
  arrow::Int32Builder ol_i_id_builder;
  arrow::Int8Builder ol_supply_w_id_builder;
  arrow::Int64Builder ol_delivery_d_builder;
  arrow::Int8Builder ol_quantity_builder;
  arrow::DoubleBuilder ol_amount_builder;
  arrow::StringBuilder ol_dist_info_builder;
};

struct ReadBuffer {
  template<class T>
  T ReadValue() {
    T val = *reinterpret_cast<T *>(buffer + read_head);
    read_head += sizeof(T);
    return val;
  }

  int ReadRow(ArrowBufferBuilder &builder) {
    if (read_head + 5 > size) return NEED_MORE;
    char packet_type = ReadValue<char>();
    if (packet_type == 'Z') return DONE;
    if (packet_type != 'D') throw std::runtime_error("malformed packet");
    uint32_t packet_size = ReadValue<uint32_t>();
    if (read_head + packet_size > size) {
      read_head -= 5;
      return NEED_MORE;
    }

    uint32_t varlen_size = ReadValue<uint32_t>();
    auto status UNUSED_ATTRIBUTE = builder.ol_dist_info_builder.Append(buffer + read_head, varlen_size);
    read_head += varlen_size;
    read_head += sizeof(uint32_t);
    auto status1 UNUSED_ATTRIBUTE = builder.ol_delivery_d_builder.Append(ReadValue<uint64_t>());
    read_head += sizeof(uint32_t);
    auto status2 UNUSED_ATTRIBUTE = builder.ol_amount_builder.Append(ReadValue<uint64_t>());
    read_head += sizeof(uint32_t);
    auto status3 UNUSED_ATTRIBUTE = builder.o_id_builder.Append(ReadValue<uint32_t>());
    read_head += sizeof(uint32_t);
    auto status4 UNUSED_ATTRIBUTE = builder.ol_i_id_builder.Append(ReadValue<uint32_t>());
    read_head += sizeof(uint32_t);
    auto status5 UNUSED_ATTRIBUTE = builder.o_d_id_builder.Append(ReadValue<uint8_t>());
    read_head += sizeof(uint32_t);
    auto status6 UNUSED_ATTRIBUTE = builder.o_w_id_builder.Append(ReadValue<uint8_t>());
    read_head += sizeof(uint32_t);
    auto status7 UNUSED_ATTRIBUTE = builder.ol_number_builder.Append(ReadValue<uint8_t>());
    read_head += sizeof(uint32_t);
    auto status8 UNUSED_ATTRIBUTE = builder.ol_supply_w_id_builder.Append(ReadValue<uint8_t>());
    read_head += sizeof(uint32_t);
    auto status9 UNUSED_ATTRIBUTE = builder.ol_quantity_builder.Append(ReadValue<uint8_t>());
    return SUCCESS;
  }

  void ShiftToHead() {
    for (uint i = 0; i < size - read_head; i++)
      buffer[i] = buffer[read_head + i];
    size = size - read_head;
    read_head = 0;
  }

  void RefillBuffer(int sock) {
    while (size < READBUF_SIZE) {
      auto read_bytes = read(sock, buffer + size, READBUF_SIZE - size);
      if (read_bytes == 0) return;
      size += read_bytes;
    }
  }

  uint32_t size = 0;
  uint32_t read_head = 0;
  char buffer[READBUF_SIZE]{};
};

int sock_connect(const char *servername, int port) {
  struct addrinfo *resolved_addr = NULL;
  struct addrinfo *iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  int tmp;
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE,
      .ai_family = AF_INET,
      .ai_socktype = SOCK_STREAM
  };
  if (sprintf(service, "%d", port) < 0)
    goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername,
            port);
    goto sock_connect_exit;
  }
  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd =
        socket(iterator->ai_family, iterator->ai_socktype,
               iterator->ai_protocol);
    if (sockfd >= 0) {
      if (servername) {
        /* Client mode. Initiate connection to remote */
        if ((tmp =
                 connect(sockfd, iterator->ai_addr, iterator->ai_addrlen))) {
          fprintf(stdout, "failed connect \n");
          close(sockfd);
          sockfd = -1;
        }
      } else {
        /* Server mode. Set up listening socket an accept a connection */
        listenfd = sockfd;
        sockfd = -1;
        if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
          goto sock_connect_exit;
        listen(listenfd, 1);
        sockfd = accept(listenfd, NULL, 0);
      }
    }
  }
  sock_connect_exit:
  if (listenfd)
    close(listenfd);
  if (resolved_addr)
    freeaddrinfo(resolved_addr);
  if (sockfd < 0) {
    if (servername)
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
    else {
      perror("server accept");
      fprintf(stderr, "accept() failed\n");
    }
  }
  return sockfd;
}

PyObject *read_table(const char *servername, double hot_ratio) {
  auto sock = sock_connect(servername, 15712);
  send(sock, &hot_ratio, sizeof(hot_ratio), 0);
  ReadBuffer reader;
  ArrowBufferBuilder builder;
  uint32_t rows_read = 0;
  while (true) {
    int result = reader.ReadRow(builder);
    if (result == DONE) break;
    if (result == NEED_MORE) {
      reader.ShiftToHead();
      reader.RefillBuffer(sock);
      continue;
    }
    rows_read++;
    if (rows_read % 50000 == 0) printf("Read %u rows \n", rows_read);
  }
  return arrow::py::wrap_table(builder.Build());
}

PYBIND11_MODULE(arrow_reader, m) {
  m.def("read_table", &read_table, "foo");
}
}
