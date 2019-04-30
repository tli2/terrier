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
#include "storage/write_ahead_log/log_io.h"

//#include "network/rdma/server.h"
//#include "network/rdma/data_format.h"
//#include "network/rdma/rdma.h"

#define ONE_MEGABYTE 1048576

namespace terrier {
char write_buf[common::Constants::BLOCK_SIZE * 50];

uint32_t sizes[10] = {8u, 8u, 4u, 4u, 1u, 1u, 1u, 1u, 1u, -1u};
struct BlockWriter {
  void ResetWrite(uint32_t batch_size) {
    num_tuples_in_batch = batch_size;
    col_starts[0] = col_write_heads[0] = 2 * sizeof(uint32_t);
    for (uint32_t i = 1; i < 10; i++)
      col_starts[i] = col_write_heads[i] = col_write_heads[i - 1] + sizes[i - 1] * batch_size;
  }

  void WriteAttribute(byte *entry, uint32_t size, uint32_t col_num) {
    if (col_num == 9) throw std::runtime_error("Varlen written as fixed length");
    if (entry != nullptr)
      std::memcpy(write_buf + (col_write_heads[col_num]), entry, size);
    col_write_heads[col_num] += size;
  }

  void WriteVarlen(storage::VarlenEntry *varlen) {
    if (varlen == nullptr) {
      uint32_t size = 0;
      std::memcpy(write_buf + (col_write_heads[9]), &size, sizeof(uint32_t));
      col_write_heads[9] += sizeof(uint32_t);
      return;
    }
    uint32_t size = varlen->Size();
    std::memcpy(write_buf + (col_write_heads[9]), &size, sizeof(uint32_t));
    col_write_heads[9] += sizeof(uint32_t);
    std::memcpy(write_buf + (col_write_heads[9]), varlen->Content(), size);
    col_write_heads[9] += size;
  }

  void FlushBatch(int fd) {
    uint32_t num_tuples = (col_write_heads[0] - col_starts[0]) / 8;
    if (num_tuples != num_tuples_in_batch) {
      // underfilled. slow path
      uint32_t total_size = 0;
      storage::PosixIoWrappers::WriteFully(fd, &num_tuples, sizeof(uint32_t));
      for (uint32_t i = 0; i < 10; i++)
        total_size += col_write_heads[i] - col_starts[i];
      storage::PosixIoWrappers::WriteFully(fd, &total_size, sizeof(uint32_t));
      for (uint32_t i = 0; i < 10; i++)
        storage::PosixIoWrappers::WriteFully(fd, write_buf + (col_starts[i]), col_write_heads[i] - col_starts[i]);
    } else {
      uint32_t total_size = col_write_heads[9] - 2 * sizeof(uint32_t);
      std::memcpy(write_buf, &num_tuples_in_batch, sizeof(uint32_t));
      std::memcpy(write_buf + sizeof(uint32_t), &total_size, sizeof(uint32_t));
      storage::PosixIoWrappers::WriteFully(fd, write_buf, col_write_heads[9]);
    }
  }

  uint32_t num_tuples_in_batch;
  uint32_t col_starts[10];
  uint32_t col_write_heads[10];
};

class TpccLoader {
 public:
  void StartGC(transaction::TransactionManager *const) {

    gc_ = new storage::GarbageCollector(&txn_manager, &access_observer_);
//    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([this] { GCThreadLoop(); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  void StartCompactor(transaction::TransactionManager *const) {
    run_compactor_ = true;
    compactor_thread_ = std::thread([this] { CompactorThreadLoop(&txn_manager); });
  }

  void EndCompactor() {
    run_compactor_ = false;
    compactor_thread_.join();
  }

  const uint64_t blockstore_size_limit_ = 50000;
  const uint64_t blockstore_reuse_limit_ = 50000;
  const uint64_t buffersegment_size_limit_ = 5000000;
  const uint64_t buffersegment_reuse_limit_ = 5000000;
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
  std::default_random_engine generator_;
  storage::LogManager *log_manager_ = nullptr;
  storage::BlockCompactor compactor_;
  storage::AccessObserver access_observer_{&compactor_};

  const int8_t num_threads_ = 2;
  const uint32_t num_precomputed_txns_per_worker_ = 500;
  const uint32_t w_payment = 43;
  const uint32_t w_delivery = 4;
  const uint32_t w_order_status = 4;
  const uint32_t w_stock_level = 4;

  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};
  BlockWriter writer;

  void ServerLoop(tpcc::Database *tpcc_db) {
    struct sockaddr_in sin;
    std::memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = INADDR_ANY;
    sin.sin_port = htons(15712);

    auto listen_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (listen_fd < 0) {
      throw std::runtime_error("Failed to create listen socket");
    }

    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    bind(listen_fd, reinterpret_cast<struct sockaddr *>(&sin), sizeof(sin));
    listen(listen_fd, 12);
    struct sockaddr_storage addr;
    socklen_t addrlen = sizeof(addr);
    fprintf(stdout, "server listening on port 15712\n");

    while (true) {
      int new_conn_fd = accept(listen_fd, reinterpret_cast<struct sockaddr *>(&addr), &addrlen);
      if (new_conn_fd == -1)
        throw std::runtime_error("Failed to accept");
      storage::DataTable *order_line = tpcc_db->order_line_table_->table_.data_table;
      std::list<storage::RawBlock *> blocks = order_line->blocks_;
      // get table name (which is now repurposed to hot_ratio) from client to server
      double hot_ratio = 0;
      storage::PosixIoWrappers::ReadFully(new_conn_fd, &hot_ratio, sizeof(double));
      fprintf(stdout, "received hot ratio: %f\n", hot_ratio);
      std::bernoulli_distribution treat_as_hot{hot_ratio};
      fprintf(stdout, "Initiating Write\n");
      uint32_t blocks_written = 0;
      for (storage::RawBlock *block : blocks) {
        if (block->controller_.CurrentBlockState() != storage::BlockState::FROZEN || treat_as_hot(generator_)) {
          WriteHotBlock(tpcc_db, block, new_conn_fd);
        } else {
          WriteColdBlock(tpcc_db, block, new_conn_fd);
        }
        blocks_written++;
        if (blocks_written % 500 == 0) printf("%u blocks written\n", blocks_written);
      }
      printf("data write completed\n");
      close(new_conn_fd);
    }
  }

  void Run() {
    // one TPCC worker = one TPCC terminal = one thread
    std::vector<tpcc::Worker> workers;
    workers.reserve(num_threads_);

    thread_pool_.Shutdown();
    thread_pool_.SetNumWorkers(num_threads_);
    thread_pool_.Startup();

    // we need transactions, TPCC database, and GC
//  log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    auto tpcc_builder = tpcc::Builder(&block_store_);

    // random number generation is slow, so we precompute the args
    std::vector<std::vector<tpcc::TransactionArgs>> precomputed_args;
    precomputed_args.reserve(workers.size());

    tpcc::Deck deck(w_payment, w_order_status, w_delivery, w_stock_level);

    for (int8_t warehouse_id = 1; warehouse_id <= num_threads_; warehouse_id++) {
      std::vector<tpcc::TransactionArgs> txns;
      txns.reserve(num_precomputed_txns_per_worker_);
      for (uint32_t i = 0; i < num_precomputed_txns_per_worker_; i++) {
        switch (deck.NextCard()) {
          case tpcc::TransactionType::NewOrder:
            txns.emplace_back(tpcc::BuildNewOrderArgs(&generator_,
                                                      warehouse_id,
                                                      num_threads_));
            break;
          case tpcc::TransactionType::Payment:
            txns.emplace_back(tpcc::BuildPaymentArgs(&generator_,
                                                     warehouse_id,
                                                     num_threads_));
            break;
          case tpcc::TransactionType::OrderStatus:
            txns.emplace_back(tpcc::BuildOrderStatusArgs(&generator_,
                                                         warehouse_id,
                                                         num_threads_));
            break;
          case tpcc::TransactionType::Delivery:
            txns.emplace_back(tpcc::BuildDeliveryArgs(&generator_,
                                                      warehouse_id,
                                                      num_threads_));
            break;
          case tpcc::TransactionType::StockLevel:
            txns.emplace_back(tpcc::BuildStockLevelArgs(&generator_,
                                                        warehouse_id,
                                                        num_threads_));
            break;
          default:throw std::runtime_error("Unexpected transaction type.");
        }
      }
      precomputed_args.emplace_back(txns);
    }

    auto *const tpcc_db = tpcc_builder.Build();
    storage::DirtyGlobals::tpcc_db = tpcc_db;

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }
    printf("loading database...\n");
    compactor_ = storage::BlockCompactor();
    access_observer_ = storage::AccessObserver(&compactor_);

    tpcc::Loader::PopulateDatabase(&txn_manager, &generator_, tpcc_db, workers);
//    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(&txn_manager);

//    StartLogging();
    std::this_thread::sleep_for(std::chrono::seconds(1));  // Let GC clean up
    StartCompactor(&txn_manager);
    // define the TPCC workload
    auto tpcc_workload = [&](int8_t worker_id) {
      auto new_order = tpcc::NewOrder(tpcc_db);
      auto payment = tpcc::Payment(tpcc_db);
      auto order_status = tpcc::OrderStatus(tpcc_db);
      auto delivery = tpcc::Delivery(tpcc_db);
      auto stock_level = tpcc::StockLevel(tpcc_db);

      for (uint32_t i = 0; i < num_precomputed_txns_per_worker_; i++) {
        auto &txn_args = precomputed_args[worker_id][i];
        switch (txn_args.type) {
          case tpcc::TransactionType::NewOrder: {
            if (!new_order.Execute(&txn_manager,
                                   &generator_,
                                   tpcc_db,
                                   &workers[worker_id],
                                   txn_args))
              txn_args.aborted++;
            break;
          }
          case tpcc::TransactionType::Payment: {
            if (!payment.Execute(&txn_manager, &generator_, tpcc_db, &workers[worker_id], txn_args)) txn_args.aborted++;
            break;
          }
          case tpcc::TransactionType::OrderStatus: {
            if (!order_status.Execute(&txn_manager,
                                      &generator_,
                                      tpcc_db,
                                      &workers[worker_id],
                                      txn_args))
              txn_args.aborted++;
            break;
          }
          case tpcc::TransactionType::Delivery: {
            if (!delivery.Execute(&txn_manager,
                                  &generator_,
                                  tpcc_db,
                                  &workers[worker_id],
                                  txn_args))
              txn_args.aborted++;
            break;
          }
          case tpcc::TransactionType::StockLevel: {
            if (!stock_level.Execute(&txn_manager,
                                     &generator_,
                                     tpcc_db,
                                     &workers[worker_id],
                                     txn_args))
              txn_args.aborted++;
            break;
          }
          default:throw std::runtime_error("Unexpected transaction type.");
        }
      }
    };
    printf("starting workload\n");
    // run the TPCC workload to completion
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, &tpcc_workload] { tpcc_workload(i); });
      }
      thread_pool_.WaitUntilAllFinished();
      printf("transactions all submitted\n");
//      EndLogging();
    }
    // cleanup
    std::this_thread::sleep_for(std::chrono::seconds(5));  // Let GC clean up
    EndCompactor();
    EndGC();
    printf("order_line table:\n");
    tpcc_db->order_line_table_->table_.data_table->InspectTable();
    printf("\n\n\n");

    ServerLoop(tpcc_db);
    // Clean up the buffers from any non-inlined VarlenEntrys in the precomputed args
    for (const auto &worker_id : precomputed_args) {
      for (const auto &args : worker_id) {
        if ((args.type == tpcc::TransactionType::Payment || args.type == tpcc::TransactionType::OrderStatus) &&
            args.use_c_last && !args.c_last.IsInlined()) {
          delete[] args.c_last.Content();
        }
      }
    }
    delete tpcc_db;
    delete log_manager_;
  }

 private:
  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{1};
  transaction::TransactionManager txn_manager{&buffer_pool_, true, LOGGING_DISABLED};
  bool first_call = true;
  uint64_t buf[1024];

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }

  std::thread compactor_thread_;
  volatile bool run_compactor_ = false;
  const std::chrono::milliseconds compaction_period_{10};

  void CompactorThreadLoop(transaction::TransactionManager *const) {
    while (run_compactor_) {
      std::this_thread::sleep_for(compaction_period_);
      compactor_.ProcessCompactionQueue(&txn_manager);
    }
  }

  void WriteColdBlock(tpcc::Database *tpcc_db, storage::RawBlock *block, int fd) {
    storage::DataTable *table = tpcc_db->order_line_table_->table_.data_table;
    const storage::BlockLayout &layout = table->accessor_.GetBlockLayout();
    uint32_t num_records = table->accessor_.GetArrowBlockMetadata(block).NumRecords();
    writer.ResetWrite(num_records);
    storage::col_id_t varlen_id(1);
    auto *varlen_bitmap = table->accessor_.ColumnNullBitmap(block, varlen_id);
    auto *varlen_values = reinterpret_cast<storage::VarlenEntry *>(table->accessor_.ColumnStart(block, varlen_id));
    for (uint32_t i = 0; i < num_records; i++)
      writer.WriteVarlen(varlen_bitmap->Test(i) ? varlen_values + i : nullptr);

    for (uint16_t j = 2; j < layout.NumColumns(); j++) {
      storage::col_id_t id(j);
      uint32_t attr_size = layout.AttrSize(id);
      auto *bitmap = table->accessor_.ColumnNullBitmap(block, id);
      auto *values = table->accessor_.ColumnStart(block, id);
      for (uint32_t i = 0; i < num_records; i++)
        writer.WriteAttribute(bitmap->Test(i) ? values + i : nullptr, attr_size, j - 2);
    }
    writer.FlushBatch(fd);
  }

  void WriteHotBlock(tpcc::Database *tpcc_db, storage::RawBlock *block, int fd) {
    storage::DataTable *table = tpcc_db->order_line_table_->table_.data_table;
    const storage::BlockLayout &layout = table->accessor_.GetBlockLayout();
    if (first_call) {
      auto initializer = storage::ProjectedRowInitializer::CreateProjectedRowInitializer(layout, layout.AllColumns());
      initializer.InitializeRow(&buf);
      first_call = false;
    }
    auto *row = reinterpret_cast<storage::ProjectedRow *>(&buf);
    transaction::TransactionContext *txn = txn_manager.BeginTransaction();
    writer.ResetWrite(block->insert_head_);
    for (uint32_t i = 0; i < layout.NumSlots(); i++) {
      storage::TupleSlot slot(block, i);
      bool visible = table->Select(txn, slot, row);
      if (!visible) continue;
      for (uint16_t j = 0; j < row->NumColumns(); j++) {
        byte *value = row->AccessWithNullCheck(j);
        storage::col_id_t id = row->ColumnIds()[j];
        if (layout.IsVarlen(id))
          writer.WriteVarlen(reinterpret_cast<storage::VarlenEntry *>(value));
        else
          writer.WriteAttribute(value, layout.AttrSize(id), j - 1);
      }
    }
    writer.FlushBatch(fd);
  }
};
}

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

char read_buf[terrier::common::Constants::BLOCK_SIZE * 50];
#define READBUF_SIZE (terrier::common::Constants::BLOCK_SIZE * 10)
#define SUCCESS 0
#define NEED_MORE 1
#define DONE 2
struct ReadBuffer {
  template<class T>
  T ReadValue() {
    T val = *reinterpret_cast<T *>(read_buf + read_head);
    read_head += sizeof(T);
    return val;
  }

  int ReadBatch(ArrowBufferBuilder &builder) {
    if (read_head + 2 * sizeof(uint32_t) > size) return has_more ? NEED_MORE : DONE;
    uint32_t batchsize = ReadValue<uint32_t>(), total_size = ReadValue<uint32_t>();
    if (read_head + total_size > size) {
      read_head -= 2 * sizeof(uint32_t);
      return NEED_MORE;
    }
    for (uint i = 0; i < batchsize; i++)
      auto status1 UNUSED_ATTRIBUTE = builder.ol_delivery_d_builder.Append(ReadValue<uint64_t>());
    for (uint i = 0; i < batchsize; i++)
      auto status2 UNUSED_ATTRIBUTE = builder.ol_amount_builder.Append(ReadValue<uint64_t>());
    for (uint i = 0; i < batchsize; i++)
      auto status3 UNUSED_ATTRIBUTE = builder.o_id_builder.Append(ReadValue<uint32_t>());
    for (uint i = 0; i < batchsize; i++)
      auto status4 UNUSED_ATTRIBUTE = builder.ol_i_id_builder.Append(ReadValue<uint32_t>());
    for (uint i = 0; i < batchsize; i++)
      auto status5 UNUSED_ATTRIBUTE = builder.o_d_id_builder.Append(ReadValue<uint8_t>());
    for (uint i = 0; i < batchsize; i++)
      auto status6 UNUSED_ATTRIBUTE = builder.o_w_id_builder.Append(ReadValue<uint8_t>());
    for (uint i = 0; i < batchsize; i++)
      auto status7 UNUSED_ATTRIBUTE = builder.ol_number_builder.Append(ReadValue<uint8_t>());
    for (uint i = 0; i < batchsize; i++)
      auto status8 UNUSED_ATTRIBUTE = builder.ol_supply_w_id_builder.Append(ReadValue<uint8_t>());
    for (uint i = 0; i < batchsize; i++)
      auto status9 UNUSED_ATTRIBUTE = builder.ol_quantity_builder.Append(ReadValue<uint8_t>());
    for (uint i = 0; i < batchsize; i++) {
      uint32_t varlen_size = ReadValue<uint32_t>();
      auto status UNUSED_ATTRIBUTE = builder.ol_dist_info_builder.Append(read_buf + read_head, varlen_size);
      read_head += varlen_size;
    }
    return SUCCESS;
  }

  void ShiftToHead() {
    for (uint i = 0; i < size - read_head; i++)
      read_buf[i] = read_buf[read_head + i];
    size = size - read_head;
    read_head = 0;
  }

  void RefillBuffer(int sock) {
    while (size < READBUF_SIZE) {
      auto read_bytes = read(sock, read_buf + size, READBUF_SIZE - size);
      if (read_bytes == 0) {
        has_more = false;
        return;
      }
      size += read_bytes;
    }
  }

  uint32_t size = 0;
  uint32_t read_head = 0;
  bool has_more = true;
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

int main(int argc, char *argv[]) {
  if (argc != 4 && argc != 2) {
    fprintf(stderr,
            "Usage: %s  <type> <hostname> <hot_ratio>\n",
            argv[0]);
    return 1;
  }

  if (strcmp(argv[1], "server") == 0) {
    terrier::storage::init_index_logger();
    terrier::storage::init_storage_logger();
    terrier::transaction::init_transaction_logger();
    terrier::TpccLoader b;
    b.Run();
    return 0;
  }

  auto sock = sock_connect(argv[2], 15712);
  double hot_ratio = std::stod(std::string(argv[3]), nullptr);
  send(sock, &hot_ratio, sizeof(hot_ratio), 0);
  printf("request sent\n");
  std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

  ReadBuffer reader;
  ArrowBufferBuilder builder;
  uint32_t rows_read = 0;
  while (true) {
    int result = reader.ReadBatch(builder);
    if (result == DONE) break;
    if (result == NEED_MORE) {
      reader.ShiftToHead();
      reader.RefillBuffer(sock);
      continue;
    }
    rows_read++;
    if (rows_read % 50000 == 0) printf("Read %u rows \n", rows_read);
  }
  builder.Build();
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  fprintf(stdout,
          "Client side TOTAL duration: %lld\n",
          std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  return 0;
}