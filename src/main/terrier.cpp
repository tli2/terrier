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

#include "network/rdma/server.h"
#include "network/rdma/data_format.h"
#include "network/rdma/rdma.h"

#define ONE_MEGABYTE 1048576

namespace terrier {
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

  const int8_t num_threads_ = 6;
  const uint32_t num_precomputed_txns_per_worker_ = 5000000;
  const uint32_t w_payment = 43;
  const uint32_t w_delivery = 4;
  const uint32_t w_order_status = 4;
  const uint32_t w_stock_level = 4;

  struct config_t config = {
      NULL,                         /* device_name */
      NULL,                         /* server_name */
      19875,                        /* tcp_port */
      1,                            /* ib_port */
      1                             /* gid_idx */
  };

  struct size_pair sizes = {0, 0};

  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};

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
    while (true) {
      int new_conn_fd = accept(listen_fd, reinterpret_cast<struct sockaddr *>(&addr), &addrlen);
      if (new_conn_fd == -1)
        throw std::runtime_error("Failed to accept");
      storage::DataTable *order_line = tpcc_db->order_line_table_->table_.data_table;
      std::list<storage::RawBlock *> blocks = order_line->blocks_;

      // RDMA stuff
      struct resources res;
      resources_init(&res);
      res.sock = new_conn_fd;

      // get table name (which is now repurposed to hot_ratio) from client to server
      char table_name[16];
      memset(table_name, 0, 16);
      int rc = sock_read_data(res.sock, sizeof(table_name), table_name);
      if (rc < 0) {
        fprintf(stderr, "failed to receive data from client\n");
        return;
      }
      fprintf(stdout, "received hot ratio: %s\n", table_name);
      double hot_ratio = std::stod(std::string(table_name), nullptr);
      std::bernoulli_distribution treat_as_hot{hot_ratio};

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
        return;
      }
      /* connect the QPs */
      if (connect_qp (&res, config)) {
        fprintf (stderr, "failed to connect QPs\n");
        return;
      }

      // initiate rdma write
      uint64_t remote_addr_start = res.remote_props.addr;
      uint64_t remote_curr_addr = remote_addr_start;
      const storage::TupleAccessStrategy &accessor = order_line->accessor_;
      fprintf (stdout, "Now initiating RDMA write\n");
      std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
      for (storage::RawBlock *block : blocks) {
        std::shared_ptr<arrow::Table> table UNUSED_ATTRIBUTE;
        if (block->controller_.CurrentBlockState() != storage::BlockState::FROZEN || treat_as_hot(generator_)) {
          table = MaterializeHotBlock(tpcc_db, block);
        } else {
          table = storage::ArrowUtil::AssembleToArrowTable(accessor, block);
        }

        int num_cols = table->num_columns();
        // fprintf (stdout, "num columns: %d\n", num_cols);
        for (int ci = 0; ci < num_cols; ci++) {
          // printf ("index: %d\n", ci);
          auto col = table->column(ci);
          // std::cout << "---- column name: " << col->field()->type()->id() << ", should not be " << arrow::Type::type::STRING << std::endl;
          // if (col->field()->type()->id() == arrow::Type::type::STRING) continue;
          // fprintf (stdout, "--- column name: %s\n", col->field()->name());
          auto array_data = col->data()->chunk(0)->data();
          int64_t length = array_data->buffers.size();
          // std::cout << "  array_data length: " << length << std::endl;
          for (int64_t bi = 0; bi < length; bi++) {
            auto buffer = array_data->buffers[bi];
            int64_t buf_size = buffer->size();
            // std::cout << "  size: " << buf_size << std::endl;
            if (buf_size == 0) break;
            uint8_t *data = (uint8_t *)buffer->data();
            
            if (0 != do_send(&res, reinterpret_cast<char *>(data), buf_size, remote_curr_addr)) return;
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
        return;
      }
      fprintf (stderr, "final sync done\n");

      // cleanup
      // resources_destroy (&res);

      // return;

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
//    printf("history table:\n");
//    tpcc_db->history_table_->table_.data_table->InspectTable();
//    printf("item table:\n");
//    tpcc_db->item_table_->table_.data_table->InspectTable();
//    printf("order table:\n");
//    tpcc_db->order_table_->table_.data_table->InspectTable();
    printf("order_line table:\n");
    tpcc_db->order_line_table_->table_.data_table->InspectTable();
    printf("\n\n\n");
//    printf("total number of transactions: %u\n", num_precomputed_txns_per_worker_ * num_threads_);
//    printf("number of transactions stalled: %u\n", storage::DirtyGlobals::blocked_transactions.load());
//    uint32_t aborted = 0;
//    for (auto &entry : precomputed_args)
//      for (auto &arg : entry)
//        aborted += arg.aborted;
//    printf("number of transactions aborted: %u\n", aborted);

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

  template<class IntType, class T>
  void Append(T *builder, storage::ProjectedRow *row, uint16_t i) {
    auto *int_pointer = row->AccessWithNullCheck(i);
    if (int_pointer == nullptr)
      auto status UNUSED_ATTRIBUTE = builder->AppendNull();
    else
      auto status1 UNUSED_ATTRIBUTE = builder->Append(*reinterpret_cast<IntType *>(int_pointer));
  }

  std::shared_ptr<arrow::Table> MaterializeHotBlock(tpcc::Database *tpcc_db, storage::RawBlock *block) {
    storage::DataTable *table = tpcc_db->order_line_table_->table_.data_table;
    const storage::BlockLayout &layout = table->accessor_.GetBlockLayout();
    if (first_call) {
      auto initializer = storage::ProjectedRowInitializer::CreateProjectedRowInitializer(layout, layout.AllColumns());
      initializer.InitializeRow(&buf);
      first_call = false;
    }
    auto *row = reinterpret_cast<storage::ProjectedRow *>(&buf);
    transaction::TransactionContext *txn = txn_manager.BeginTransaction();
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
    for (uint32_t i = 0; i < layout.NumSlots(); i++) {
      storage::TupleSlot slot(block, i);
      bool visible = table->Select(txn, slot, row);
      if (!visible) continue;
      Append<uint32_t>(&o_id_builder, row, storage::DirtyGlobals::ol_o_id_insert_pr_offset);
      Append<uint8_t>(&o_d_id_builder, row, storage::DirtyGlobals::ol_d_id_insert_pr_offset);
      Append<uint8_t>(&o_w_id_builder, row, storage::DirtyGlobals::ol_w_id_insert_pr_offset);
      Append<uint8_t>(&ol_number_builder, row, storage::DirtyGlobals::ol_number_insert_pr_offset);
      Append<uint32_t>(&ol_i_id_builder, row, storage::DirtyGlobals::ol_i_id_insert_pr_offset);
      Append<uint8_t>(&ol_supply_w_id_builder, row, storage::DirtyGlobals::ol_supply_w_id_insert_pr_offset);
      Append<uint64_t>(&ol_delivery_d_builder, row, storage::DirtyGlobals::ol_delivery_d_insert_pr_offset);
      Append<uint8_t>(&ol_quantity_builder, row, storage::DirtyGlobals::ol_quantity_insert_pr_offset);
      Append<double>(&ol_amount_builder, row, storage::DirtyGlobals::ol_amount_insert_pr_offset);

      auto *varlen_pointer = row->AccessWithNullCheck(storage::DirtyGlobals::ol_dist_info_insert_pr_offset);
      if (varlen_pointer == nullptr) {
        auto status UNUSED_ATTRIBUTE = ol_dist_info_builder.AppendNull();
      } else {
        auto *entry = reinterpret_cast<storage::VarlenEntry *>(varlen_pointer);
        auto status2 UNUSED_ATTRIBUTE =
            ol_dist_info_builder.Append(reinterpret_cast<const uint8_t *>(entry->Content()), entry->Size());
      }
    }

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

    std::vector<std::shared_ptr<arrow::Field>> schema_vector{arrow::field("o_id", arrow::uint32()),
                                                             arrow::field("o_d_id", arrow::uint8()),
                                                             arrow::field("o_w_id", arrow::uint8()),
                                                             arrow::field("ol_number", arrow::uint8()),
                                                             arrow::field("ol_i_id", arrow::uint32()),
                                                             arrow::field("ol_supply_w_id", arrow::uint8()),
                                                             arrow::field("ol_delivery_d", arrow::uint64()),
                                                             arrow::field("ol_quantity", arrow::uint8()),
                                                             arrow::field("ol_amount", arrow::float64()),
                                                             arrow::field("ol_dist_info", arrow::utf8())};

    std::vector<std::shared_ptr<arrow::Array>> table_vector{o_id, o_d_id, o_w_id, ol_number,
                                                            ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info};
    return arrow::Table::Make(std::make_shared<arrow::Schema>(schema_vector), table_vector);
  }
};
}

int main() {
  terrier::storage::init_index_logger();
  terrier::storage::init_storage_logger();
  terrier::transaction::init_transaction_logger();
  terrier::TpccLoader b;
  b.Run();
  return 0;
}