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

namespace terrier {
class TpccLoader {
 public:
  void StartGC(transaction::TransactionManager *const txn_manager) {

    gc_ = new storage::GarbageCollector(txn_manager, &access_observer_);
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

  void StartCompactor(transaction::TransactionManager *const txn_manager) {
    run_compactor_ = true;
    compactor_thread_ = std::thread([this, txn_manager] { CompactorThreadLoop(txn_manager); });
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
  const uint32_t num_precomputed_txns_per_worker_ = 100000;
  const uint32_t w_payment = 43;
  const uint32_t w_delivery = 4;
  const uint32_t w_order_status = 4;
  const uint32_t w_stock_level = 4;

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
      const storage::TupleAccessStrategy &accessor = order_line->accessor_;
      for (storage::RawBlock *block : blocks) {
        if (block->controller_.CurrentBlockState() != storage::BlockState::FROZEN) continue;
        std::shared_ptr<arrow::Table> table = storage::ArrowUtil::AssembleToArrowTable(accessor, block);
        // TODO(Tianyu): Do things!
      }
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
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
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

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }

  std::thread compactor_thread_;
  volatile bool run_compactor_ = false;
  const std::chrono::milliseconds compaction_period_{10};

  void CompactorThreadLoop(transaction::TransactionManager *const txn_manager) {
    while (run_compactor_) {
      std::this_thread::sleep_for(compaction_period_);
      compactor_.ProcessCompactionQueue(txn_manager);
    }
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