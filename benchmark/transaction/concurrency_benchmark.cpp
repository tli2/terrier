#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_thread_pool.h"

#define LOG_FILE_NAME "concurrency_benchmark.log"

namespace terrier {

// This benchmark simulates the concurrent query execution in a YCSB-like benchmark.
// We are interested in the system's raw performance, so the tuple's contents are intentionally left garbage and we
// don't verify correctness. That's the job of the Google Tests.

class ConcurrencyBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    LOG_INFO("Setup once.\n");
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);

    if (enable_gc_and_wal_ == true) {
      log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    }
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, enable_gc_and_wal_, log_manager_);

    if (enable_gc_and_wal_ == true) {
      // start logging and GC threads
      StartLogging(10);
      StartGC(txn_manager_, 10);
    }

    // Initialize the data structure for task queues
    for (uint32_t i = 0; i < num_threads_; ++i) {
      task_queues_.emplace_back(std::queue<time_point>());
    }
  }

  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
    if (enable_gc_and_wal_ == true) {
      EndGC();
      EndLogging();

      delete log_manager_;
    }
    delete txn_manager_;
    task_queues_.clear();
  }

  // Tuple layout
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_, column_size_, column_size_, column_size_,
                                      column_size_, column_size_, column_size_, column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};

  // Workload
  const uint32_t num_operations_ = 1000000;
  // const uint32_t num_threads_ = TestThreadPool::HardwareConcurrency();
  const uint32_t num_threads_ = 4;
  const uint64_t buffer_pool_reuse_limit_ = 10000000;
  // Number of transactions per second
  const uint64_t txn_rates_ = 1000000;
  // Throttling threshold for the work queue size
  const uint64_t work_queue_threshold_ = 100;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000, 1000};
  // storage::RecordBufferSegmentPool buffer_pool_{1000, 100};
  storage::RecordBufferSegmentPool buffer_pool_{buffer_pool_reuse_limit_, buffer_pool_reuse_limit_};
  std::thread log_thread_;
  bool logging_;
  volatile bool run_gc_ = false;
  std::thread gc_thread_;
  storage::GarbageCollector *gc_;
  transaction::TransactionManager *txn_manager_;
  storage::LogManager *log_manager_ = LOGGING_DISABLED;

  bool enable_gc_and_wal_ = false;

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  typedef std::chrono::high_resolution_clock::time_point time_point;
  // Latches to protect the worker queues
  std::vector<common::SpinLatch> task_queue_latches_{num_threads_};
  // One task queue per worker thead
  std::vector<std::queue<time_point>> task_queues_;

  std::thread task_submitting_thread_;
  bool task_submitting_;

  void StartTaskSubmitting(uint64_t task_submit_period_nano) {
    task_submitting_ = true;
    // LOG_INFO("start submitting");
    task_submitting_thread_ =
        std::thread([task_submit_period_nano, this] { TaskSubmitThreadLoop(task_submit_period_nano); });
  }

  void EndTaskSubmitting() { task_submitting_thread_.join(); }

 private:
  void StartLogging(uint32_t log_period_milli) {
    logging_ = true;
    log_thread_ = std::thread([log_period_milli, this] { LogThreadLoop(log_period_milli); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_->Shutdown();
  }

  void StartGC(transaction::TransactionManager *txn_manager, uint32_t gc_period_milli) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([gc_period_milli, this] { GCThreadLoop(gc_period_milli); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  void LogThreadLoop(uint32_t log_period_milli) {
    while (logging_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(log_period_milli));
      log_manager_->Process();
    }
  }

  void GCThreadLoop(uint32_t gc_period_milli) {
    while (run_gc_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_period_milli));
      gc_->PerformGarbageCollection();
    }
  }

  void TaskSubmitThreadLoop(uint64_t task_submit_period_nano) {
    auto start = std::chrono::high_resolution_clock::now();
    time_point current;
    std::chrono::duration<uint64_t, std::nano> diff;
    uint64_t submitted_count = 0;
    uint64_t shortest_queue_size;
    uint32_t shortest_queue_id;

    while (submitted_count < num_operations_) {
      current = std::chrono::high_resolution_clock::now();
      diff = current - start;
      if (diff.count() > submitted_count * task_submit_period_nano) {
        submitted_count++;
        shortest_queue_size = task_queues_[0].size();
        shortest_queue_id = 0;
        for (uint32_t i = 1; i < num_threads_; ++i) {
          auto queue_size = task_queues_[i].size();
          if (queue_size < shortest_queue_size) {
            shortest_queue_size = queue_size;
            shortest_queue_id = i;
          }
        }
        // Reject txns if the queue is larger than 100
        if (shortest_queue_size > work_queue_threshold_) {
          // LOG_INFO("Rejecting 1!");
          continue;
        }

        task_queue_latches_[shortest_queue_id].Lock();
        task_queues_[shortest_queue_id].emplace(current);
        // LOG_INFO("add one item in thread {}, current size {}!", shortest_queue_id,
        //         task_queues_[shortest_queue_id].size());
        task_queue_latches_[shortest_queue_id].Unlock();
      }
      // std::this_thread::sleep_for(std::chrono::nanoseconds(task_submit_period_nano / 10));
    }
    task_submitting_ = false;
    // LOG_INFO("finished submitting!");
  }
};

// Insert the num_inserts_ of tuples into a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentInsert)(benchmark::State &state) {
  TestThreadPool thread_pool;

  std::atomic<uint64_t> total_latency(0);
  std::atomic<uint64_t> total_committed(0);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    LOG_INFO("start the loop");
    // The data table used in the experiments
    storage::DataTable table{&block_store_, layout_, storage::layout_version_t(0)};

    auto workload = [&](uint32_t id) {
      std::chrono::duration<uint64_t, std::nano> thread_total_latency(0);
      int thread_total_committed(0);

      while (task_submitting_ == true or task_queues_[id].size() > 0) {
        // LOG_INFO("flag {} size {} thread {}", task_submitting_, task_queues_[id].size(), id);
        // LOG_INFO("running!!");
        if (task_queues_[id].size() == 0) {
          std::this_thread::sleep_for(std::chrono::nanoseconds(1));
          continue;
        }
        // Insert buffer pointers
        byte *redo_buffer;
        storage::ProjectedRow *redo;

        redo_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
        redo = initializer_.InitializeRow(redo_buffer);
        StorageTestUtil::PopulateRandomRow(redo, layout_, 0, &generator_);

        // LOG_INFO("before locking thread {}!!", id);
        task_queue_latches_[id].Lock();
        time_point start = task_queues_[id].front();
        // LOG_INFO("pop one item in thread {}, current size {}!", id, task_queues_[id].size());
        task_queues_[id].pop();
        // LOG_INFO("pop one item in thread {}, remaining size {}!", id, task_queues_[id].size());
        task_queue_latches_[id].Unlock();
        auto *txn = txn_manager_->BeginTransaction();
        auto inserted = table.Insert(txn, *redo);
        auto callback = [start, &thread_total_latency, &thread_total_committed] {
          auto end = std::chrono::high_resolution_clock::now();
          std::chrono::duration<uint64_t, std::nano> diff = end - start;
          thread_total_latency += diff;
          thread_total_committed++;
        };
        // a captureless thunk
        auto thunk = [](void *arg) { (*static_cast<decltype(callback) *>(arg))(); };
        txn_manager_->Commit(txn, thunk, &callback);

        if (enable_gc_and_wal_ == true) {
          auto *record = txn->StageWrite(nullptr, inserted, initializer_);
          TERRIER_MEMCPY(record->Delta(), redo, redo->Size());
        }

        delete[] redo_buffer;

        if (enable_gc_and_wal_ == false) {
          delete txn;
        }
      }
      total_latency += thread_total_latency.count();
      total_committed += thread_total_committed;
    };
    StartTaskSubmitting(1000000000 / txn_rates_);
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
    EndTaskSubmitting();
  }

  LOG_INFO("Average latency: {}", total_latency.load() / total_committed.load());
  state.SetItemsProcessed(total_committed.load());
}

// Read the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentRandomRead)(benchmark::State &state) {
  TestThreadPool thread_pool;

  std::atomic<uint64_t> total_latency(0);
  std::atomic<uint64_t> total_committed(0);

  // The data table used in the experiments
  storage::DataTable table{&block_store_, layout_, storage::layout_version_t(0)};

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_operations_; ++i) {
    read_order.emplace_back(table.Insert(&txn, *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    LOG_INFO("start the loop");
    auto workload = [&](uint32_t id) {
      std::chrono::duration<uint64_t, std::nano> thread_total_latency(0);
      int thread_total_committed(0);
      int i = 0;
      while (task_submitting_ == true or task_queues_[id].size() > 0) {
        if (task_queues_[id].size() == 0) {
          std::this_thread::sleep_for(std::chrono::nanoseconds(1));
          continue;
        }

        // Read buffer pointers;
        byte *read_buffer;
        storage::ProjectedRow *read;

        // generate a ProjectedRow buffer to Read
        read_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
        read = initializer_.InitializeRow(read_buffer);

        task_queue_latches_[id].Lock();
        time_point start = task_queues_[id].front();
        task_queues_[id].pop();
        task_queue_latches_[id].Unlock();
        auto *txn = txn_manager_->BeginTransaction();
        table.Select(txn, read_order[(rand_read_offsets[id] + i) % read_order.size()], read);

        auto callback = [start, &thread_total_latency, &thread_total_committed] {
          auto end = std::chrono::high_resolution_clock::now();
          std::chrono::duration<uint64_t, std::nano> diff = end - start;
          thread_total_latency += diff;
          thread_total_committed++;
        };
        // a captureless thunk
        auto thunk = [](void *arg) { (*static_cast<decltype(callback) *>(arg))(); };
        txn_manager_->Commit(txn, thunk, &callback);

        delete[] read_buffer;

        if (enable_gc_and_wal_ == false) {
          delete txn;
        }
      }
      total_latency += thread_total_latency.count();
      total_committed += thread_total_committed;
    };
    StartTaskSubmitting(1000000000 / txn_rates_);
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
    EndTaskSubmitting();
  }

  LOG_INFO("Average latency: {}", total_latency.load() / total_committed.load());
  state.SetItemsProcessed(total_committed.load());
}

// Update the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentRandomUpdate)(benchmark::State &state) {
  TestThreadPool thread_pool;

  std::atomic<uint64_t> total_latency(0);
  std::atomic<uint64_t> total_committed(0);

  // The data table used in the experiments
  storage::DataTable table{&block_store_, layout_, storage::layout_version_t(0)};

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_operations_; ++i) {
    read_order.emplace_back(table.Insert(&txn, *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }

  // Number of aborted transactions
  std::atomic<uint32_t> num_aborts(0);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    LOG_INFO("start the loop");
    auto workload = [&](uint32_t id) {
      std::chrono::duration<uint64_t, std::nano> thread_total_latency(0);
      int thread_total_committed(0);
      int i = 0;
      while (task_submitting_ == true or task_queues_[id].size() > 0) {
        if (task_queues_[id].size() == 0) {
          std::this_thread::sleep_for(std::chrono::nanoseconds(1));
          continue;
        }

        // Update buffer pointers
        byte *redo_buffer;
        storage::ProjectedRow *redo;

        redo_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
        redo = initializer_.InitializeRow(redo_buffer);
        StorageTestUtil::PopulateRandomRow(redo, layout_, 0, &generator_);

        task_queue_latches_[id].Lock();
        time_point start = task_queues_[id].front();
        task_queues_[id].pop();
        task_queue_latches_[id].Unlock();
        auto *txn = txn_manager_->BeginTransaction();
        auto update_slot = read_order[(rand_read_offsets[id] + i) % read_order.size()];
        if (enable_gc_and_wal_ == true) {
          auto *record = txn->StageWrite(nullptr, update_slot, initializer_);
          TERRIER_MEMCPY(record->Delta(), redo_, redo_->Size());
        }
        bool update_result = table.Update(txn, update_slot, *redo);
        if (update_result == false) {
          // LOG_INFO("Aborting because update failure!!\n");
          txn_manager_->Abort(txn);
          num_aborts += 1;
        } else {
          auto callback = [start, &thread_total_latency, &thread_total_committed] {
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<uint64_t, std::nano> diff = end - start;
            thread_total_latency += diff;
            thread_total_committed++;
          };

          // a captureless thunk
          auto thunk = [](void *arg) { (*static_cast<decltype(callback) *>(arg))(); };
          txn_manager_->Commit(txn, thunk, &callback);
        }

        delete[] redo_buffer;

        if (enable_gc_and_wal_ == false) {
          delete txn;
        }
      }
      total_latency += thread_total_latency.count();
      total_committed += thread_total_committed;
    };
    StartTaskSubmitting(1000000000 / txn_rates_);
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
    EndTaskSubmitting();
  }

  LOG_INFO("Number of aborted txns: {}", num_aborts.load());
  LOG_INFO("Average latency: {}", total_latency.load() / total_committed.load());
  state.SetItemsProcessed(total_committed.load());
}

// Delete the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentRandomDelete)(benchmark::State &state) {
  TestThreadPool thread_pool;

  std::atomic<uint64_t> total_latency(0);
  std::atomic<uint64_t> total_committed(0);

  // The data table used in the experiments
  storage::DataTable table{&block_store_, layout_, storage::layout_version_t(0)};

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_operations_; ++i) {
    read_order.emplace_back(table.Insert(&txn, *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }

  // Number of aborted transactions
  std::atomic<uint32_t> num_aborts(0);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      std::chrono::duration<uint64_t, std::nano> thread_total_latency(0);
      int thread_total_committed(0);
      int i = 0;
      while (task_submitting_ == true or task_queues_[id].size() > 0) {
        if (task_queues_[id].size() == 0) {
          std::this_thread::sleep_for(std::chrono::nanoseconds(1));
          continue;
        }

        task_queue_latches_[id].Lock();
        time_point start = task_queues_[id].front();
        task_queues_[id].pop();
        task_queue_latches_[id].Unlock();
        auto *txn = txn_manager_->BeginTransaction();
        auto delete_slot = read_order[(rand_read_offsets[id] + i) % read_order.size()];
        bool delete_result = table.Delete(txn, delete_slot);
        if (delete_result == false) {
          txn_manager_->Abort(txn);
          num_aborts += 1;
        } else {
          auto callback = [start, &thread_total_latency, &thread_total_committed] {
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<uint64_t, std::nano> diff = end - start;
            thread_total_latency += diff;
            thread_total_committed++;
          };

          // a captureless thunk
          auto thunk = [](void *arg) { (*static_cast<decltype(callback) *>(arg))(); };
          txn_manager_->Commit(txn, thunk, &callback);
        }

        if (enable_gc_and_wal_ == false) {
          delete txn;
        }
      }
      total_latency += thread_total_latency.count();
      total_committed += thread_total_committed;
    };
    StartTaskSubmitting(1000000000 / txn_rates_);
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
    EndTaskSubmitting();
  }

  LOG_INFO("Number of aborted txns: {}", num_aborts.load());
  LOG_INFO("Average latency: {}", total_latency.load() / total_committed.load());
  state.SetItemsProcessed(total_committed.load());
}  // namespace terrier

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime()->MinTime(10);

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentRandomRead)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->MinTime(10);

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentRandomUpdate)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->MinTime(10);

// BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentRandomDelete)->Unit(benchmark::kMillisecond)->UseRealTime();
}  // namespace terrier
