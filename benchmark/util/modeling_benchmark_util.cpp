#include "util/modeling_benchmark_util.h"
#include <algorithm>
#include <utility>
#include <vector>
#include "common/allocator.h"

namespace terrier {
RandomTransaction::RandomTransaction(ModelingBenchmarkObject *test_object)
    : test_object_(test_object),
      txn_(test_object->txn_manager_->BeginTransaction()),
      aborted_(false),
      start_time_(txn_->StartTime()),
      commit_time_(UINT64_MAX),
      buffer_(common::AllocationUtil::AllocateAligned(test_object->row_initializer_.ProjectedRowSize())) {}

RandomTransaction::~RandomTransaction() {
  if (!test_object_->gc_on_) delete txn_;
  delete[] buffer_;
  for (auto &entry : updates_) delete[] reinterpret_cast<byte *>(entry.second);
}

template <class Random>
void RandomTransaction::RandomUpdate(Random *generator) {
  if (aborted_) return;
  storage::TupleSlot updated =
      RandomTestUtil::UniformRandomElement(test_object_->last_checked_version_, generator)->first;
  std::vector<storage::col_id_t> update_col_ids =
      StorageTestUtil::ProjectionListRandomColumns(test_object_->layout_, generator);
  storage::ProjectedRowInitializer initializer(test_object_->layout_, update_col_ids);
  auto *update_buffer = buffer_;
  storage::ProjectedRow *update = initializer.InitializeRow(update_buffer);

  StorageTestUtil::PopulateRandomRow(update, test_object_->layout_, 0.0, generator);
  // TODO(Tianyu): Hardly efficient, but will do for testing.
  if (test_object_->wal_on_) {
    auto *record = txn_->StageWrite(nullptr, updated, initializer);
    TERRIER_MEMCPY(record->Delta(), update, update->Size());
  }
  auto result = test_object_->table_.Update(txn_, updated, *update);
  aborted_ = !result;
}

template <class Random>
void RandomTransaction::RandomInsert(Random *generator) {
  if (aborted_) return;
  std::vector<storage::col_id_t> insert_col_ids = StorageTestUtil::ProjectionListAllColumns(test_object_->layout_);
  storage::ProjectedRowInitializer initializer(test_object_->layout_, insert_col_ids);
  auto *insert_buffer = buffer_;
  storage::ProjectedRow *insert = initializer.InitializeRow(insert_buffer);

  StorageTestUtil::PopulateRandomRow(insert, test_object_->layout_, 0.0, generator);
  storage::TupleSlot inserted = test_object_->table_.Insert(txn_, *insert);
  // TODO(Tianyu): Hardly efficient, but will do for testing.
  if (test_object_->wal_on_) {
    auto *record = txn_->StageWrite(nullptr, inserted, initializer);
    TERRIER_MEMCPY(record->Delta(), insert, insert->Size());
  }
}

template <class Random>
void RandomTransaction::RandomSelect(Random *generator) {
  if (aborted_) return;
  storage::TupleSlot selected =
      RandomTestUtil::UniformRandomElement(test_object_->last_checked_version_, generator)->first;
  auto *select_buffer = buffer_;
  storage::ProjectedRow *select = test_object_->row_initializer_.InitializeRow(select_buffer);
  test_object_->table_.Select(txn_, selected, select);
}

void RandomTransaction::Finish(transaction::callback_fn callback, void *callback_arg) {
  if (aborted_)
    test_object_->txn_manager_->Abort(txn_);
  else
    commit_time_ = test_object_->txn_manager_->Commit(txn_, callback, callback_arg);
}

ModelingBenchmarkObject::ModelingBenchmarkObject(
    const std::vector<uint8_t> &attr_sizes, uint32_t initial_table_size, uint32_t txn_length,
    std::vector<double> operation_ratio, storage::BlockStore *block_store,
    storage::RecordBufferSegmentPool *buffer_pool, std::default_random_engine *generator, bool &task_submitting,
    std::vector<std::queue<time_point>> &task_queues, std::vector<common::SpinLatch> &task_queue_latches, bool gc_on,
    transaction::TransactionManager *txn_manager, storage::LogManager *log_manager)
    : txn_length_(txn_length),
      operation_ratio_(std::move(operation_ratio)),
      generator_(generator),
      layout_({attr_sizes}),
      table_(block_store, layout_, storage::layout_version_t(0)),
      txn_manager_(txn_manager),
      gc_on_(gc_on),
      wal_on_(log_manager != LOGGING_DISABLED),
      task_submitting_(task_submitting),
      task_queues_(task_queues),
      task_queue_latches_(task_queue_latches),
      abort_count_(0),
      commit_count_(0),
      latency_count_(0) {
  // Bootstrap the table to have the specified number of tuples
  PopulateInitialTable(initial_table_size, generator_);
}

ModelingBenchmarkObject::~ModelingBenchmarkObject() {
  if (!gc_on_) delete initial_txn_;
}

// Caller is responsible for freeing the returned results if bookkeeping is on.
void ModelingBenchmarkObject::SimulateOltp() {
  TestThreadPool thread_pool;

  std::atomic<uint64_t> total_latency(0);
  std::atomic<uint64_t> total_committed(0);
  // Number of aborted transactions
  std::atomic<uint32_t> num_aborts(0);

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

      uint32_t txn_cnt = 0;

      // LOG_INFO("before locking thread {}!!", id);
      task_queue_latches_[id].Lock();
      time_point start = task_queues_[id].front();
      // LOG_INFO("pop one item in thread {}, current size {}!", id, task_queues_[id].size());
      task_queues_[id].pop();
      // LOG_INFO("pop one item in thread {}, remaining size {}!", id, task_queues_[id].size());
      task_queue_latches_[id].Unlock();

      auto callback = [start, &thread_total_latency, &thread_total_committed] {
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<uint64_t, std::nano> diff = end - start;
        thread_total_latency += diff;
        thread_total_committed++;
      };
      // a captureless thunk
      auto thunk = [](void *arg) { (*static_cast<decltype(callback) *>(arg))(); };

      auto txn = new RandomTransaction(this);

      SimulateOneTransaction(txn, txn_cnt++, thunk, &callback);

      if (txn->aborted_) num_aborts++;

      if (gc_on_ == false) {
        delete txn;
      }
    }
    total_latency += thread_total_latency.count();
    total_committed += thread_total_committed;
  };
  thread_pool.RunThreadsUntilFinish(task_queues_.size(), workload);

  abort_count_ = num_aborts.load();
  commit_count_ = total_committed.load();
  latency_count_ = total_latency.load();
}

void ModelingBenchmarkObject::SimulateOneTransaction(terrier::RandomTransaction *txn, uint32_t txn_id,
                                                     transaction::callback_fn callback, void *callback_arg) {
  std::default_random_engine thread_generator(txn_id);

  auto insert = [&] { txn->RandomInsert(&thread_generator); };
  auto update = [&] { txn->RandomUpdate(&thread_generator); };
  auto select = [&] { txn->RandomSelect(&thread_generator); };
  RandomTestUtil::InvokeWorkloadWithDistribution({insert, update, select}, operation_ratio_, &thread_generator,
                                                 txn_length_);
  txn->Finish(callback, callback_arg);
}

template <class Random>
void ModelingBenchmarkObject::PopulateInitialTable(uint32_t num_tuples, Random *generator) {
  initial_txn_ = txn_manager_->BeginTransaction();
  byte *redo_buffer = nullptr;

  redo_buffer = common::AllocationUtil::AllocateAligned(row_initializer_.ProjectedRowSize());
  row_initializer_.InitializeRow(redo_buffer);

  for (uint32_t i = 0; i < num_tuples; i++) {
    auto *const redo = reinterpret_cast<storage::ProjectedRow *>(redo_buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, 0.0, generator);
    storage::TupleSlot inserted = table_.Insert(initial_txn_, *redo);
    // TODO(Tianyu): Hardly efficient, but will do for testing.
    if (wal_on_) {
      auto *record = initial_txn_->StageWrite(nullptr, inserted, row_initializer_);
      TERRIER_MEMCPY(record->Delta(), redo, redo->Size());
    }
    last_checked_version_.emplace_back(inserted, nullptr);
  }
  txn_manager_->Commit(initial_txn_, TestCallbacks::EmptyCallback, nullptr);
  // cleanup if not keeping track of all the inserts.
  delete[] redo_buffer;
}
}  // namespace terrier
