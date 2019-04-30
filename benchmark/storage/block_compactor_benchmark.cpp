#include <arrow/api.h>
#include <random>
#include <utility>
#include <vector>
#include <memory>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/block_compactor.h"
#include "storage/garbage_collector.h"
#include "util/storage_test_util.h"

namespace terrier {
class BlockCompactorBenchmark : public benchmark::Fixture {
 public:

 protected:
  storage::BlockStore block_store_{5000, 5000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  storage::BlockLayout layout_{{8, VARLEN_COLUMN, VARLEN_COLUMN}};
  storage::TupleAccessStrategy accessor_{layout_};

  storage::DataTable table_{&block_store_, layout_, storage::layout_version_t(0)};
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};
  storage::GarbageCollector gc_{&txn_manager_};
  storage::BlockCompactor compactor_;

  uint32_t num_blocks_ = 500;

  uint32_t CalculateOptimal(std::vector<storage::RawBlock *> blocks) {
    std::unordered_map<storage::RawBlock *, uint32_t> num_tuples;
    uint32_t total_num_tuples = 0;
    const storage::TupleAccessStrategy &accessor = table_.accessor_;
    const storage::BlockLayout &layout = accessor.GetBlockLayout();
    for (storage::RawBlock *block : blocks) {
      uint32_t &count = num_tuples[block];
      auto *bitmap = accessor.AllocationBitmap(block);
      for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
        if (!bitmap->Test(offset)) {
          count++;
          total_num_tuples++;
        }
      }
    }

    std::sort(blocks.begin(), blocks.end(), [&](storage::RawBlock *a, storage::RawBlock *b) {
      auto a_filled = num_tuples[a];
      auto b_filled = num_tuples[b];
      // We know these finds will not return end() because we constructed the vector from the map
      return a_filled >= b_filled;
    });

    uint32_t f_size = total_num_tuples / layout.NumSlots();
    uint32_t p_size = total_num_tuples % layout.NumSlots();
    uint32_t min_movement = UINT32_MAX;
    for (storage::RawBlock *p : blocks) {
      uint32_t num_movements = 0;
      auto *bitmap = accessor.AllocationBitmap(p);
      for (uint32_t offset = 0; offset < p_size; offset++) {
        if (!bitmap->Test(offset)) {
          num_movements++;
          total_num_tuples++;
        }
      }
      uint32_t num_f = 0;
      for (storage::RawBlock *f : blocks) {
        if (f == p) continue;
        num_f++;
        num_movements += layout.NumSlots() - num_tuples[f];
        if (num_f == f_size) break;
      }
      if (num_movements < min_movement) min_movement = num_movements;
    }
    return min_movement;
  }

  void RunFull(benchmark::State &state, double percent_empty,
               storage::ArrowColumnType type = storage::ArrowColumnType::GATHERED_VARLEN) {
    uint32_t num_tuples = 0;
//    uint32_t optimal_move = 0;
    // NOLINTNEXTLINE
    for (auto _ : state) {
      compactor_.tuples_moved_ = 0;
      num_tuples = 0;
      std::vector<storage::RawBlock *> blocks;
      for (uint32_t i = 0; i < num_blocks_; i++) {
        storage::RawBlock *block = block_store_.Get();
        block->data_table_ = &table_;
        num_tuples += StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, percent_empty, &generator_);
        auto &arrow_metadata = accessor_.GetArrowBlockMetadata(block);
        for (storage::col_id_t col_id : layout_.AllColumns()) {
          if (layout_.IsVarlen(col_id))
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = type;
          else
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
        }
        blocks.push_back(block);
      }
//      optimal_move = CalculateOptimal(blocks);
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      uint64_t compaction_ms;
      {
        common::ScopedTimer timer(&compaction_ms);
        compactor_.ProcessCompactionQueue(&txn_manager_);
      }
      gc_.PerformGarbageCollection();
      gc_.PerformGarbageCollection();
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      uint64_t gather_ms;
      {
        common::ScopedTimer timer(&gather_ms);
        compactor_.ProcessCompactionQueue(&txn_manager_);
      }
      for (storage::RawBlock *block : blocks) block_store_.Release(block);
      state.SetIterationTime(static_cast<double>(gather_ms + compaction_ms) / 1000.0);
    }
    state.SetItemsProcessed(static_cast<int64_t>(num_blocks_ * state.iterations()));

//    printf("With %f percent empty, %u tuples in total, optimal %u tuples moved in total\n", percent_empty, num_tuples, optimal_move);
  }

  void RunCompaction(benchmark::State &state,
                     double percent_empty,
                     storage::ArrowColumnType type = storage::ArrowColumnType::GATHERED_VARLEN) {
    // NOLINTNEXTLINE
    for (auto _ : state) {
      std::vector<storage::RawBlock *> blocks;
      for (uint32_t i = 0; i < num_blocks_; i++) {
        storage::RawBlock *block = block_store_.Get();
        block->data_table_ = &table_;
        StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, percent_empty, &generator_);
        auto &arrow_metadata = accessor_.GetArrowBlockMetadata(block);
        for (storage::col_id_t col_id : layout_.AllColumns()) {
          if (layout_.IsVarlen(col_id))
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = type;
          else
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
        }
        blocks.push_back(block);
      }
      // generate our table and instantiate GC
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      uint64_t elapsed_ms;
      {
        common::ScopedTimer timer(&elapsed_ms);
        compactor_.ProcessCompactionQueue(&txn_manager_);
      }
      gc_.PerformGarbageCollection();
      gc_.PerformGarbageCollection();
      for (storage::RawBlock *block : blocks) block_store_.Release(block);
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }
    state.SetItemsProcessed(static_cast<int64_t>(num_blocks_ * state.iterations()));
  }

  void RunGather(benchmark::State &state, double percent_empty, storage::ArrowColumnType type) {
    // NOLINTNEXTLINE
    for (auto _ : state) {
      std::vector<storage::RawBlock *> blocks;
      for (uint32_t i = 0; i < num_blocks_; i++) {
        storage::RawBlock *block = block_store_.Get();
        block->data_table_ = &table_;
        StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, percent_empty, &generator_);
        auto &arrow_metadata = accessor_.GetArrowBlockMetadata(block);
        for (storage::col_id_t col_id : layout_.AllColumns()) {
          if (layout_.IsVarlen(col_id))
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = type;
          else
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
        }
        blocks.push_back(block);
      }
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      compactor_.ProcessCompactionQueue(&txn_manager_);
      gc_.PerformGarbageCollection();
      gc_.PerformGarbageCollection();
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      uint64_t time;
      {
        common::ScopedTimer timer(&time);
        compactor_.ProcessCompactionQueue(&txn_manager_);
      }
      for (storage::RawBlock *block : blocks) block_store_.Release(block);
      state.SetIterationTime(static_cast<double>(time) / 1000.0);
    }
    state.SetItemsProcessed(static_cast<int64_t>(num_blocks_ * state.iterations()));
  }

  void RunStrawman(benchmark::State &state, double percent_empty) {
    std::vector<storage::RawBlock *> start_blocks;
    for (uint32_t i = 0; i < num_blocks_; i++) {
      storage::RawBlock *block = block_store_.Get();
      block->data_table_ = &table_;
      StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, percent_empty, &generator_);
      start_blocks.push_back(block);
    }
    // NOLINTNEXTLINE
    for (auto _ : state) {
      std::vector<storage::RawBlock *> blocks;
      for (storage::RawBlock *block : start_blocks) {
        storage::RawBlock *copied_block = block_store_.Get();
        std::memcpy(copied_block, block, common::Constants::BLOCK_SIZE);
        blocks.push_back(copied_block);
      }

      uint64_t elapsed_ms;
      {
        common::ScopedTimer timer(&elapsed_ms);
        transaction::TransactionContext *txn = txn_manager_.BeginTransaction();
        storage::ProjectedRowInitializer initializer =
            storage::ProjectedRowInitializer::CreateProjectedRowInitializer(layout_,
                                                                            StorageTestUtil::ProjectionListAllColumns(
                                                                                layout_));
        byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
        auto *read_row = initializer.InitializeRow(buffer);
//        arrow::Int64Builder int_builder, int_builder2;
        arrow::StringBuilder string_builder, string_builder2;
        for (storage::RawBlock *block : blocks) {
          for (uint32_t i = 0; i < layout_.NumSlots(); i++) {
            storage::TupleSlot slot(block, i);
            bool visible = table_.Select(txn, slot, read_row);
            if (!visible) continue;
//            auto *int_pointer = read_row->AccessWithNullCheck(1);
//            if (int_pointer == nullptr)
//              auto status5 UNUSED_ATTRIBUTE = int_builder.AppendNull();
//            else
//              auto status6 UNUSED_ATTRIBUTE = int_builder.Append(*reinterpret_cast<uint64_t *>(int_pointer));
//            auto *int_pointer2 = read_row->AccessWithNullCheck(2);
//            if (int_pointer2 == nullptr)
//              auto status10 UNUSED_ATTRIBUTE = int_builder2.AppendNull();
//            else
//              auto status11 UNUSED_ATTRIBUTE = int_builder2.Append(*reinterpret_cast<uint64_t *>(int_pointer2));
            auto *varlen_pointer = read_row->AccessWithNullCheck(0);
            if (varlen_pointer == nullptr) {
              auto status UNUSED_ATTRIBUTE = string_builder.AppendNull();
            } else {
              auto *entry = reinterpret_cast<storage::VarlenEntry *>(varlen_pointer);
              auto status2 UNUSED_ATTRIBUTE =
                  string_builder.Append(reinterpret_cast<const uint8_t *>(entry->Content()), entry->Size());
            }

            auto *varlen_pointer2 = read_row->AccessWithNullCheck(1);
            if (varlen_pointer2 == nullptr) {
              auto status UNUSED_ATTRIBUTE = string_builder2.AppendNull();
            } else {
              auto *entry = reinterpret_cast<storage::VarlenEntry *>(varlen_pointer2);
              auto status2 UNUSED_ATTRIBUTE =
                  string_builder2.Append(reinterpret_cast<const uint8_t *>(entry->Content()), entry->Size());
            }
          }
          std::shared_ptr<arrow::Array> string_column, string_column2;
//          std::shared_ptr<arrow::Array> int_column, int_column2;
//          auto status3 UNUSED_ATTRIBUTE = int_builder.Finish(&int_column);
//          auto status12 UNUSED_ATTRIBUTE = int_builder2.Finish(&int_column2);
          auto status4 UNUSED_ATTRIBUTE = string_builder.Finish(&string_column);
          auto status5 UNUSED_ATTRIBUTE = string_builder2.Finish(&string_column2);

          std::vector<std::shared_ptr<arrow::Field>> schema_vector{arrow::field("1", arrow::utf8()),
                                                                   arrow::field("2", arrow::utf8())};

          std::vector<std::shared_ptr<arrow::Array>> table_vector{string_column, string_column2};
          volatile std::shared_ptr<arrow::Table> table =
              arrow::Table::Make(std::make_shared<arrow::Schema>(schema_vector), table_vector);
        }
      }
      gc_.PerformGarbageCollection();
      gc_.PerformGarbageCollection();
      for (storage::RawBlock *block : blocks) block_store_.Release(block);
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }
    for (storage::RawBlock *block : start_blocks) block_store_.Release(block);

    state.
        SetItemsProcessed(num_blocks_
                              * static_cast
                                  <int64_t>(state
                                      .
                                          iterations()
                              ));
  }

};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman0)(benchmark::State &state) {
  RunStrawman(state, 0.0);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman001)(benchmark::State &state) {
  RunStrawman(state, 0.01);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman005)(benchmark::State &state) {
  RunStrawman(state, 0.05);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman01)(benchmark::State &state) {
  RunStrawman(state, 0.1);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman02)(benchmark::State &state) {
  RunStrawman(state, 0.2);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman04)(benchmark::State &state) {
  RunStrawman(state, 0.4);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman06)(benchmark::State &state) {
  RunStrawman(state, 0.6);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman08)(benchmark::State &state) {
  RunStrawman(state, 0.8);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction0)(benchmark::State &state) {
  RunCompaction(state, 0.0);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction001)(benchmark::State &state) {
  RunCompaction(state, 0.01);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction005)(benchmark::State &state) {
  RunCompaction(state, 0.05);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction01)(benchmark::State &state) {
  RunCompaction(state, 0.1);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction02)(benchmark::State &state) {
  RunCompaction(state, 0.2);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction04)(benchmark::State &state) {
  RunCompaction(state, 0.4);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction06)(benchmark::State &state) {
  RunCompaction(state, 0.6);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction08)(benchmark::State &state) {
  RunCompaction(state, 0.8);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Throughput0)(benchmark::State &state) {
  RunFull(state, 0.0);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Throughput001)(benchmark::State &state) {
  RunFull(state, 0.01);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Throughput005)(benchmark::State &state) {
  RunFull(state, 0.05);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Throughput01)(benchmark::State &state) {
  RunFull(state, 0.1);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Throughput02)(benchmark::State &state) {
  RunFull(state, 0.2);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Throughput04)(benchmark::State &state) {
  RunFull(state, 0.4);
}
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Throughput06)(benchmark::State &state) {
  RunFull(state, 0.6);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Throughput08)(benchmark::State &state) {
  RunFull(state, 0.8);
}


// NOLINTNEXTLINE
//BENCHMARK_DEFINE_F(BlockCompactorBenchmark, DictionaryCompressionThroughput)(benchmark::State &state) {
//  // NOLINTNEXTLINE
//  for (auto _ : state) {
//    std::vector<storage::RawBlock *> blocks;
//    for (uint32_t i = 0; i < num_blocks_; i++) {
//      storage::RawBlock *block = block_store_.Get();
//      block->data_table_ = &table_;
//      StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, percent_empty_, &generator_);
//      auto &arrow_metadata = accessor_.GetArrowBlockMetadata(block);
//      for (storage::col_id_t col_id : layout_.AllColumns()) {
//        if (layout_.IsVarlen(col_id)) {
//          arrow_metadata.GetColumnInfo(layout_, col_id).Type() = storage::ArrowColumnType::DICTIONARY_COMPRESSED;
//        } else {
//          arrow_metadata.GetColumnInfo(layout_, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
//        }
//      }
//      blocks.push_back(block);
//    }
//    // generate our table and instantiate GC
//    for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
//    compactor_.ProcessCompactionQueue(&txn_manager_);
//    gc_.PerformGarbageCollection();
//    gc_.PerformGarbageCollection();
//    for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
//    uint64_t elapsed_ms;
//    {
//      common::ScopedTimer timer(&elapsed_ms);
//      compactor_.ProcessCompactionQueue(&txn_manager_);
//    }
//    for (storage::RawBlock *block : blocks) block_store_.Release(block);
//    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
//  }
//  state.SetItemsProcessed(static_cast<int64_t>(num_blocks_ * state.iterations()));
//}

//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(2);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman0)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman001)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman005)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman01)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman02)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman04)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman06)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman08)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction0)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2)->Repetitions(10);
//
//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction001)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2)->Repetitions(10);
//
//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction005)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2)->Repetitions(10);
//
//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction01)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2)->Repetitions(10);
//
//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction02)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2)->Repetitions(10);
//
//
//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction04)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2)->Repetitions(10);
//
//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction06)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2)->Repetitions(10);
//
//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction08)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Throughput0)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Throughput001)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Throughput005)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Throughput01)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Throughput02)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Throughput04)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Throughput06)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Throughput08)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(10);

//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, DictionaryCompressionThroughput)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->MinTime(2);


}  // namespace terrier
