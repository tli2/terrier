#include "storage/block_compactor.h"
#include <vector>
#include "arrow/api.h"
#include "storage/block_access_controller.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier {
struct BlockCompactorTest : public ::terrier::TerrierTest {
  storage::BlockStore block_store_{5000, 5000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  storage::BlockLayout layout_{{8, 8, VARLEN_COLUMN}};
  storage::TupleAccessStrategy accessor_{layout_};

  storage::DataTable table_{&block_store_, layout_, storage::layout_version_t(0)};
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};
  storage::GarbageCollector gc_{&txn_manager_};
  storage::BlockCompactor compactor_;

  uint32_t num_blocks_ = 500;
  double percent_empty_ = 0.01;

  uint32_t CalculateOptimal(std::vector<storage::RawBlock *> blocks) {
    std::unordered_map<storage::RawBlock *, uint32_t> num_tuples;
    uint32_t total_num_tuples = 0;
    const storage::TupleAccessStrategy &accessor = table_.accessor_;
    const storage::BlockLayout &layout = accessor.GetBlockLayout();
    for (storage::RawBlock *block : blocks) {
      uint32_t &count = num_tuples[block];
      auto *bitmap = accessor.AllocationBitmap(block);
      for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
        if (bitmap->Test(offset)) {
          count++;
          total_num_tuples++;
        }
      }
    }

    std::sort(blocks.begin(), blocks.end(), [&](storage::RawBlock *a, storage::RawBlock *b) {
      TERRIER_ASSERT(num_tuples.find(a) != num_tuples.end() && num_tuples.find(b) != num_tuples.end(), "WTF");
      auto a_filled = num_tuples[a];
      auto b_filled = num_tuples[b];
      // We know these finds will not return end() because we constructed the vector from the map
      return a_filled > b_filled;
    });

    uint32_t f_size = total_num_tuples / layout.NumSlots();
    uint32_t p_size = total_num_tuples % layout.NumSlots();
    uint32_t min_movement = UINT32_MAX;
    for (storage::RawBlock *p : blocks) {
      uint32_t num_movements = 0;
      auto *bitmap = accessor.AllocationBitmap(p);
      for (uint32_t offset = 0; offset < p_size; offset++) {
        if (!bitmap->Test(offset))
          num_movements++;
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

  uint32_t NumEmpty(const std::vector<storage::RawBlock *> &blocks) {
    uint32_t result = 0;
    const storage::TupleAccessStrategy &accessor = table_.accessor_;
    const storage::BlockLayout &layout = accessor.GetBlockLayout();
    for (storage::RawBlock *block : blocks) {
      bool empty = true;
      auto *bitmap = accessor.AllocationBitmap(block);
      for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
        if (bitmap->Test(offset)) {
          empty = false;
          break;
        }
      }
      if (empty) result++;
    }
    return result;
  }

  void RunFull(double percent_empty,
               storage::ArrowColumnType type = storage::ArrowColumnType::GATHERED_VARLEN) {
    uint32_t num_tuples = 0;
    // NOLINTNEXTLINE
    compactor_.tuples_moved_ = 0;
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
    printf("blocks generated\n");
//    uint32_t optimal_move = CalculateOptimal(blocks);
    for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
    compactor_.ProcessCompactionQueue(&txn_manager_);
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();


    for (storage::RawBlock *block : blocks) block_store_.Release(block);

    printf("With %f percent empty,"
           "%u tuples in total,"
           "actually moved %u,"
           "%u blocks can be freed\n",
           percent_empty,
           num_tuples,
           compactor_.tuples_moved_,
           NumEmpty(blocks));
  }
};

// NOLINTNEXTLINE
TEST_F(BlockCompactorTest, SingleBlockCompactionTest) {
//  RunFull(0);
  RunFull(0.01);
  RunFull(0.05);
  RunFull(0.1);
  RunFull(0.2);
  RunFull(0.4);
  RunFull(0.6);
  RunFull(0.8);
}

// NOLINTNEXTLINE
// TEST_F(BlockCompactorTest, SingleBlockDictionaryTest) {
//  std::default_random_engine generator;
//  storage::BlockStore block_store{1, 1};
//  storage::RawBlock *block = block_store.Get();
//  storage::BlockLayout layout({8, 8, VARLEN_COLUMN});
//  storage::TupleAccessStrategy accessor(layout);
//  accessor.InitializeRawBlock(block, storage::layout_version_t(0));
//
//  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
//  storage::DataTable table(&block_store, layout, storage::layout_version_t(0));
//  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
//  // Enable GC to cleanup transactions started by the block compactor
//  transaction::TransactionManager txn_manager(&buffer_pool, true, LOGGING_DISABLED);
//  storage::GarbageCollector gc(&txn_manager);
//
//  auto tuples = StorageTestUtil::PopulateBlockRandomly(layout, block, 0.1, &generator);
//  auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);
//  for (storage::col_id_t col_id : layout.AllColumns()) {
//    if (layout.IsVarlen(col_id)) {
//      arrow_metadata.GetColumnInfo(layout, col_id).type_ = storage::ArrowColumnType::GATHERED_VARLEN;
//    } else {
//      arrow_metadata.GetColumnInfo(layout, col_id).type_ = storage::ArrowColumnType::FIXED_LENGTH;
//    }
//  }
//
//  storage::BlockCompactor compactor;
//  compactor.PutInQueue({block, &table});
//  compactor.ProcessCompactionQueue(&txn_manager);  // should always succeed with no other threads
//
//  storage::ProjectedRowInitializer initializer(layout, StorageTestUtil::ProjectionListAllColumns(layout));
//  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
//  auto *read_row = initializer.InitializeRow(buffer);
//  std::vector<storage::ProjectedRow *> moved_rows;
//  // This transaction is guaranteed to start after the compacting one commits
//  transaction::TransactionContext *txn = txn_manager.BeginTransaction();
//  auto num_tuples = tuples.size();
//  for (uint32_t i = 0; i < layout.NumSlots(); i++) {
//    storage::TupleSlot slot(block, i);
//    bool visible = table.Select(txn, slot, read_row);
//    if (i >= num_tuples) {
//      EXPECT_FALSE(visible);  // Should be deleted after compaction
//    } else {
//      EXPECT_TRUE(visible);  // Should be filled after compaction
//      auto it = tuples.find(slot);
//      if (it != tuples.end()) {
//        // Here we can assume that the row is not moved. Check that everything is still equal. Has to be deep
//        // equality because varlens are moved.
//        EXPECT_TRUE(StorageTestUtil::ProjectionListEqualDeep(layout, it->second, read_row));
//        delete[] reinterpret_cast<byte *>(tuples[slot]);
//        tuples.erase(slot);
//      } else {
//        // Need to copy and do quadratic comparison later.
//        byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
//        std::memcpy(buffer, read_row, initializer.ProjectedRowSize());
//        moved_rows.push_back(reinterpret_cast<storage::ProjectedRow *>(buffer));
//      }
//    }
//  }
//  txn_manager.Commit(txn, [](void *) -> void {}, nullptr);  // Commit: will be cleaned up by GC
//  delete[] buffer;
//
//  for (auto *moved_row : moved_rows) {
//    bool match_found = false;
//    for (auto &entry : tuples) {
//      // This comparison needs to be deep because varlens are moved.
//      if (StorageTestUtil::ProjectionListEqualDeep(layout, entry.second, moved_row)) {
//        // Here we can assume that the row is not moved. All good.
//        delete[] reinterpret_cast<byte *>(entry.second);
//        tuples.erase(entry.first);
//        match_found = true;
//        break;
//      }
//    }
//    // the read tuple should be one of the original tuples that are moved.
//    EXPECT_TRUE(match_found);
//    delete[] reinterpret_cast<byte *>(moved_row);
//  }
//  // All tuples from the original block should have been accounted for.
//  EXPECT_TRUE(tuples.empty());
//  gc.PerformGarbageCollection();
//  gc.PerformGarbageCollection();  // Second call to deallocate.
//  // Deallocated arrow buffers
//  for (const auto &col_id : layout.AllColumns()) {
//    arrow_metadata.Deallocate(layout, col_id);
//  }
//  delete block;
//}

}  // namespace terrier
