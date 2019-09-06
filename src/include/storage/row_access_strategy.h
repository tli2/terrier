#include <utility>
#include <vector>
#include "common/container/concurrent_bitmap.h"
#include "common/macros.h"
#include "storage/arrow_block_metadata.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"

namespace terrier::storage {
class DataTable;

class RowAccessStrategy {
  /* Block Header layout:
   * -----------------------------------------------------------------------------------------------------------------
   * | data_table *(64) | padding (16) | layout_version (16) | insert_head (32) |        control_block (64)          |
   * -----------------------------------------------------------------------------------------------------------------
   * | bitmap for slots | projected rows |
   */
  struct Block {
    MEM_REINTERPRETATION_ONLY(Block)

    common::RawConcurrentBitmap *SlotAllocationBitmap(const BlockLayout &layout) {
      return reinterpret_cast<common::RawConcurrentBitmap *>(block_.content_);
    }

    RawBlock block_;
  };
};
} // namespace terrier::storage