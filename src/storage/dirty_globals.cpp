#include "storage/dirty_globals.h"
#include <cstdint>
namespace terrier::storage {
tpcc::Database *DirtyGlobals::tpcc_db = nullptr;
uint8_t DirtyGlobals::o_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::o_d_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::o_w_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::o_c_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::o_entry_d_insert_pr_offset = 0;
uint8_t DirtyGlobals::o_carrier_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::o_ol_cnt_insert_pr_offset = 0;
uint8_t DirtyGlobals::o_all_local_insert_pr_offset = 0;

uint8_t DirtyGlobals::o_id_key_pr_offset = 0;
uint8_t DirtyGlobals::o_d_id_key_pr_offset = 0;
uint8_t DirtyGlobals::o_w_id_key_pr_offset = 0;
uint8_t DirtyGlobals::o_id_secondary_key_pr_offset = 0;
uint8_t DirtyGlobals::o_d_id_secondary_key_pr_offset = 0;
uint8_t DirtyGlobals::o_w_id_secondary_key_pr_offset = 0;
uint8_t DirtyGlobals::o_c_id_secondary_key_pr_offset = 0;

uint8_t DirtyGlobals::ol_o_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_d_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_w_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_number_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_i_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_supply_w_id_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_delivery_d_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_quantity_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_amount_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_dist_info_insert_pr_offset = 0;
uint8_t DirtyGlobals::ol_o_id_key_pr_offset = 0;
uint8_t DirtyGlobals::ol_d_id_key_pr_offset = 0;
uint8_t DirtyGlobals::ol_w_id_key_pr_offset = 0;
uint8_t DirtyGlobals::ol_number_key_pr_offset = 0;
std::atomic<uint32_t> DirtyGlobals::blocked_transactions = 0;
std::atomic<uint32_t> DirtyGlobals::aborted_transactions = 0;
}  // namespace terrier::storage

