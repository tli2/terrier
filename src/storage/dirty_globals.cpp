#include "storage/dirty_globals.h"
#include <cstdint>
namespace terrier::storage {
tpcc::Database *DirtyGlobals::tpcc_db = nullptr;
uint8_t o_id_insert_pr_offset = 0;
uint8_t o_d_id_insert_pr_offset = 0;
uint8_t o_w_id_insert_pr_offset = 0;
uint8_t o_c_id_insert_pr_offset = 0;
uint8_t o_entry_d_insert_pr_offset = 0;
uint8_t o_carrier_id_insert_pr_offset = 0;
uint8_t o_ol_cnt_insert_pr_offset = 0;
uint8_t o_all_local_insert_pr_offset = 0;

uint8_t o_id_key_pr_offset = 0;
uint8_t o_d_id_key_pr_offset = 0;
uint8_t o_w_id_key_pr_offset = 0;
uint8_t o_id_secondary_key_pr_offset = 0;
uint8_t o_d_id_secondary_key_pr_offset = 0;
uint8_t o_w_id_secondary_key_pr_offset = 0;
uint8_t o_c_id_secondary_key_pr_offset = 0;

uint8_t ol_o_id_insert_pr_offset = 0;
uint8_t ol_d_id_insert_pr_offset = 0;
uint8_t ol_w_id_insert_pr_offset = 0;
uint8_t ol_number_insert_pr_offset = 0;
uint8_t ol_i_id_insert_pr_offset = 0;
uint8_t ol_supply_w_id_insert_pr_offset = 0;
uint8_t ol_delivery_d_insert_pr_offset = 0;
uint8_t ol_quantity_insert_pr_offset = 0;
uint8_t ol_amount_insert_pr_offset = 0;
uint8_t ol_dist_info_insert_pr_offset = 0;
uint8_t ol_o_id_key_pr_offset = 0;
uint8_t ol_d_id_key_pr_offset = 0;
uint8_t ol_w_id_key_pr_offset = 0;
uint8_t ol_number_key_pr_offset = 0;
}  // namespace terrier::storage

