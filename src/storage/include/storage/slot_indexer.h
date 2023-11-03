#ifndef __SLOT_INDEXER_H__
#define __SLOT_INDEXER_H__
#include <vector>

namespace storage {
class SlotIndexer {
public:
  SlotIndexer(uint32_t inst_num) : inst_num_(inst_num) {}
  ~SlotIndexer() {}
  uint32_t GetInstanceID(int32_t slot_id) {return 0; }
  void ReshardSlots(const std::vector<uint32_t>& slots) {}

private:
  uint32_t inst_num_;
};
} // namespace storage end

#endif
