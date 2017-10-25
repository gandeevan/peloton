#include "resource_tracking/resource_tracker.h"
#include "storage/data_table.h"
namespace peloton {
namespace storage{
class DataTable;
}
namespace eviction {

    void EvictDataFromTable (storage::DataTable* table);

    void EvictTileGroup (std::shared_ptr<storage::TileGroup> tg);


}
}
