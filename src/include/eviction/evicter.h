#include "ltm/resource_tracker.h"
#include "storage/data_table.h"
namespace peloton {
namespace storage{
class DataTable;
}
namespace eviction {
class Evicter {
public:
    void EvictDataFromTable (storage::DataTable* table);
private:
    void EvictTileGroup (std::shared_ptr<storage::TileGroup> *tg);
};

}
}
