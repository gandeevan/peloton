#include "eviction/evicter.h"
#include "type/serializeio.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "util/file_util.h"
#include "storage/tile.h"
#include "util/output_buffer.h"

namespace peloton {
namespace eviction{
    void Evicter::EvictDataFromTable (storage::DataTable* table){
        for(uint offset = 0; offset < table->GetTileGroupCount(); offset++){
            auto tg = table->GetTileGroup(offset);
            if(tg->GetHeader()->IsEvictable()){
                EvictTileGroup(&tg);
                //table->DeleteTileGroup(offset);
                tg.reset();
            }
            }
        }

    void Evicter::EvictTileGroup(std::shared_ptr<storage::TileGroup> *tg){
        CopySerializeOutput output;
        FileHandle f;
        OutputBuffer* bf = new OutputBuffer();
        for(uint offset = 0; offset < (*tg)->GetTileCount(); offset++){
            auto tile = (*tg)->GetTile(offset);
            tile->SerializeTo(output, (*tg)->GetActiveTupleCount());
        }
        //FileUtil::OpenFile("/tmp/log/evict_tg", "wb", f);
        bf->WriteData(output.Data(), output.Size());


          //  fwrite((const void *) (bf->GetData()), bf->GetSize(), 1, f.file);

        //  Call fsync
            //FileUtil::FFlushFsync(f);
            //FileUtil::CloseFile(f);
            delete bf;
        }


}
}
