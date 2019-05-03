package engine.index;


import engine.storage.TableRecord;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class HashTableIndex extends BaseUnorderedIndex {


    private ConcurrentHashMap<String, TableRecord> hash_index_ = new ConcurrentHashMap<>();

    @Override
    public TableRecord SearchRecord(String primary_key) {
        return hash_index_.get(primary_key);
    }

    @Override
    public boolean InsertRecord(String key, TableRecord record) {
        hash_index_.put(key, record);
        return true;
    }

    @Override
    public Iterator<TableRecord> iterator() {
        return hash_index_.values().iterator();
    }
}
