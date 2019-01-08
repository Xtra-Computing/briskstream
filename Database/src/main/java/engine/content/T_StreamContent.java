package engine.content;

import engine.common.OrderLock;
import engine.storage.SchemaRecord;
import engine.storage.datatype.DataBox;
import engine.transaction.impl.TxnContext;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static applications.CONTROL.enable_debug;
import static applications.CONTROL.enable_mvcc;

public abstract class T_StreamContent implements Content {
    public final static String T_STREAMCONTENT = "T_STREAMCONTENT";
    public TreeMap<Long, SchemaRecord> versions = new TreeMap<>();//TODO: In fact... there can be at most only one write to the d_record concurrently. It is safe to just use sorted hashmap.
    public SchemaRecord record;


    @Override
    public boolean TryReadLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryWriteLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void SetTimestamp(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long GetTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ReleaseReadLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ReleaseWriteLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryWriteLock(OrderLock lock, TxnContext txn_context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryReadLock(OrderLock lock, TxnContext txn_context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean AcquireReadLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean AcquireWriteLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean RequestWriteAccess(long timestamp, List<DataBox> data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean RequestReadAccess(long timestamp, List<DataBox> data, boolean[] is_ready) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void RequestCommit(long timestamp, boolean[] is_ready) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void RequestAbort(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long GetLWM() {
        throw new UnsupportedOperationException();
    }

    /**
     * For now, it's ok to simply use single-version. As there's no application requires resolving cross-records dependency.
     *
     * @param ts
     * @return
     */
    @Override
    public SchemaRecord readValues(long ts) {
//        if (record == null)
//            System.nanoTime();
//        if (record.getValues() == null)
//            System.nanoTime();
//
//        return record.getValues() == null ? versions.lastEntry().getValue() : record;
//        return this.record;

//        SchemaRecord value_list = versions.lastEntry().getValue();
//        if (value_list == null || value_list.getValues() == null)
//            System.out.println("version is empty:" + versions.toString());
//        return value_list;//for a record, it cannot be written out-of-order, it's safe to simply return the newest value_list.

        if (enable_mvcc) {
            if (enable_debug)
                if (versions.get(ts) == null && versions.lowerEntry(ts) == null) {
                    System.out.println("Store wrong!");
                    System.exit(-1);
                }

            SchemaRecord record_at_ts;
            Map.Entry<Long, SchemaRecord> entry = versions.lowerEntry(ts);//always get the original (previous) version.

            if (entry != null) {
                record_at_ts = entry.getValue();

            } else
                record_at_ts = versions.get(ts);//not modified in last round

            return record_at_ts;
        } else
            return record;
    }

    @Override
    public void updateValues(long ts, SchemaRecord record) {

        if (enable_mvcc)
            versions.putIfAbsent(ts, record);
        else
            this.record = record;
    }

    @Override
    public boolean AcquireCertifyLock() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void ReleaseCertifyLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void AddLWM(long ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void DeleteLWM(long ts) {
        throw new UnsupportedOperationException();
    }


    //used in SStore
    @Override
    public boolean TryLockPartitions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void LockPartitions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void UnlockPartitions() {
        throw new UnsupportedOperationException();
    }


}
