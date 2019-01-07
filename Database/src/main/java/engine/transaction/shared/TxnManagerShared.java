package engine.transaction.shared;

import engine.Database;
import engine.Meta.MetaTypes;
import engine.common.OrderLock;
import engine.storage.SchemaRecord;
import engine.storage.SchemaRecordRef;
import engine.storage.SchemaRecords;
import engine.storage.datatype.DataBox;
import engine.transaction.TxnManager;
import engine.transaction.impl.TxnContext;

import java.util.LinkedList;
import java.util.List;

public abstract class TxnManagerShared implements TxnManager {

    public OrderLock getOrderLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, double[] enqueue_time) {
        return false;
    }

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String key, List<DataBox> value, double[] enqueue_time) {
        return false;
    }

    @Override
    public void start_evaluate(int taskId, int fid, long bid) {

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) {
        return false;
    }

    @Override
    public boolean CommitTransaction(TxnContext txn_context) {
        return false;
    }

    @Override
    public boolean SelectKeyRecord(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) {
        return false;
    }

    @Override
    public boolean SelectRecords(Database db, TxnContext txn_context, String table_name, int i, String secondary_key, SchemaRecords records, MetaTypes.AccessType accessType, LinkedList<Long> gap) {
        return false;
    }
}
