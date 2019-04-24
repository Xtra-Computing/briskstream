package applications.bolts.lr.txn;

import applications.param.lr.LREvent;
import brisk.components.operators.api.TransactionalBolt;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import engine.Meta.MetaTypes;
import org.slf4j.Logger;

import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.enable_latency_measurement;
import static engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.BEGIN_PREPARE_TIME_MEASURE;
import static engine.profiler.Metrics.MeasureTools.END_PREPARE_TIME_MEASURE;

public abstract class TPBolt extends TransactionalBolt {
    public TPBolt(Logger log, int fid) {
        super(log, fid);
    }

    protected void read_core(LREvent event) throws InterruptedException {
//
//        int sum = 0;
//        for (int i = 0; i < NUM_ACCESSES; ++i) {
//            SchemaRecordRef ref = event.getRecord_refs()[i];
//            try {
//                DataBox dataBox = ref.getRecord().getValues().get(1);
//                int read_result = Integer.parseInt(dataBox.getString().trim());
//                sum += read_result;
//            } catch (Exception e) {
//                System.out.println("Null Pointer Exception at: " + event.getBid() + "i:" + i);
//                System.out.println("What causes the exception?" + event.getKeys()[i]);
//            }
//        }
//
//        if (enable_speculative) {
//            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
//            //now we assume it's all correct for testing its upper bond.
//            //so nothing is send out.
//        } else
//            collector.force_emit(event.getBid(), sum, event.getTimestamp());//the tuple is finished finally.
    }


    protected void write_core(LREvent event) throws InterruptedException {
//        for (int i = 0; i < NUM_ACCESSES; ++i) {
//            List<DataBox> values = event.getValues()[i];
//            SchemaRecordRef recordRef = event.getRecord_refs()[i];
//            SchemaRecord record = recordRef.getRecord();
//            List<DataBox> recordValues = record.getValues();
//            recordValues.get(1).setString(values.get(1).getString(), VALUE_LEN);
//        }
//        collector.force_emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
    }

    protected void read_lock_ahead(LREvent Event, long bid) throws DatabaseException {
//        for (int i = 0; i < NUM_ACCESSES; ++i)
//            transactionManager.lock_ahead(txn_context, "MicroTable",
//                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_ONLY);
    }


    protected void write_lock_ahead(LREvent Event, long bid) throws DatabaseException {
//        for (int i = 0; i < NUM_ACCESSES; ++i)
//            transactionManager.lock_ahead(txn_context, "MicroTable",
//                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_WRITE);
    }

    private boolean process_request_noLock(LREvent event, MetaTypes.AccessType accessType) throws DatabaseException {
//        for (int i = 0; i < NUM_ACCESSES; ++i) {
//            boolean rt = transactionManager.SelectKeyRecord_noLock(txn_context, "MicroTable",
//                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
//            if (rt) {
//                assert event.getRecord_refs()[i].getRecord() != null;
//            } else {
//                return true;
//            }
//        }
        return false;
    }

    private boolean process_request(LREvent event, MetaTypes.AccessType accessType) throws DatabaseException {
//        for (int i = 0; i < NUM_ACCESSES; ++i) {
//            boolean rt = transactionManager.SelectKeyRecord(txn_context, "MicroTable",
//                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
//            if (rt) {
//                assert event.getRecord_refs()[i].getRecord() != null;
//            } else {
//                return true;
//            }
//        }
        return false;
    }

    protected boolean read_request(LREvent event) throws DatabaseException {

        if (process_request_noLock(event, READ_ONLY)) return false;
        return true;
    }


    protected boolean write_request(LREvent event) throws DatabaseException {

        if (process_request_noLock(event, READ_WRITE)) return false;
        return true;
    }

    protected boolean read_request_lock(LREvent event) throws DatabaseException {

        if (process_request(event, READ_ONLY)) return false;
        return true;
    }


    protected boolean write_request_lock(LREvent event) throws DatabaseException {

        if (process_request(event, READ_WRITE)) return false;
        return true;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);

        long bid = in.getBID();

        LREvent event = (LREvent) db.eventManager.get((int) bid);

        Long timestamp;//in.getLong(1);

        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        boolean flag = event.READ_EVENT();

        (event).setTimestamp(timestamp);
        END_PREPARE_TIME_MEASURE(thread_Id);

        if (flag) {
            read_handle(event, timestamp);
        } else {
            write_handle(event, timestamp);
        }
    }

    protected abstract void write_handle(LREvent event, Long timestamp) throws DatabaseException, InterruptedException;

    protected abstract void read_handle(LREvent event, Long timestamp) throws InterruptedException, DatabaseException;
}
