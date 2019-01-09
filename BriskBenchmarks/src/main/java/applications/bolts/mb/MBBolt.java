package applications.bolts.mb;

import applications.param.MicroEvent;
import brisk.components.operators.api.TransactionalBolt;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.SchemaRecord;
import engine.storage.SchemaRecordRef;
import engine.storage.datatype.DataBox;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.enable_latency_measurement;
import static applications.CONTROL.enable_speculative;
import static applications.constants.MicroBenchmarkConstants.Constant.VALUE_LEN;
import static engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.BEGIN_PREPARE_TIME_MEASURE;
import static engine.profiler.Metrics.MeasureTools.END_PREPARE_TIME_MEASURE;

public abstract class MBBolt extends TransactionalBolt {
    public MBBolt(Logger log, int fid) {
        super(log, fid);
    }

    protected void read_core(MicroEvent event) throws InterruptedException {

        int sum = 0;
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];

            try {
                DataBox dataBox = ref.record.getValues().get(1);
                int read_result = Integer.parseInt(dataBox.getString().trim());
                sum += read_result;
            } catch (Exception e) {
                System.out.println("Null Pointer Exception at: " + event.getBid() + "i:" + i + " ref cnt:" + ref.cnt);
                System.out.println("What causes the exception?" + ref.record);
                System.out.println("What causes the exception?" + ref.record.getValues());
                System.out.println("What causes the exception?" + ref.record.getValues().get(1));
            }
        }

        if (enable_speculative) {
            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
            //now we assume it's all correct for testing its upper bond.
            //so nothing is send out.

        } else
            collector.force_emit(event.getBid(), sum, event.getTimestamp());//the tuple is finished.
    }


    protected void write_core(MicroEvent event) throws InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            List<DataBox> values = event.getValues()[i];
            SchemaRecordRef recordRef = event.getRecord_refs()[i];
            SchemaRecord record = recordRef.record;
            List<DataBox> recordValues = record.getValues();
            recordValues.get(1).setString(values.get(1).getString(), VALUE_LEN);
        }
        collector.force_emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
    }

    protected void read_lock_ahead(MicroEvent Event, long bid) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txn_context, "MicroTable",
                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_ONLY);
    }


    protected void write_lock_ahead(MicroEvent Event, long bid) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txn_context, "MicroTable",
                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_WRITE);
    }

    private boolean process_request_noLock(MicroEvent event, MetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord_noLock(txn_context, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].record != null;
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean process_request(MicroEvent event, MetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord(txn_context, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].record != null;
            } else {
                return true;
            }
        }
        return false;
    }

    protected boolean read_request(MicroEvent event, long bid) throws DatabaseException {

        if (process_request_noLock(event, READ_ONLY)) return false;
        return true;
    }


    protected boolean write_request(MicroEvent event, long bid) throws DatabaseException {

        if (process_request_noLock(event, READ_WRITE)) return false;
        return true;
    }

    protected boolean read_request_lock(MicroEvent event, long bid) throws DatabaseException {

        if (process_request(event, READ_ONLY)) return false;
        return true;
    }


    protected boolean write_request_lock(MicroEvent event, long bid) throws DatabaseException {

        if (process_request(event, READ_WRITE)) return false;
        return true;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);

        long bid = in.getBID();

        MicroEvent event = (MicroEvent) db.eventManager.get((int) bid);

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

    protected abstract void write_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException;

    protected abstract void read_handle(MicroEvent event, Long timestamp) throws InterruptedException, DatabaseException;
}
