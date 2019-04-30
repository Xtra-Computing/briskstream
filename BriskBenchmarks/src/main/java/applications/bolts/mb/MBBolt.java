package applications.bolts.mb;

import applications.param.mb.MicroEvent;
import applications.sink.SINKComBO;
import brisk.components.operators.api.TransactionalBolt;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.runtime.tuple.impl.msgs.GeneralMsg;
import engine.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.SchemaRecord;
import engine.storage.SchemaRecordRef;
import engine.storage.datatype.DataBox;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.*;
import static applications.Constants.DEFAULT_STREAM_ID;
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
                DataBox dataBox = ref.getRecord().getValues().get(1);
                int read_result = Integer.parseInt(dataBox.getString().trim());
                sum += read_result;
            } catch (Exception e) {
                System.out.println("Null Pointer Exception at: " + event.getBid() + "i:" + i);
                System.out.println("What causes the exception?" + event.getKeys()[i]);
            }
        }

        if (enable_speculative) {
            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
            //now we assume it's all correct for testing its upper bond.
            //so nothing is send out.

        } else {
            if (!enable_app_combo) {
                collector.emit(event.getBid(), sum, event.getTimestamp());//the tuple is finished finally.
            } else {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, sum)));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }


    protected void write_core(MicroEvent event) throws InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            List<DataBox> values = event.getValues()[i];
            SchemaRecordRef recordRef = event.getRecord_refs()[i];
            SchemaRecord record = recordRef.getRecord();
            List<DataBox> recordValues = record.getValues();
            recordValues.get(1).setString(values.get(1).getString(), VALUE_LEN);
        }
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true)));//(long bid, int sourceId, TopologyContext context, Message message)
        }
    }

    protected void read_lock_ahead(MicroEvent Event) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txn_context, "MicroTable",
                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_ONLY);
    }


    protected void write_lock_ahead(MicroEvent Event) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txn_context, "MicroTable",
                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_WRITE);
    }

    private boolean process_request_noLock(MicroEvent event, MetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord_noLock(txn_context, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean process_request(MicroEvent event, MetaTypes.AccessType accessType) throws DatabaseException, InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord(txn_context, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    protected boolean read_request(MicroEvent event) throws DatabaseException {

        if (process_request_noLock(event, READ_ONLY)) return false;
        return true;
    }


    protected boolean write_request(MicroEvent event) throws DatabaseException {

        if (process_request_noLock(event, READ_WRITE)) return false;
        return true;
    }

    protected boolean read_request_lock(MicroEvent event) throws DatabaseException, InterruptedException {

        if (process_request(event, READ_ONLY)) return false;
        return true;
    }


    protected boolean write_request_lock(MicroEvent event) throws DatabaseException, InterruptedException {

        if (process_request(event, READ_WRITE)) return false;
        return true;
    }



    protected abstract void write_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException;

    protected abstract void read_handle(MicroEvent event, Long timestamp) throws InterruptedException, DatabaseException;
}
