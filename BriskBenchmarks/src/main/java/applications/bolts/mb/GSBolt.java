package applications.bolts.mb;

import applications.param.mb.MicroEvent;
import brisk.components.operators.api.TransactionalBolt;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.runtime.tuple.impl.msgs.GeneralMsg;
import engine.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.SchemaRecord;
import engine.storage.SchemaRecordRef;
import engine.storage.datatype.DataBox;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import java.util.List;

import static applications.CONTROL.*;
import static applications.Constants.DEFAULT_STREAM_ID;
import static applications.constants.MicroBenchmarkConstants.Constant.VALUE_LEN;
import static engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.*;
import static engine.profiler.Metrics.MeasureTools.END_LOCK_TIME_MEASURE_ACC;
import static engine.profiler.Metrics.MeasureTools.END_WAIT_TIME_MEASURE_ACC;

public abstract class GSBolt extends TransactionalBolt {

    public GSBolt(Logger log, int fid) {
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
        event.sum = sum;

    }

    protected void read_post(MicroEvent event) throws InterruptedException {
        if (enable_speculative) {
            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
            //now we assume it's all correct for testing its upper bond.
            //so nothing is send out.

        } else {
            if (!enable_app_combo) {
                collector.emit(event.getBid(), event.sum, event.getTimestamp());//the tuple is finished finally.
            } else {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.sum)));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }

    }
    protected void write_post(MicroEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true)));//(long bid, int sourceId, TopologyContext context, Message message)
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
    }



    protected void read_lock_ahead(MicroEvent Event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_ONLY);
    }


    protected void write_lock_ahead(MicroEvent Event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_WRITE);
    }

    private boolean process_request_noLock(MicroEvent event, TxnContext txnContext, MetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean process_request(MicroEvent event, TxnContext txnContext, MetaTypes.AccessType accessType) throws DatabaseException, InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    protected boolean read_request_noLock(MicroEvent event, TxnContext txnContext) throws DatabaseException {

        if (process_request_noLock(event, txnContext, READ_ONLY)) return false;
        return true;
    }


    protected boolean write_request_noLock(MicroEvent event, TxnContext txnContext) throws DatabaseException {

        if (process_request_noLock(event, txnContext, READ_WRITE)) return false;
        return true;
    }

    protected boolean read_request(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        if (process_request(event, txnContext, READ_ONLY)) return false;
        return true;
    }


    protected boolean write_request(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        if (process_request(event, txnContext, READ_WRITE)) return false;
        return true;
    }

    public transient TxnContext[] txn_context = new TxnContext[combo_bid_size];

    //lock-ahead phase.
    protected void LAL_process(long _bid) throws DatabaseException, InterruptedException {

        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            txn_context[(int) (i - _bid)] = new TxnContext(thread_Id, this.fid, i);

            MicroEvent event = (MicroEvent) db.eventManager.get((int) i);

            BEGIN_WAIT_TIME_MEASURE(thread_Id);
            //ensures that locks are added in the event sequence order.
            transactionManager.getOrderLock().blocking_wait(i);

            BEGIN_LOCK_TIME_MEASURE(thread_Id);
            boolean flag = event.READ_EVENT();
            if (flag) {//read
                read_lock_ahead(event, txn_context[(int) (i - _bid)]);
            } else {
                write_lock_ahead(event, txn_context[(int) (i - _bid)]);
            }

            END_LOCK_TIME_MEASURE_ACC(thread_Id);
            transactionManager.getOrderLock().advance();
            END_WAIT_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void PostLAL_process(long _bid) throws DatabaseException, InterruptedException {
        //txn process phase.
        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            MicroEvent event = (MicroEvent) db.eventManager.get((int) i);

            boolean flag = event.READ_EVENT();

            if (flag) {//read

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                read_request_noLock(event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                read_core(event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            } else {

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                write_request_noLock(event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                write_core(event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            }
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }

    //post stream processing phase..
    protected void post_process(long _bid, long timestamp) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            MicroEvent event = (MicroEvent) db.eventManager.get((int) i);
            (event).setTimestamp(timestamp);
            boolean flag = event.READ_EVENT();
            if (flag) {//read
                read_post(event);
            } else {
                write_post(event);
            }
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

}
