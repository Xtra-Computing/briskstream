package applications.bolts.transactional.ss;

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
import static applications.constants.GrepSumConstants.Constant.VALUE_LEN;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static engine.profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class SSBolt extends TransactionalBolt {

    public SSBolt(Logger log, int fid) {
        super(log, fid);
        this.configPrefix = "ss";
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {

    }

    protected boolean READ_CORE(MicroEvent event) {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];

            if (ref.isEmpty())
                return false;//not yet processed.

            DataBox dataBox = ref.getRecord().getValues().get(1);
            int read_result = Integer.parseInt(dataBox.getString().trim());
            event.result[i] = read_result;
        }
        return true;
    }

//    volatile int com_result = 0;

    protected void READ_POST(MicroEvent event) throws InterruptedException {
        int sum = 0;
        if (POST_COMPUTE_COMPLEXITY != 0) {
            for (int i = 0; i < NUM_ACCESSES; ++i) {
                sum += event.result[i];
            }
            for (int j = 0; j < POST_COMPUTE_COMPLEXITY; ++j)
                sum += System.nanoTime();

        }
//        com_result = sum;

        if (enable_speculative) {
            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
            //now we assume it's all correct for testing its upper bond.
            //so nothing is send out.

        } else {
            if (!enable_app_combo) {
                collector.emit(event.getBid(), sum, event.getTimestamp());//the tuple is finished finally.
            } else {
                if (enable_latency_measurement) {
                    sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, sum, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
                }
            }
        }
        sum = 0;
    }

    protected void WRITE_POST(MicroEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {

            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }


    protected void WRITE_CORE(MicroEvent event) {



        List<DataBox> values = event.getValues()[0];

        //Insert into S1 VALUES(....)
        SchemaRecordRef recordRef1 = event.getRecord_refs()[0];
        SchemaRecord record1 = recordRef1.getRecord();
        List<DataBox> recordValues1 = record1.getValues();
        recordValues1.get(1).setString(values.get(1).getString(), VALUE_LEN);

        //Insert into S2 SELECT * FROM S1;
        SchemaRecordRef recordRef2 = event.getRecord_refs()[1];
        SchemaRecord record2 = recordRef2.getRecord();
        List<DataBox> recordValues2 = record2.getValues();
        //using S1's value to update S2.
        recordValues2.get(1).setString(recordValues1.get(1).getString(), VALUE_LEN);

        //Insert into S3 SELECT * FROM S2;
        SchemaRecordRef recordRef3 = event.getRecord_refs()[1];
        SchemaRecord record3 = recordRef3.getRecord();
        List<DataBox> recordValues3 = record3.getValues();
        //using S2's value to update S3.
        recordValues3.get(1).setString(recordValues2.get(1).getString(), VALUE_LEN);

    }


    protected void WRITE_LOCK_AHEAD(MicroEvent Event, TxnContext txnContext) throws DatabaseException {
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


    protected boolean write_request_noLock(MicroEvent event, TxnContext txnContext) throws DatabaseException {

        return !process_request_noLock(event, txnContext, READ_WRITE);
    }


    //lock_ratio-ahead phase.
    protected void LAL_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        //ONLY USED BY LAL, LWM, and PAT.
    }


    //post stream processing phase..
    protected void POST_PROCESS(long _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            MicroEvent event = (MicroEvent) input_event;
            (event).setTimestamp(timestamp);
            boolean flag = event.READ_EVENT();
            if (flag) {//read
                READ_POST(event);
            } else {
                WRITE_POST(event);
            }
        }
        END_POST_TIME_MEASURE(thread_Id);
    }


}
