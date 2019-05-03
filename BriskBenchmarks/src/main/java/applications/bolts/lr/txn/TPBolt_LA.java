package applications.bolts.lr.txn;

import applications.param.lr.LREvent;
import engine.DatabaseException;
import engine.storage.datatype.DataBox;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import java.util.HashSet;

import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.*;

public abstract class TPBolt_LA extends TPBolt {

    public TPBolt_LA(Logger log, int fid) {
        super(log, fid);
    }


    @Override
    protected void write_handle(LREvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(event.getBid());//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        write_request_LA(event);
        long lock_time_measure = END_LOCK_TIME_MEASURE_ACC(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);

        BEGIN_TP_TIME_MEASURE(thread_Id);
        write_request(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        read_core(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);

    }

    @Override
    protected void read_core(LREvent event) throws InterruptedException {
        Integer vid = event.getVSreport().getVid();

        DataBox speed_value_box = event.speed_value.getRecord().getValues().get(1);
        DataBox cnt_value_box = event.count_value.getRecord().getValues().get(1);

        HashSet cnt_segment = cnt_value_box.getHashSet();
        double latestAvgSpeeds = speed_value_box.getDouble();

        cnt_segment.add(vid);//GetAndUpdate hashset; updated state also. TODO: be careful of this.

        int count = cnt_segment.size();

        double lav;
        if (latestAvgSpeeds == 0) {//not initialized
            lav = event.getVSreport().getAvgSpeed();
        } else
            lav = (latestAvgSpeeds + event.getVSreport().getAvgSpeed()) / 2;

        speed_value_box.setDouble(lav);//write to state.

        toll_process(event.getBid(), vid, count, lav, event.getPOSReport().getTime());
    }


    protected void write_request_LA(LREvent event) throws DatabaseException {

        transactionManager.lock_ahead(txn_context, "segment_speed"
                , String.valueOf(event.getVSreport().getSegment()), event.speed_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "segment_cnt"
                , String.valueOf(event.getVSreport().getSegment()), event.count_value, READ_WRITE);
    }

    protected void write_request(LREvent event) throws DatabaseException {
        //it simply construct the operations and return.
        transactionManager.SelectKeyRecord_noLock(txn_context
                , "segment_speed"
                , String.valueOf(event.getVSreport().getSegment())
                , event.speed_value//holder to be filled up.
                , READ_WRITE
        );

        transactionManager.SelectKeyRecord_noLock(txn_context
                , "segment_cnt"
                , String.valueOf(event.getVSreport().getSegment())
                , event.count_value//holder to be filled up.
                , READ_WRITE
        );


    }

}

