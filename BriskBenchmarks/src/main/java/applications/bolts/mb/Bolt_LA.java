package applications.bolts.mb;

import applications.param.mb.MicroEvent;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.*;

public abstract class Bolt_LA extends GSBolt {

    public Bolt_LA(Logger log, int fid) {
        super(log, fid);
    }

    int _combo_bid_size = 1;//must be one.

    //lock_ratio-ahead phase.
    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException, InterruptedException {

        for (long i = _bid; i < _bid + _combo_bid_size; i++) {

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

            long lock_time_measure = END_LOCK_TIME_MEASURE_ACC(thread_Id);
            transactionManager.getOrderLock().advance();
            END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        //pre stream processing phase..

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        Long timestamp;//in.getLong(1);
        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        long _bid = in.getBID();

        END_PREPARE_TIME_MEASURE(thread_Id);


        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);//need to amortize.

        LAL_PROCESS(_bid);

        PostLAL_process(_bid);

        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE(thread_Id);

        POST_PROCESS(_bid, timestamp, 1);//otherwise deadlock.

        END_TOTAL_TIME_MEASURE_ACC(thread_Id, 1);//otherwise deadlock.
    }
}
