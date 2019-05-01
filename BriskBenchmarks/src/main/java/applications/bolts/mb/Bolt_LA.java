package applications.bolts.mb;

import applications.param.mb.MicroEvent;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static applications.CONTROL.combo_bid_size;
import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.*;

public abstract class Bolt_LA extends MBBolt {

    public transient TxnContext[] txn_context = new TxnContext[combo_bid_size];

    public Bolt_LA(Logger log, int fid) {
        super(log, fid);
    }

    @Override
    protected void read_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException {
    }


    @Override
    protected void write_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException {
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

        //lock-ahead phase.
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

        //txn process phase.
        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            MicroEvent event = (MicroEvent) db.eventManager.get((int) i);

            boolean flag = event.READ_EVENT();

            if (flag) {//read

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                read_request(event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                read_core(event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            } else {

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                write_request(event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                write_core(event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            }
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE_LAL(thread_Id);


        //post stream processing phase..
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
        END_TOTAL_TIME_MEASURE_ACC(thread_Id);

    }

}
