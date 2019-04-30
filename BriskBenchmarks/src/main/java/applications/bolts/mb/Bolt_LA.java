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


    public Bolt_LA(Logger log, int fid) {
        super(log, fid);
    }

    @Override
    protected void read_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        long bid = event.getBid();

        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the event sequence order.
        transactionManager.getOrderLock().blocking_wait(bid);

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        read_lock_ahead(event);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();

        END_WAIT_TIME_MEASURE_ACC(thread_Id);

        BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
        read_request(event);
        END_TP_CORE_TIME_MEASURE(txn_context.thread_Id, 1);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        read_core(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);
        END_TRANSACTION_TIME_MEASURE(thread_Id, txn_context);

    }


    @Override
    protected void write_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        long bid = event.getBid();

        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(bid);//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        write_lock_ahead(event);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.
        END_WAIT_TIME_MEASURE_ACC(thread_Id);

        write_request(event);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        write_core(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);
        END_TRANSACTION_TIME_MEASURE(thread_Id, txn_context);

    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {


        Long timestamp;//in.getLong(1);
        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//


        long _bid = in.getBID();

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);//need to amortize.

        //lock-ahead phase.
        for (long i = _bid; i < _bid + combo_bid_size; i++) {


            BEGIN_PREPARE_TIME_MEASURE(thread_Id);

            MicroEvent event = (MicroEvent) db.eventManager.get((int) i);

            (event).setTimestamp(timestamp);

            END_PREPARE_TIME_MEASURE_ACC(thread_Id);//accumulate


            (event).setTimestamp(timestamp);

            boolean flag = event.READ_EVENT();
            if (flag) {//read

                BEGIN_WAIT_TIME_MEASURE(thread_Id);
                //ensures that locks are added in the event sequence order.
                transactionManager.getOrderLock().blocking_wait(i);
                BEGIN_LOCK_TIME_MEASURE_ACC(thread_Id);
                read_lock_ahead(event);
                END_LOCK_TIME_MEASURE(thread_Id);
                transactionManager.getOrderLock().advance();
                END_WAIT_TIME_MEASURE_ACC(thread_Id);


            }


        }


    }

}
