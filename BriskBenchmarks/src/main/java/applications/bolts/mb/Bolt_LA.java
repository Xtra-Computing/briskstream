package applications.bolts.mb;

import applications.param.mb.MicroEvent;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static engine.profiler.Metrics.MeasureTools.*;
import static engine.profiler.Metrics.MeasureTools.END_COMPUTE_TIME_MEASURE;
import static engine.profiler.Metrics.MeasureTools.END_TRANSACTION_TIME_MEASURE;

public abstract class Bolt_LA extends MBBolt{


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

        END_WAIT_TIME_MEASURE(thread_Id);

        BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
        read_request(event);
        END_TP_CORE_TIME_MEASURE(txn_context.thread_Id, 1);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        read_core(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);
        END_TRANSACTION_TIME_MEASURE(thread_Id);

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
        END_WAIT_TIME_MEASURE(thread_Id);

        write_request(event);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        write_core(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);
        END_TRANSACTION_TIME_MEASURE(thread_Id);


    }
}
