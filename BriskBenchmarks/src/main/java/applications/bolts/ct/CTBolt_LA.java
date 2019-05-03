package applications.bolts.ct;

import applications.param.ct.DepositEvent;
import applications.param.ct.TransactionEvent;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.*;

public class CTBolt_LA extends CTBolt {

    public CTBolt_LA(Logger log, int fid) {
        super(log, fid);
    }


    @Override
    protected void deposite_handle(DepositEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(event.getBid());//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        deposite_request_lock_ahead(event);
        long lock_time_measure =   END_LOCK_TIME_MEASURE_ACC(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);


        BEGIN_TP_TIME_MEASURE(thread_Id);
        deposite_request(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        DEPOSITE_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);

    }

    @Override
    protected void transfer_handle(TransactionEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(event.getBid());//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        transfer_request_lock_ahead(event);
        long lock_time_measure =  END_LOCK_TIME_MEASURE_ACC(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);

        BEGIN_TP_TIME_MEASURE(thread_Id);
        transfer_request(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        TRANSFER_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);


    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);

        long bid = in.getBID();

        Object event = db.eventManager.get((int) bid);

        Long timestamp;//in.getLong(1);

        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        END_PREPARE_TIME_MEASURE(thread_Id);

        dispatch_process(event, timestamp);
    }
}
