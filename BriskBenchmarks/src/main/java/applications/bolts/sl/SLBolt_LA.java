package applications.bolts.sl;

import applications.param.sl.DepositEvent;
import applications.param.sl.TransactionEvent;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static engine.profiler.Metrics.MeasureTools.*;

public class SLBolt_LA extends SLBolt {
    int _combo_bid_size = 1;


    public SLBolt_LA(Logger log, int fid) {
        super(log, fid);
    }


    protected void PostLAL_process(long _bid) throws DatabaseException, InterruptedException {

        //txn process phase.
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {

            Object event = db.eventManager.get((int) i);

            if (event instanceof DepositEvent) {

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                DEPOSITE_REQUEST_NOLOCK((DepositEvent) event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                DEPOSITE_REQUEST_CORE((DepositEvent) event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            } else {

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                TRANSFER_REQUEST_NOLOCK((TransactionEvent) event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                TRANSFER_REQUEST_CORE((TransactionEvent) event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            }
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }

    }


    @Override
    protected void LAL_PROCESS(long _bid) throws InterruptedException, DatabaseException {
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the event sequence order.
        transactionManager.getOrderLock().blocking_wait(_bid);

        long lock_time_measure = 0;
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {

            txn_context[(int) (i - _bid)] = new TxnContext(thread_Id, this.fid, i);

            Object event = db.eventManager.get((int) i);

            BEGIN_LOCK_TIME_MEASURE(thread_Id);

            if (event instanceof DepositEvent) {
                DEPOSITE_LOCK_AHEAD((DepositEvent) event, txn_context[(int) (i - _bid)]);
            } else if (event instanceof TransactionEvent) {
                TRANSFER_LOCK_AHEAD((TransactionEvent) event, txn_context[(int) (i - _bid)]);
            } else {
                throw new UnsupportedOperationException();
            }

            lock_time_measure += END_LOCK_TIME_MEASURE_ACC(thread_Id);
        }
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
    }


}
