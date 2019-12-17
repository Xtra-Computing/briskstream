package applications.bolts.transactional.tp;

import applications.param.lr.LREvent;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static engine.profiler.MeasureTools.*;

public abstract class TPBolt_LA extends TPBolt {

    public TPBolt_LA(Logger log, int fid) {
        super(log, fid);
    }



    protected void LAL(LREvent event, long i, long _bid) throws DatabaseException {
        REQUEST_LOCK_AHEAD(event, txn_context[(int) (i - _bid)]);
    }


    //lock_ratio-ahead phase.
    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        int _combo_bid_size = 1;

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        transactionManager.getOrderLock().blocking_wait(_bid);

        long lock_time_measure = 0;
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {

            txn_context[(int) (i - _bid)] = new TxnContext(thread_Id, this.fid, i);

            LREvent event = (LREvent) input_event;

            LAL(event, i, _bid);
            BEGIN_LOCK_TIME_MEASURE(thread_Id);

            lock_time_measure += END_LOCK_TIME_MEASURE_ACC(thread_Id);
        }
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
    }


    protected void PostLAL_process(long _bid) throws DatabaseException {
        int _combo_bid_size = 1;
        //txn process phase.
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {

            LREvent event = (LREvent) input_event;

            TXN_REQUEST_NOLOCK(event, txn_context[(int) (i - _bid)]);


            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            TXN_REQUEST_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);


            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }


}

