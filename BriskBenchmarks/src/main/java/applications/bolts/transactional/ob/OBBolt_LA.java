package applications.bolts.transactional.ob;

import applications.param.TxnEvent;
import applications.param.ob.AlertEvent;
import applications.param.ob.BuyingEvent;
import applications.param.ob.ToppingEvent;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static engine.profiler.MeasureTools.*;

public abstract class OBBolt_LA extends OBBolt {

    int _combo_bid_size = 1;


    public OBBolt_LA(Logger log, int fid) {
        super(log, fid);
    }


    protected void LAL(Object event, long i, long _bid) throws DatabaseException {
        if (event instanceof BuyingEvent) {
            BUYING_REQUEST_LOCKAHEAD((BuyingEvent) event, txn_context[(int) (i - _bid)]);
        } else if (event instanceof AlertEvent) {
            ALERT_REQUEST_LOCKAHEAD((AlertEvent) event, txn_context[(int) (i - _bid)]);
        } else if (event instanceof ToppingEvent) {
            TOPPING_REQUEST_LOCKAHEAD((ToppingEvent) event, txn_context[(int) (i - _bid)]);
        } else
            throw new UnsupportedOperationException();
    }


    //lock_ratio-ahead phase.
    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException, InterruptedException {

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        transactionManager.getOrderLock().blocking_wait(_bid);

        long lock_time_measure = 0;

        for (long i = _bid; i < _bid + _combo_bid_size; i++) {

            txn_context[(int) (i - _bid)] = new TxnContext(thread_Id, this.fid, i);


            BEGIN_LOCK_TIME_MEASURE(thread_Id);

            LAL(input_event, i, _bid);

            lock_time_measure += END_LOCK_TIME_MEASURE_ACC(thread_Id);
        }
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
    }


    protected void PostLAL_process(long _bid) throws DatabaseException {

        //txn process phase.
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {

            TxnEvent event = (TxnEvent) input_event;

            if (event instanceof BuyingEvent) {

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                BUYING_REQUEST_NOLOCK((BuyingEvent) event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                BUYING_REQUEST_CORE((BuyingEvent) event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            } else if (event instanceof AlertEvent) {
                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                ALERT_REQUEST_NOLOCK((AlertEvent) event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                ALERT_REQUEST_CORE((AlertEvent) event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            } else {
                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                TOPPING_REQUEST_NOLOCK((ToppingEvent) event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                TOPPING_REQUEST_CORE((ToppingEvent) event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            }
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }


}
