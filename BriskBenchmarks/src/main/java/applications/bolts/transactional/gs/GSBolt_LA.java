package applications.bolts.transactional.gs;

import applications.param.mb.MicroEvent;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static engine.profiler.MeasureTools.*;


public abstract class GSBolt_LA extends GSBolt {

    public GSBolt_LA(Logger log, int fid) {
        super(log, fid);
    }

    int _combo_bid_size = 1;


    protected void LAL(MicroEvent event, long i, long _bid) throws DatabaseException {
        boolean flag = event.READ_EVENT();
        if (flag) {//read
            READ_LOCK_AHEAD(event, txn_context[(int) (i - _bid)]);
        } else {
            WRITE_LOCK_AHEAD(event, txn_context[(int) (i - _bid)]);
        }
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

            MicroEvent event = (MicroEvent) input_event;
            LAL(event, i, _bid);
            BEGIN_LOCK_TIME_MEASURE(thread_Id);


            lock_time_measure += END_LOCK_TIME_MEASURE_ACC(thread_Id);
        }
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
    }

    protected void PostLAL_process(long _bid) throws DatabaseException, InterruptedException {

        //txn process phase.
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {

            MicroEvent event = (MicroEvent) input_event;

            boolean flag = event.READ_EVENT();

            if (flag) {//read

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                read_request_noLock(event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                READ_CORE(event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            } else {

                BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
                write_request_noLock(event, txn_context[(int) (i - _bid)]);
                END_TP_CORE_TIME_MEASURE_ACC(thread_Id);

                BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
                WRITE_CORE(event);
                END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            }
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }
}
