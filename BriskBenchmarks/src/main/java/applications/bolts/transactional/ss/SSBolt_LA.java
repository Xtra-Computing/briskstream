package applications.bolts.transactional.ss;

import applications.param.mb.MicroEvent;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;

import static engine.profiler.MeasureTools.*;


public abstract class SSBolt_LA extends SSBolt {

    public SSBolt_LA(Logger log, int fid) {
        super(log, fid);
    }

    int _combo_bid_size = 1;


    //should only write.
    protected void LAL(MicroEvent event, long i, long _bid) throws DatabaseException {
        WRITE_LOCK_AHEAD(event, txn_context[(int) (i - _bid)]);
    }


    //lock_ratio-ahead phase.
    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException, InterruptedException {

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        transactionManager.getOrderLock().blocking_wait(_bid);
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        MicroEvent event = (MicroEvent) input_event;
        LAL(event, _bid, _bid);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE(thread_Id);
    }

    protected void PostLAL_process(long _bid) throws DatabaseException, InterruptedException {

        //txn process phase.
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {
            MicroEvent event = (MicroEvent) input_event;

            write_request_noLock(event, txn_context[(int) (i - _bid)]);
            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            WRITE_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);

            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }
}
