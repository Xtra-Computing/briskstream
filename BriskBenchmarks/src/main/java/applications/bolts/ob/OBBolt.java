package applications.bolts.ob;

import applications.param.TransactionEvent;
import applications.param.ob.AlertEvent;
import applications.param.ob.BuyingEvent;
import applications.param.ob.ToppingEvent;
import applications.tasks.stateless_task;
import brisk.components.operators.api.TransactionalBolt;
import engine.DatabaseException;
import engine.storage.datatype.DataBox;
import org.slf4j.Logger;

import java.util.List;

import static applications.constants.OnlineBidingSystemConstants.Constant.NUM_ACCESSES_PER_BUY;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;

public abstract class OBBolt extends TransactionalBolt {

    public OBBolt(Logger log, int fid) {
        super(log, fid);
    }

    @Override
    protected TransactionEvent next_event(long bid, Long timestamp) {
        return null;
    }


    int random_integer;

    /**
     * Perform some dummy calculation to simulate authentication process..
     *
     * @param bid
     * @param timestamp
     */
    protected void auth(long bid, Long timestamp) {
//        System.out.println(generatedString);
        stateless_task.random_compute(100);
    }


    protected void Topping_REQUEST_LA(ToppingEvent event) throws DatabaseException {
        for (int i = 0; i < event.getNum_access(); ++i)
            transactionManager.lock_ahead(txn_context, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
    }


    protected void Topping_REQUEST(ToppingEvent event) throws DatabaseException {
        for (int i = 0; i < event.getNum_access(); ++i) {
            transactionManager.SelectKeyRecord_noLock(txn_context, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].record != null;
        }
    }


    protected void Topping_CORE(ToppingEvent event) throws InterruptedException {

        for (int i = 0; i < event.getNum_access(); ++i) {
            List<DataBox> values = event.record_refs[i].record.getValues();
            long newQty = values.get(2).getLong() + event.getItemTopUp()[i];
            values.get(2).setLong(newQty);
        }
        collector.force_emit(event.getBid(), true, event.getTimestamp());//the tuple is immediately finished.
    }

    protected void Alert_REQUEST_LA(AlertEvent event) throws DatabaseException {
        for (int i = 0; i < event.getNum_access(); ++i)
            transactionManager.lock_ahead(txn_context, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
    }

    protected void Alert_REQUEST(AlertEvent event) throws DatabaseException {
        for (int i = 0; i < event.getNum_access(); ++i) {
            transactionManager.SelectKeyRecord_noLock(txn_context, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].record != null;
        }
    }


    protected void Alert_CORE(AlertEvent event) throws InterruptedException {
        for (int i = 0; i < event.getNum_access(); ++i) {
            List<DataBox> values = event.record_refs[i].record.getValues();
            long newPrice = event.getAsk_price()[i];
            values.get(1).setLong(newPrice);
        }
        collector.force_emit(event.getBid(), true, event.getTimestamp());//the tuple is immediately finished.

    }

    protected void Buying_REQUEST_LA(BuyingEvent event) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i)
            transactionManager.lock_ahead(txn_context, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
    }


    protected void Buying_REQUEST(BuyingEvent event) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i) {
            transactionManager.SelectKeyRecord_noLock(txn_context, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].record != null;
        }
    }

    protected void Buying_CORE(BuyingEvent event) throws InterruptedException {


        //measure_end if any item is not able to buy.

        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i) {
            long bidPrice = event.getBidPrice(i);
            long qty = event.getBidQty(i);


            List<DataBox> values = event.record_refs[i].record.getValues();
            long askPrice = values.get(1).getLong();
            long left_qty = values.get(2).getLong();
            if (bidPrice < askPrice || qty > left_qty) {
                //bid failed.
                collector.force_emit(event.getBid(), new BidingResult(event, false), event.getTimestamp());
                return;
            }
        }

        //if allowed to proceed.
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i) {
            long bidPrice = event.getBidPrice(i);
            long qty = event.getBidQty(i);

            List<DataBox> values = event.record_refs[i].record.getValues();
            long askPrice = values.get(1).getLong();
            long left_qty = values.get(2).getLong();

            //bid success
            values.get(2).setLong(left_qty - qty);//new quantity.
        }
        collector.force_emit(event.getBid(), new BidingResult(event, true), event.getTimestamp());
    }


    protected void dispatch_process(Object event, Long timestamp) throws DatabaseException, InterruptedException {
        if (event instanceof BuyingEvent) {
            ((BuyingEvent) event).setTimestamp(timestamp);
            buy_handle((BuyingEvent) event, timestamp);//buy item at certain price.
        } else if (event instanceof AlertEvent) {
            ((AlertEvent) event).setTimestamp(timestamp);
            altert_handle((AlertEvent) event, timestamp);//alert price
        } else if (event instanceof ToppingEvent) {
            ((ToppingEvent) event).setTimestamp(timestamp);
            topping_handle((ToppingEvent) event, timestamp);//topping qty
        }
    }

    protected abstract void buy_handle(BuyingEvent event, Long timestamp) throws DatabaseException, InterruptedException;

    protected abstract void altert_handle(AlertEvent event, Long timestamp) throws DatabaseException, InterruptedException;

    protected abstract void topping_handle(ToppingEvent event, Long timestamp) throws DatabaseException, InterruptedException;
}
