package applications.bolts.ct;

import applications.param.DepositEvent;
import applications.param.TransactionEvent;
import brisk.components.operators.api.TransactionalBolt;
import engine.DatabaseException;
import engine.storage.datatype.DataBox;
import org.slf4j.Logger;

import java.util.List;

import static engine.Meta.MetaTypes.AccessType.READ_WRITE;

public abstract class CTBolt extends TransactionalBolt {

    public CTBolt(Logger log, int fid) {
        super(log, fid);
    }



    protected void deposite_request_lock_ahead(DepositEvent event) throws DatabaseException {

        transactionManager.lock_ahead(txn_context, "accounts", event.getAccountId(), event.account_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);

    }

    protected boolean deposite_request(DepositEvent event) throws DatabaseException {

        transactionManager.SelectKeyRecord_noLock(txn_context, "accounts", event.getAccountId(), event.account_value, READ_WRITE);

        transactionManager.SelectKeyRecord_noLock(txn_context, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);

        assert event.account_value.record != null && event.asset_value.record != null;

        return true;
    }



    protected void DEPOSITE_CORE(DepositEvent event) throws InterruptedException {
        List<DataBox> values = event.account_value.record.getValues();

        long newAccountValue = values.get(1).getLong() + event.getAccountTransfer();

        values.get(1).setLong(newAccountValue);

        List<DataBox> asset_values = event.asset_value.record.getValues();

        long newAssetValue = values.get(1).getLong() + event.getBookEntryTransfer();

        asset_values.get(1).setLong(newAssetValue);

        collector.force_emit(event.getBid(), null, event.getTimestamp());
    }

    protected void transfer_request_lock_ahead(TransactionEvent event) throws DatabaseException {
        transactionManager.lock_ahead(txn_context, "accounts", event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "accounts", event.getTargetAccountId(), event.dst_account_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "bookEntries", event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "bookEntries", event.getTargetBookEntryId(), event.dst_asset_value, READ_WRITE);
    }

    protected boolean transfer_request(TransactionEvent event) throws DatabaseException {


        transactionManager.SelectKeyRecord_noLock(txn_context, "accounts", event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txn_context, "accounts", event.getTargetAccountId(), event.dst_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txn_context, "bookEntries", event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txn_context, "bookEntries", event.getTargetBookEntryId(), event.dst_asset_value, READ_WRITE);


        assert event.src_account_value.record != null && event.dst_account_value.record != null && event.src_asset_value.record != null && event.dst_asset_value.record != null;
        return true;
    }



    protected void TRANSFER_CORE(TransactionEvent event) throws InterruptedException {
        // check the preconditions

        DataBox sourceAccountBalance_value = event.src_account_value.record.getValues().get(1);
        final long sourceAccountBalance = sourceAccountBalance_value.getLong();

        DataBox sourceAssetValue_value = event.src_asset_value.record.getValues().get(1);
        final long sourceAssetValue = sourceAssetValue_value.getLong();

        DataBox targetAccountBalance_value = event.dst_account_value.record.getValues().get(1);
        final long targetAccountBalance = targetAccountBalance_value.getLong();

        DataBox targetAssetValue_value = event.dst_asset_value.record.getValues().get(1);
        final long targetAssetValue = targetAssetValue_value.getLong();


        if (sourceAccountBalance > event.getMinAccountBalance()
                && sourceAccountBalance > event.getAccountTransfer()
                && sourceAssetValue > event.getBookEntryTransfer()) {

            // compute the new balances
            final long newSourceBalance = sourceAccountBalance - event.getAccountTransfer();
            final long newTargetBalance = targetAccountBalance + event.getAccountTransfer();
            final long newSourceAssets = sourceAssetValue - event.getBookEntryTransfer();
            final long newTargetAssets = targetAssetValue + event.getBookEntryTransfer();


            // write back the updated values
            sourceAccountBalance_value.setLong(newSourceBalance);
            targetAccountBalance_value.setLong(newTargetBalance);

            targetAccountBalance_value.setLong(newSourceAssets);
            targetAssetValue_value.setLong(newTargetAssets);

            collector.force_emit(event.getBid(), new TransactionResult(event, true, newSourceBalance, newTargetBalance), event.getTimestamp());

        } else {
            collector.force_emit(event.getBid(), new TransactionResult(event, false, sourceAccountBalance, targetAccountBalance), event.getTimestamp());
        }

    }

    @Override
    protected TransactionEvent next_event(long bid, Long timestamp) {
        return null;
    }

    @Override
    public void prepareEvents() {

    }


    protected void dispatch_process(Object event, Long timestamp) throws DatabaseException, InterruptedException {
        if (event instanceof DepositEvent) {
//            LOG.info(((DepositEvent) event).getBid() + " " + event.toString());
            deposite_handle((DepositEvent) event, timestamp);//buy item at certain price.
        } else if (event instanceof TransactionEvent) {
//            LOG.info(((TransactionEvent) event).getBid() + " " + event.toString());
            transfer_handle((TransactionEvent) event, timestamp);//alert price
        } else {
            throw new UnsupportedOperationException();
        }
    }

    protected abstract void transfer_handle(TransactionEvent event, Long timestamp) throws DatabaseException, InterruptedException;

    protected abstract void deposite_handle(DepositEvent event, Long timestamp) throws DatabaseException, InterruptedException;
}
