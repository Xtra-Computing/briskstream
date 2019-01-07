package applications.bolts.ct;

import applications.param.DepositEvent;
import applications.param.TransactionEvent;
import brisk.components.operators.api.TransactionalBolt;
import engine.storage.datatype.DataBox;
import org.slf4j.Logger;

import java.util.List;

public abstract class CTBolt extends TransactionalBolt {

    public CTBolt(Logger log, int fid) {
        super(log, fid);
    }


    protected void DEPOSITE_CORE(DepositEvent event) {
        List<DataBox> values = event.account_value.record.getValues();

        long newAccountValue = values.get(1).getLong() + event.getAccountTransfer();

        values.get(1).setLong(newAccountValue);

        List<DataBox> asset_values = event.asset_value.record.getValues();

        long newAssetValue = values.get(1).getLong() + event.getBookEntryTransfer();

        asset_values.get(1).setLong(newAssetValue);
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
}
