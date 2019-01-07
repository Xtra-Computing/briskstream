package applications.param;

import engine.storage.SchemaRecordRef;
import engine.storage.datatype.DataBox;

import java.util.List;

public class DepositEvent {

    //embeded state.
    final long bid;//as msg id.
    protected final int pid=0;
    public double[] index_time = new double[1];
    //updated state...to be written.
    public long newAccountValue;
    public long newAssetValue;
    //place-rangeMap.
    public SchemaRecordRef account_value = new SchemaRecordRef();
    public SchemaRecordRef asset_value = new SchemaRecordRef();
    private String accountId;

    //expected state.
    //long Item_value=0;
    //long asset_value=0;
    private String bookEntryId;
    private long accountTransfer;
    private long bookEntryTransfer;
    private long timestamp;//emit timestamp

    /**
     * Creates a new DepositEvent.
     */
    public DepositEvent(
            long bid, String accountId,
            String bookEntryId,
            long accountTransfer,
            long bookEntryTransfer) {
        this.bid = bid;
        this.accountId = accountId;
        this.bookEntryId = bookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getBookEntryId() {
        return bookEntryId;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public long getAccountTransfer() {
        return accountTransfer;
    }

    public long getBookEntryTransfer() {
        return bookEntryTransfer;
    }

    public List<DataBox> getUpdatedAcount_value() {
        return null;
    }

    public List<DataBox> getUpdatedAsset_value() {
        return null;
    }


    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "DepositEvent {"
                + "accountId=" + accountId
                + ", bookEntryId=" + bookEntryId
                + ", accountTransfer=" + accountTransfer
                + ", bookEntryTransfer=" + bookEntryTransfer
                + '}';
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getBid() {
        return bid;
    }

    public int getPid() {
        return pid;
    }

    public int num_p() {
        return 0;
    }

    public long[] getBid_array() {
        return new long[0];
    }
}
