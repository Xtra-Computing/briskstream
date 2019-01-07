package applications.param;

import engine.storage.SchemaRecordRef;
import engine.storage.datatype.DataBox;

import java.util.List;

public class TransactionEvent {

	public long getBid() {
		return bid;
	}

	//embeded state.
    final long bid;
    public double[] index_time = new double[1];
    public SchemaRecordRef src_account_value = new SchemaRecordRef();
    public SchemaRecordRef dst_account_value = new SchemaRecordRef();
    public SchemaRecordRef src_asset_value = new SchemaRecordRef();
    public SchemaRecordRef dst_asset_value = new SchemaRecordRef();
    public boolean[] success = new boolean[1];// pointer of event success.
    private String sourceAccountId;
    private String targetAccountId;
    private String sourceBookEntryId;
    private String targetBookEntryId;
    private long accountTransfer;
    private long bookEntryTransfer;
    private long minAccountBalance;
	private long timestamp;

	/**
     * Creates a new TransactionEvent for the given accounts and book entries.
     */
    public TransactionEvent(
            long bid, String sourceAccountId,
            String targetAccountId,
            String sourceBookEntryId,
            String targetBookEntryId,
            long accountTransfer,
            long bookEntryTransfer,
            long minAccountBalance) {
        this.bid = bid;

        this.sourceAccountId = sourceAccountId;
        this.targetAccountId = targetAccountId;
        this.sourceBookEntryId = sourceBookEntryId;
        this.targetBookEntryId = targetBookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        this.minAccountBalance = minAccountBalance;
    }

    public String getSourceAccountId() {
        return sourceAccountId;
    }

    public String getTargetAccountId() {
        return targetAccountId;
    }

    public String getSourceBookEntryId() {
        return sourceBookEntryId;
    }

    public String getTargetBookEntryId() {
        return targetBookEntryId;
    }

    public long getMinAccountBalance() {
        return minAccountBalance;
    }

    public long getAccountTransfer() {
        return accountTransfer;
    }

    public long getBookEntryTransfer() {
        return bookEntryTransfer;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<DataBox> getUpdatedSourceBalance() {
        return null;
    }

    public List<DataBox> getUpdatedTargetBalance() {
        return null;
    }

    public List<DataBox> getUpdatedSourceAsset_value() {
        return null;
    }

    public List<DataBox> getUpdatedTargetAsset_value() {
        return null;
    }


    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "TransactionEvent {"
                + "sourceAccountId=" + sourceAccountId
                + ", targetAccountId=" + targetAccountId
                + ", sourceBookEntryId=" + sourceBookEntryId
                + ", targetBookEntryId=" + targetBookEntryId
                + ", accountTransfer=" + accountTransfer
                + ", bookEntryTransfer=" + bookEntryTransfer
                + ", minAccountBalance=" + minAccountBalance
                + '}';
    }

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

    public int getPid() {
        return 0;
    }

    public int num_p() {
        return 0;
    }

    public long[] getBid_array() {
        return new long[0];
    }
}
