package applications.bolts.tp;


import applications.param.txn.lr.LREvent;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_ts_nopush extends TPBolt_ts {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_ts_nopush.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TPBolt_ts_nopush(int fid) {
        super(fid);
    }


    @Override
    protected void REQUEST_CONSTRUCT(LREvent event, TxnContext txnContext) throws DatabaseException {
        //it simply construct the operations and return.
        transactionManager.Asy_ReadRecord(txnContext
                , "segment_speed"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.speed_value,//holder to be filled up.
                null
        );          //asynchronously return.

        transactionManager.Asy_ReadRecord(txnContext
                , "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.count_value//holder to be filled up.
                , null
        );          //asynchronously return.
        LREvents.add(event);
    }

    @Override
    protected void REQUEST_REQUEST_CORE() {

        for (LREvent event : LREvents) {
            TXN_REQUEST_CORE(event);
        }
    }
}
