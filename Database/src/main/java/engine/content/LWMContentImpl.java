package engine.content;

import engine.Meta.MetaTypes;
import engine.common.SpinLock;
import engine.storage.SchemaRecord;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;

import static engine.content.common.ContentCommon.kRecycleLength;
import static engine.profiler.Metrics.MeasureTools.BEGIN_TP_CORE_TIME_MEASURE;
import static engine.profiler.Metrics.MeasureTools.END_TP_CORE_TIME_MEASURE;

/**
 * This corresponds to ACEP's SharedTable, but for every table d_record.
 */
public class LWMContentImpl extends LWMContent {
    public final static String LWM_CONTENT = "LWM_CONTENT";
    private static final Logger LOG = LoggerFactory.getLogger(LWMContentImpl.class);
    SpinLock wait_lock_ = new SpinLock();
    volatile boolean is_certifying_ = false;
    volatile long read_count_;
    volatile boolean is_writing_ = false;
    volatile TreeSet<Long> set = new TreeSet<>();
    volatile long lwm = Long.MAX_VALUE;

    public LWMContentImpl() {
        set.add(lwm);
    }


    @Override
    public boolean AcquireReadLock() {
        boolean rt = true;
        wait_lock_.Lock();
        if (is_certifying_) {
            rt = false;
        } else {
            ++read_count_;
        }
        wait_lock_.Unlock();
        return rt;
    }

    /**
     * Read lock will not block write.. --> major difference to S2PL.
     * Write will still prevent Write.. --> multiple write to a d_record is not allowed.
     *
     * @return
     */
    @Override
    public boolean AcquireWriteLock() {
        boolean rt = true;
        wait_lock_.Lock();
        if (is_writing_ == true || is_certifying_ == true) {
            rt = false;
        } else {
            is_writing_ = true;
        }
        wait_lock_.Unlock();
        return rt;
    }

    @Override
    public void ReleaseReadLock() {
        wait_lock_.Lock();
        assert (read_count_ > 0);
        --read_count_;
        wait_lock_.Unlock();
    }

    @Override
    public void ReleaseWriteLock() {
        wait_lock_.Lock();
        assert (is_writing_ == true);
        is_writing_ = false;
        wait_lock_.Unlock();
    }


    @Override
    public long GetLWM() {
        return lwm;
    }

    @Override
    public SchemaRecord ReadAccess(TxnContext txn_context, MetaTypes.AccessType accessType) {
        int retry_count = 0;
        long bid = txn_context.getBID();
        switch (accessType) {
            case READ_ONLY:
                while (bid > GetLWM() && !Thread.currentThread().isInterrupted()) {
//					retry_count++;
//					if (retry_count > 100) {
//						LOG.error("Retry:" + retry_count + " ts: " + ts + " lwm:" + lwm);
//					}
                }
                break;
            case READ_WRITE:
                while (bid != GetLWM() && !Thread.currentThread().isInterrupted()) {
//					retry_count++;
//					if (retry_count > 100) {
//						LOG.error("Retry:" + retry_count + " ts: " + ts + " lwm:" + lwm);
//					}
                }
                break;
        }
        //TODO: there is a null pointer error at this line.
        BEGIN_TP_CORE_TIME_MEASURE(txn_context.thread_Id);
        SchemaRecord record = readValues(bid);
        END_TP_CORE_TIME_MEASURE(txn_context.thread_Id, 1);
        return record;

    }

    @Override
    public SchemaRecord ReadAccess(long ts, MetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaRecord readPreValues(long ts) {
        return null;
    }

    //However, once T is ready to commit, it must obtain a certify lock on all items that it currently holds write locks on before it can commit.
    @Override
    public boolean AcquireCertifyLock() {
        boolean rt = true;
        wait_lock_.Lock();
        assert (is_writing_);
        assert (!is_certifying_);
        if (read_count_ != 0) {
            rt = false;
        } else {
            is_writing_ = false;
            is_certifying_ = true;
        }
        wait_lock_.Unlock();
        return rt;
    }

    @Override
    public void WriteAccess(long commit_timestamp, SchemaRecord local_record_) {
        rw_lock_.AcquireWriteLock();
        updateValues(commit_timestamp, local_record_);
        CollectGarbage();
        rw_lock_.ReleaseWriteLock();
    }

    private void CollectGarbage() {
        if (versions.size() > kRecycleLength) {
            ClearHistory(lwm);
        }
    }

    private void ClearHistory(long min_thread_ts) {
//		versions.clear();
        versions.headMap(min_thread_ts).clear();
    }

    @Override
    public void ReleaseCertifyLock() {
        wait_lock_.Lock();
        assert (is_certifying_);
        is_certifying_ = false;
        wait_lock_.Unlock();
    }

    private void MaintainLWM() {
        lwm = set.first();
    }

    @Override
    public synchronized void AddLWM(long ts) {
        set.add(ts);
        MaintainLWM();
    }

    @Override
    public synchronized void DeleteLWM(long ts) {
        set.remove(ts);
        MaintainLWM();
    }


}
