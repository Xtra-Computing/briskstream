package engine.content;

import engine.Meta.MetaTypes;
import engine.common.OrderLock;
import engine.common.RWLock;
import engine.storage.SchemaRecord;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;


/**
 * #elif defined(LOCK) || defined(OCC) || defined(SILO) || defined(ST)
 * LockContentImpl content_;
 */
public class LockContentImpl extends LockContent {
    public final static String LOCK_CONTENT = "LOCK_CONTENT";
    private static final Logger LOG = LoggerFactory.getLogger(LockContentImpl.class);
    AtomicLong timestamp_ = new AtomicLong(0);
    RWLock lock_ = new RWLock();


    //used by non-blocking lock.
    @Override
    public boolean TryWriteLock(OrderLock orderLock, TxnContext txn_context) {

        return orderLock.blocking_wait(txn_context.getBID()) &&
                lock_.TryWriteLock();
    }

    @Override
    public boolean TryReadLock(OrderLock orderLock, TxnContext txn_context) {
        return orderLock.blocking_wait(txn_context.getBID()) &&
                lock_.TryReadLock();
    }

    @Override
    public boolean AcquireReadLock() {
        lock_.AcquireReadLock();
        return true;
    }

    @Override
    public boolean AcquireWriteLock() {
        lock_.AcquireWriteLock();
        return true;
    }

    @Override
    public SchemaRecord ReadAccess(long ts, MetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaRecord readPreValues(long ts) {
        return null;
    }


    @Override
    public boolean TryReadLock() {
        return lock_.TryReadLock();
    }

    @Override
    public boolean TryWriteLock() {
        return lock_.TryWriteLock();
    }


    @Override
    public void SetTimestamp(long timestamp) {
        timestamp_.set(timestamp);
    }

    @Override
    public long GetTimestamp() {
        return timestamp_.get();
    }

    @Override
    public void ReleaseReadLock() {
        lock_.ReleaseReadLock();
    }

    @Override
    public void ReleaseWriteLock() {
        lock_.ReleaseWriteLock();
    }


}
