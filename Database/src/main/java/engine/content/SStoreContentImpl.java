package engine.content;

import engine.Meta.MetaTypes;
import engine.common.SpinLock;
import engine.storage.SchemaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;



public class SStoreContentImpl extends SStoreContent {
    private static final Logger LOG = LoggerFactory.getLogger(SStoreContentImpl.class);
    public final int pid;
    final SpinLock spinlock_;//Each partition has a spin lock.
    AtomicLong timestamp_ = new AtomicLong(0);
    public final static String SSTORE_CONTENT = "SSTORE_CONTENT";

    public SStoreContentImpl(SpinLock[] spinlock_, int pid) {
        this.pid = pid;
        this.spinlock_ = spinlock_[pid];
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
    public SchemaRecord ReadAccess(long ts, MetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    public boolean TryLockPartitions() {
        return spinlock_.Try_Lock();
    }

    public void LockPartitions() {

        spinlock_.Lock();
    }

    public void UnlockPartitions() {
        spinlock_.Unlock();
    }
}
