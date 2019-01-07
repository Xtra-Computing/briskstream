package engine.common;

public class RWLock {

    final static int NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2;//enum LockType : size_t{ NO_LOCK, READ_LOCK, WRITE_LOCK };
    SpinLock spinlock_ = new SpinLock();
    volatile int reader_count_ = 0;
    volatile int lock_type_ = NO_LOCK;

    public void AcquireReadLock() {
        while (lock_type_ == WRITE_LOCK) {
            //spin wait.
        }
        spinlock_.Lock();
        if (lock_type_ == WRITE_LOCK) {
            spinlock_.Unlock();
        } else {
            if (lock_type_ == NO_LOCK) {
                lock_type_ = READ_LOCK;
                ++reader_count_;
            } else {
                assert (lock_type_ == READ_LOCK);
                ++reader_count_;
            }
            spinlock_.Unlock();
            return;
        }
    }

    public void AcquireWriteLock() {
        while (true) {
            while (lock_type_ != NO_LOCK) {
            }
            spinlock_.Lock();
            if (lock_type_ != NO_LOCK) {
                spinlock_.Unlock();
            } else {
                assert (lock_type_ == NO_LOCK);
                lock_type_ = WRITE_LOCK;
                spinlock_.Unlock();
                return;
            }
        }
    }

    public void ReleaseReadLock() {
        spinlock_.Lock();
        --reader_count_;
        if (reader_count_ == 0) {
            lock_type_ = NO_LOCK;
        }
        spinlock_.Unlock();
    }

    public boolean TryReadLock() {
        boolean rt = false;
        spinlock_.Lock();
        if (lock_type_ == NO_LOCK) {
            lock_type_ = READ_LOCK;
            ++reader_count_;
            rt = true;
        } else if (lock_type_ == READ_LOCK) {
            ++reader_count_;
            rt = true;
        } else {
            rt = false;
        }
        spinlock_.Unlock();
        return rt;
    }

    public boolean TryWriteLock() {
        boolean rt = false;
        spinlock_.Lock();
        if (lock_type_ == NO_LOCK) {
            lock_type_ = WRITE_LOCK;

            rt = true;
        } else {
            rt = false;
        }
        spinlock_.Unlock();
        return rt;
    }

    public void ReleaseWriteLock() {
        spinlock_.Lock();
        lock_type_ = NO_LOCK;
        spinlock_.Unlock();
    }


}
