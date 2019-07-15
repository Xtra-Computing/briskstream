package engine;

/**
 * The LockManager provides a basic locking implementation that ensures that only one transaction runs at a time.
 */
public class LockManager {
    private boolean databaseLocked;
    private long databaseTransactionOwner;

    public LockManager() {
        this.databaseLocked = false;
        this.databaseTransactionOwner = -1;
    }

    /**
     * Acquires a Lock on tableNum of type lockType for transaction transNum.
     *
     * @param tableName the database to Lock on
     * @param transNum  the transactions id
     * @param lockType  the type of Lock
     */
    public synchronized void acquireLock(String tableName, long transNum, LockType lockType) {
        while (this.databaseLocked) {
            if (this.databaseTransactionOwner == transNum) {
                break;
            }
            try {
                wait();
            } catch (InterruptedException ignored) {

            }
        }

        this.databaseTransactionOwner = transNum;
        this.databaseLocked = true;
    }

    /**
     * Releases transNum's Lock on tableName.
     *
     * @param tableName the table that was locked
     * @param transNum  the transaction that held the Lock
     */
    public synchronized void releaseLock(String tableName, long transNum) {
        if (this.databaseLocked && this.databaseTransactionOwner == transNum) {
            this.databaseLocked = false;
            this.databaseTransactionOwner = -1;

            notifyAll();
        }
    }

    /**
     * Returns a boolean indicating whether or not transNum holds a Lock of type lockType on tableName.
     *
     * @param tableName the table that we're checking
     * @param transNum  the transaction that we're checking for
     * @param lockType  the Lock type
     * @return whether the Lock is held or not
     */
    public synchronized boolean holdsLock(String tableName, long transNum, LockType lockType) {
        return this.databaseLocked && this.databaseTransactionOwner == transNum;
    }

    public enum LockType {SHARED, EXCLUSIVE}
}
