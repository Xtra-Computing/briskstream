package brisk.util;

import java.util.concurrent.locks.ReentrantLock;

import static applications.CONTROL.combo_bid_size;

public class BID {

    static ReentrantLock counterLock = new ReentrantLock(true); // enable fairness policy

    private volatile int bid = 0;

    private static BID ourInstance = new BID();

    public static BID getInstance() {
        return ourInstance;
    }


    //return the starting point of bid.
    public int get() {
        counterLock.lock();
        int rt = bid;
        // Always good practice to enclose locks in a try-finally block
        try {
            bid += combo_bid_size;//increment bid by combo_bid_size times...
        } finally {
            counterLock.unlock();
        }
        return rt;
    }

}
