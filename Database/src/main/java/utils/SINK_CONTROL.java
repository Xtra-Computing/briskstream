package utils;

import java.util.concurrent.locks.ReentrantLock;

import static applications.CONTROL.combo_bid_size;

public class SINK_CONTROL {

    static ReentrantLock counterLock = new ReentrantLock(true); // enable fairness policy

    private volatile int counter = 0;

    public double throughput = 0;


    private static SINK_CONTROL ourInstance = new SINK_CONTROL();
    private int _combo_bid_size;

    public static SINK_CONTROL getInstance() {
        return ourInstance;
    }


    public void config(int _combo_bid_size) {
        this._combo_bid_size = _combo_bid_size;//it must be one for LAL, LWM, and PAT.
    }


    //return the starting point of counter.
    public int GetAndUpdate() {
        counterLock.lock();
        int rt = counter;
        // Always good practice to enclose locks in a try-finally block
        try {
            counter += _combo_bid_size;//increment counter by combo_bid_size times...
        } finally {
            counterLock.unlock();
        }
        return rt;
    }


}
