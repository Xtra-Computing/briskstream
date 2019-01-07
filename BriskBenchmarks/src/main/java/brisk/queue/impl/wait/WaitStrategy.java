package brisk.queue.impl.wait;


import brisk.queue.impl.PaddedLong;

import java.util.concurrent.atomic.AtomicLong;

public interface WaitStrategy {

    int waitFor(AtomicLong tail, AtomicLong head, int capacity, PaddedLong headCache) throws InterruptedException;

    void signalAllWhenBlocking();
}