package brisk.queue.impl.wait;

import brisk.queue.impl.PaddedLong;

import java.util.concurrent.atomic.AtomicLong;

public final class YieldingWaitStrategy implements WaitStrategy {
    private static final int SPIN_TRIES = 1;

    private boolean _test_offer(final AtomicLong tail, final AtomicLong head, final int capacity, final PaddedLong headCache) {
        final long currentTail = tail.get();
        final long wrapPoint = currentTail - capacity;
        if (headCache.value <= wrapPoint) {
            headCache.value = head.get();
            return headCache.value > wrapPoint;
        }
        return true;
    }

    @Override
    public int waitFor(final AtomicLong tail, final AtomicLong head, final int capacity, final PaddedLong headCache) {
        int counter = SPIN_TRIES;
        while (!_test_offer(tail, head, capacity, headCache)) {
            counter = applyWaitMethod(counter);
        }
        return 0;
    }

    private int applyWaitMethod(int counter) {

        if (0 == counter) {
            Thread.yield();
        } else {
            --counter;
        }

        return counter;
    }


    @Override
    public void signalAllWhenBlocking() {

    }
}