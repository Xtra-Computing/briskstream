package brisk.queue.impl.wait;

import brisk.queue.impl.PaddedLong;

import java.util.concurrent.atomic.AtomicLong;

public final class SleepWaitStrategy implements WaitStrategy {
    private static final int SPIN_TRIES = 0;
    private static final int sleep_ns = 0;
    private static final int sleep_ms = 10;

    private boolean _test_offer(final AtomicLong tail, final AtomicLong head, final int capacity, final PaddedLong headCache) {
        final long currentTail = tail.get();
        final long wrapPoint = currentTail - capacity;
        if (headCache.value <= wrapPoint) {
            headCache.value = head.get();//producer will experience a cache miss here.
            return headCache.value > wrapPoint;
        }
        return true;
    }

    @Override
    public int waitFor(final AtomicLong tail, final AtomicLong head, final int capacity, final PaddedLong headCache) throws InterruptedException {
        int counter = SPIN_TRIES;//try SPIN_TRIES more times, and then go to wait method.
        int sleep_times = 0;
        while (!_test_offer(tail, head, capacity, headCache)) {
            counter = applyWaitMethod(counter);
            sleep_times++;
        }
        return (sleep_times - SPIN_TRIES) * (sleep_ns + 1000 * sleep_ms);
    }

    private int applyWaitMethod(int counter) throws InterruptedException {

        if (0 == counter) {
            Thread.sleep(sleep_ms, sleep_ns);
        } else {
            --counter;
        }

        return counter;
    }


    @Override
    public void signalAllWhenBlocking() {

    }
}