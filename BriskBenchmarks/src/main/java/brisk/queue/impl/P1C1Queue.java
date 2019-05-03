package brisk.queue.impl;

/*
 * Copyright 2012 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import applications.util.OsUtils;
import brisk.queue.impl.wait.SleepWaitStrategy;
import brisk.queue.impl.wait.WaitStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <ul>
 * <li>lock_ratio free, observing partition writer principal.
 * <li>Replacing the long fields with AtomicLong and using lazySet instead of
 * volatile assignment.
 * <li>Using the power of 2 mask, forcing the capacity to next power of 2.
 * <li>Adding head and tail cache fields. Avoiding redundant volatile reads.
 * <li>Padding head/tail AtomicLong fields. Avoiding false sharing.
 * <li>Padding head/tail cache fields. Avoiding false sharing.
 * </ul>
 */
public final class P1C1Queue<E> implements Queue<E> {
    private static final Logger LOG = LoggerFactory.getLogger(P1C1Queue.class);
    private final int capacity;
    private final int mask;
    private final E[] buffer;

    private final AtomicLong tail = new PaddedAtomicLong(0);
    private final AtomicLong head = new PaddedAtomicLong(0);
    private final PaddedLong tailCache = new PaddedLong();
    private final PaddedLong headCache = new PaddedLong();
    private final WaitStrategy waitStrategy = new SleepWaitStrategy();
    private int cnt = 0;
    private boolean profile = false;

    @SuppressWarnings("unchecked")
    public P1C1Queue() {
//        this.context = context;
        if (OsUtils.isWindows()) {
            this.capacity = findNextPositivePowerOfTwo(1024);//1073741824
        } else {
            this.capacity = findNextPositivePowerOfTwo(1073740);//
        }

        mask = this.capacity - 1;
        buffer = (E[]) new Object[this.capacity];
        boolean ready = true;
    }

    private static int findNextPositivePowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    public boolean add(final E e) {
        if (offer(e)) {
            return true;
        }

        throw new IllegalStateException("MyQueue is full");
    }

    private boolean non_blocingOffer(final E e) {
//        cnt++;
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final long currentTail = tail.get();
        final long wrapPoint = currentTail - capacity;
        if (headCache.value <= wrapPoint) {//"MyQueue is full"
            headCache.value = head.get();
            if (headCache.value <= wrapPoint) {
//                LOG.info("MyQueue is full!! I have offered:" + cnt + "times, and the capacity is only:" + this.capacity);
                return false;//simply drop the tuple.
            }
        }
        buffer[(int) currentTail & mask] = e;
        tail.lazySet(currentTail + 1);
        return true;
    }

    /**
     * blocking offer.
     *
     * @param e
     * @return
     * @throws InterruptedException
     */
    private boolean _offer(final E e) throws InterruptedException {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final long currentTail = tail.get();
        final long wrapPoint = currentTail - capacity;
        if (headCache.value <= wrapPoint) {
            headCache.value = head.get();//producer will experience a cache miss here.
            if (headCache.value <= wrapPoint) {//"MyQueue is full"
//                LOG.info("MyQueue is full!! I have offered:" + cnt + "times, and the capacity is only:" + this.capacity);
                waitStrategy.waitFor(tail, head, capacity, headCache);
            }
        }
        buffer[(int) currentTail & mask] = e;
        tail.lazySet(currentTail + 1);

        return true;
    }

    public int offer_record(final E e) {
        int record = 0;
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final long currentTail = tail.get();
        final long wrapPoint = currentTail - capacity;
        if (headCache.value <= wrapPoint) {
            headCache.value = head.get();//producer will experience a cache miss here.
            if (headCache.value <= wrapPoint) {//"MyQueue is full"
                try {
                    record = waitStrategy.waitFor(tail, head, capacity, headCache);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        buffer[(int) currentTail & mask] = e;
        tail.lazySet(currentTail + 1);
        return record;

    }

    /**
     * Implement new queue full sync_ratio strategy. By default, non-blocking.
     *
     * @param e
     * @return
     */
    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        try {
            if (!profile) {
//                LOG.info("non_blocking");
                return non_blocingOffer(e);//disable queue full effect in profiling phase.
            } else {
                return _offer(e);
            }
        } catch (InterruptedException e1) {
            LOG.info("Interrupted, thread stop");
            return false;
        }
        //return true;
    }

    public boolean offer_nowait(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        //if(context.profile) {
        //LOG.info("non_blocking");
        // return non_blocingOffer(e);//disable queue full effect in profiling phase.
        return non_blocingOffer(e);
        //return true;
    }

    //head pointer owns by consumer
    public E poll() {
        final long currentHead = head.get();
        if (currentHead >= tailCache.value) {
            tailCache.value = tail.get();//consumer will experience a cache miss here.
            if (currentHead >= tailCache.value) {
                return null;
            }
        }

        final int index = (int) currentHead & mask;
        final E e = buffer[index];
        buffer[index] = null;//relax_reset it
        head.lazySet(currentHead + 1);

        return e;
    }

    public E remove() {
        final E e = poll();
        if (null == e) {
            throw new NoSuchElementException("MyQueue is empty");
        }

        return e;
    }

    public E element() {
        final E e = peek();
        if (null == e) {
            throw new NoSuchElementException("MyQueue is empty");
        }

        return e;
    }

    public E peek() {
        return buffer[(int) head.get() & mask];
    }

    public int size() {
        return (int) (tail.get() - head.get());
    }

    public boolean isEmpty() {
        return tail.get() == head.get();
    }

    public boolean contains(final Object o) {
        if (null == o) {
            return false;
        }

        for (long i = head.get(), limit = tail.get(); i < limit; i++) {
            final E e = buffer[(int) i & mask];
            if (o.equals(e)) {
                return true;
            }
        }

        return false;
    }

    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(final T[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(final Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(final Collection<?> c) {
        for (final Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    public boolean addAll(final Collection<? extends E> c) {
        for (final E e : c) {
            add(e);
        }

        return true;
    }

    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        Object value;
        do {
            value = poll();
        } while (null != value);
    }


    public boolean isProfile() {
        return profile;
    }

    public void setProfile(boolean profile) {
        this.profile = profile;
    }
}