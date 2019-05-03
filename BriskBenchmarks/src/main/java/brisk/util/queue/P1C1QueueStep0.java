package brisk.util.queue;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * <ul>
 * <li>lock_ratio free, observing partition writer principal.
 * </ul>
 */
final class P1C1QueueStep0<E> implements Queue<E> {
    private final E[] buffer;

    private volatile long tail = 0;
    private volatile long head = 0;

    @SuppressWarnings("unchecked")
    public P1C1QueueStep0(final int capacity) {
        buffer = (E[]) new Object[capacity];
    }

    public boolean add(final E e) {
        if (offer(e)) {
            return true;
        }

        throw new IllegalStateException("MyQueue is full");
    }

    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }

        final long currentTail = tail;
        final long wrapPoint = currentTail - buffer.length;
        if (head <= wrapPoint) {
            return false;
        }

        buffer[(int) (currentTail % buffer.length)] = e;
        tail = currentTail + 1;

        return true;
    }

    public E poll() {
        final long currentHead = head;
        if (currentHead >= tail) {
            return null;
        }

        final int index = (int) (currentHead % buffer.length);
        final E e = buffer[index];
        buffer[index] = null;
        head = currentHead + 1;

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
        return buffer[(int) (head % buffer.length)];
    }

    public int size() {
        return (int) (tail - head);
    }

    public boolean isEmpty() {
        return tail == head;
    }

    public boolean contains(final Object o) {
        if (null == o) {
            return false;
        }

        for (long i = head, limit = tail; i < limit; i++) {
            final E e = buffer[(int) (i % buffer.length)];
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
}