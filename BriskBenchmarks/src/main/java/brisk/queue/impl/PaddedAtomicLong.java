package brisk.queue.impl;

import java.util.concurrent.atomic.AtomicLong;

class PaddedAtomicLong extends AtomicLong {
    private static final long serialVersionUID = 5826979353123727160L;
    public volatile long p1, p2, p3, p4, p5, p6 = 7;

    public PaddedAtomicLong() {
    }

    public PaddedAtomicLong(final long initialValue) {
        super(initialValue);
    }
}