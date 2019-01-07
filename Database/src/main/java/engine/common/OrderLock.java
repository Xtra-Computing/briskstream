package engine.common;

import applications.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Order lock should be globally shared.
 */
public class OrderLock implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(OrderLock.class);
    private static final long serialVersionUID = 1347267778748318967L;
    private static OrderLock ourInstance = new OrderLock();

//	SpinLock spinlock_ = new SpinLock();
//	volatile int fid = 0;
volatile AtomicLong bid = new AtomicLong();
    //	private transient HashMap<Integer, HashMap<Integer, Boolean>> executors_ready;//<FID, ExecutorID, true/false>
    private int end_fid;

    private OrderLock() {
        OsUtils.configLOG(LOG);
    }

    public static OrderLock getInstance() {
        return ourInstance;
    }

//	public int getFID() {
//		return fid;
//	}

    public long getBID() {
        return bid.get();
    }

//	public synchronized void advanceFID() {
//		fid++;
//	}

//	public synchronized void try_fill_gap() {
//		bid.getAndIncrement();
////		fid = 0;
//	}

    public void setBID(long bid) {
        this.bid.set(bid);
    }

    protected void fill_gap(LinkedList<Long> gap) {
//		while (!gap.isEmpty()) {
//			try_fill_gap(gap.);
//			gap.getAndDecrement();
//		}
        for (int i = 0; i < gap.size(); i++) {
            Long g = gap.get(i);
            if (!try_fill_gap(g)) {
                return;
            }
        }
    }

    /**
     * fill the gap.
     *
     * @param g the gap immediately follows previous item.
     */
    public boolean try_fill_gap(Long g) {
        if (getBID() == g) {
            bid.incrementAndGet();//allow next batch to proceed.
            return true;
        }
        return false;
    }

    public boolean blocking_wait(final long bid) {

        while (!this.bid.compareAndSet(bid, bid)) {
            //not ready for this batch to proceed! Wait for previous batch to finish execution.
            if (Thread.currentThread().isInterrupted()) {
//				 throw new InterruptedException();
                return false;
            }
//			fill_gap(gap);
        }
        return true;
    }

    public void advance() {

//		try_fill_gap();
//		try {
//			Thread.sleep(10);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
        bid.incrementAndGet();//allow next batch to proceed.
//		//LOG.DEBUG(Thread.currentThread().getName() + " advance bid to: " + bid+ " @ "+ DateTime.now());
//		if (joinedOperators(txn_context)) {
////			advanceFID();//allow next operator to proceed.
//
//			if (txn_context.getFID() == end_fid) {
//
//			}
//			executors_ready_rest(txn_context);
//		}
    }

//	public void initial(HashMap<Integer, HashMap<Integer, Boolean>> map) {
//		executors_ready = map;
//	}
//
//	public void set_executor_ready(int fid, int task_id) {
//		executors_ready.get(fid).put(task_id, true);
//	}

//	/**
//	 * have received all tuples from source.
//	 *
//	 * @return
//	 */
//	private boolean all_executors_ready(int fid) {
//		return !(executors_ready.get(fid).containsValue(false));
//	}

//	public void setEnd_fid(int end_fid) {
//		this.end_fid = end_fid;
//	}

//	/**
//	 * If the fid corresponding executors all finished their execution.
//	 *
//	 * @param txnContext
//	 * @return
//	 */
//	private synchronized boolean joinedOperators(TxnContext txnContext) {
//		set_executor_ready(txnContext.getFID(), txnContext.getTaskID());
//		return all_executors_ready(txnContext.getFID());
//	}
//
//	private void executors_ready_rest(TxnContext txnContext) {
//		final HashMap<Integer, Boolean> map = executors_ready.get(txnContext.getFID());
//		for (int task_id : map.keySet()) {
//			map.put(task_id, false);
//		}
//	}


}
