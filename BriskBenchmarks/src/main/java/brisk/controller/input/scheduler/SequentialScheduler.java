package brisk.controller.input.scheduler;

import brisk.controller.input.InputStreamController;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.optimization.model.STAT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;


/**
 * Created by shuhaozhang on 17/7/16.
 * every executor owns its own input scheduler.
 */
public class SequentialScheduler extends InputStreamController {
    private static final Logger LOG = LoggerFactory.getLogger(SequentialScheduler.class);
    private static final long serialVersionUID = 5653765958500376011L;
    private final LinkedList<Queue> LQ = new LinkedList<>();

    /**
     * non-blocking fetch
     *
     * @return
     */
    private int size;
    private int current = 0;

    public void initialize() {
        super.initialize();
        for (String streamId : keySet) {
            LQ.addAll(getRQ().get(streamId).values());
        }
        size = LQ.size();
        current = 0;
        if (size == 0) {
            LOG.info("MyQueue initialize wrong");
            System.exit(-1);
        }
    }


    @Override
    public TransferTuple fetchResults_inorder() {
        if (current == size) {
            current = 0;
        }
        return fetchFromqueue_inorder(LQ.get(current++));


    }
//
//	@Override
//	public TransferTuple fetchResults() {
//		TransferTuple tuple = null;
//		int cnt = 0;
//		do {
//			if (current == size) {
//				current = 0;
//			}
//			tuple = fetchFromqueue(LQ.get(current++));
//			cnt++;
//		} while (tuple == null && cnt < size);
//
//		return tuple;//return a tuple or null after failed trying all queues.
//	}

    @Override
    public TransferTuple fetchResults() {
        if (current == size) {
            current = 0;
        }
        return fetchFromqueue(LQ.get(current++));
    }

    /**
     * @return
     */
    @Override
    public Tuple fetchResults_single() {
        if (current == size) {
            current = 0;
        }
        return fetchFromqueue_single(LQ.get(current++));
    }

    @Override
    public TransferTuple fetchResults(STAT stat, int batch) {
        if (current == size) {
            current = 0;
        }
        return fetchFromqueue(LQ.get(current++), stat, batch);
    }
}
