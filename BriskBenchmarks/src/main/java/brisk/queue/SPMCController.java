package brisk.queue;

import applications.util.OsUtils;
import brisk.execution.ExecutionNode;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Queue;

/**
 * Created by shuhaozhang on 11/7/16.
 * There's one PC per pair of "downstream, downstream operator".
 * PC is owned by streamController, which is owned by each executor.
 */
public class SPMCController extends QueueController {
    private static final Logger LOG = LoggerFactory.getLogger(SPMCController.class);
    private static final long serialVersionUID = 6892681774173050027L;
    private Queue outputQueue;//<Downstream executor ID, corresponding output queue>

    /**
     * This is where partition ratio is being updated.
     *
     * @param downExecutor_list
     */
    public SPMCController(HashMap<Integer, ExecutionNode> downExecutor_list) {
        super(downExecutor_list);
    }


    public boolean isEmpty() {
        return outputQueue.isEmpty();
    }

    /**
     * Allocate memory for queue structure here.
     *
     * @param linked
     * @param desired_elements_epoch_per_core
     */
    public void allocate_queue(boolean linked, int desired_elements_epoch_per_core) {


        //clean_executorInformation the queue if it exist

        if (outputQueue != null) {
//                if(queue instanceof P1C1Queue)
            LOG.info("relax_reset the old queue");
            outputQueue.clear();
            System.gc();
        }

        if (OsUtils.isWindows()) {
            outputQueue = new SpmcArrayQueue(1024);
        } else if (OsUtils.isMac()) {
            outputQueue = new SpmcArrayQueue(1024);
        } else {

            if (linked) {
                LOG.info("There is no linked implementation for SPMC queue.");
                System.exit(-1);
            } else {
                outputQueue = new SpmcArrayQueue((int) Math.pow(2, 18));
            }
        }
    }

    public Queue get_queue(int executor) {
        return outputQueue;
    }

}

