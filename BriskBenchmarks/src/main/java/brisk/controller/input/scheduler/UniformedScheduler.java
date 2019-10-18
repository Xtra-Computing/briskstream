package brisk.controller.input.scheduler;

import brisk.controller.input.InputStreamController;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.optimization.model.STAT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.CompactHashMap.QuickHashMap;

import java.util.Arrays;

/**
 * TODO: The over head is still too large.
 * Created by shuhaozhang on 17/7/16.
 */
public class UniformedScheduler extends InputStreamController {
    private static final Logger LOG = LoggerFactory.getLogger(UniformedScheduler.class);
    private static final long serialVersionUID = -8233684569637244620L;
    private final QuickHashMap<String, Integer[]> queues = new QuickHashMap<>();
    private String[] streams;
    private int stream_index = 0;
    private int queue_index = 0;

    public void initialize() {
        super.initialize();
        this.streams = Arrays.copyOf(getRQ().keySet().toArray(), getRQ().keySet().size(), String[].class);
        for (String streamId : getRQ().keySet()) {
            queues.put(streamId, Arrays.copyOf(getRQ().get(streamId).keySet().toArray(), getRQ().get(streamId).size(), Integer[].class));
        }
    }


    @Override
    public TransferTuple fetchResults_inorder() {
        //        TransferTuple[] t = new TransferTuple[batch];
        for (int i = stream_index++; i < streams.length + stream_index; i++) {
            String streamId = streams[i % streams.length];
            //assert RQ != null;
            //final HashMap<Integer, P1C1Queue<TransferTuple>> integerP1C1QueueHashMap = RQ.get(streamId);
            Integer[] qids = queues.get(streamId);
            int queueIdLength = qids.length;
            for (int j = queue_index++; j < queueIdLength + queue_index; ) {
                //Integer[] queueId = Arrays.copyOf(integerP1C1QueueHashMap.keySet().toArray(),integerP1C1QueueHashMap.fieldSize(),Integer[].class);
                int q_index = qids[j % queueIdLength];
                if (queueIdLength > 1)
                    //LOG.DEBUG("Uniformed shoulder, queue index:" + q_index);

//                for (int b = 0; b < batch; b++) {
//                    t[b] = fetchFromqueue((P1C1Queue) getRQ().get(streamId).get(q_index));
//                }
                    return fetchFromqueue_inorder(getRQ().get(streamId).get(q_index));
            }
        }
        return null;
    }

    @Override
    public TransferTuple fetchResults() {
//        TransferTuple[] t = new TransferTuple[batch];
        for (int i = stream_index++; i < streams.length + stream_index; i++) {
            String streamId = streams[i % streams.length];
            //assert RQ != null;
            //final HashMap<Integer, P1C1Queue<TransferTuple>> integerP1C1QueueHashMap = RQ.get(streamId);
            Integer[] qids = queues.get(streamId);
            int queueIdLength = qids.length;
            for (int j = queue_index++; j < queueIdLength + queue_index; ) {
                //Integer[] queueId = Arrays.copyOf(integerP1C1QueueHashMap.keySet().toArray(),integerP1C1QueueHashMap.fieldSize(),Integer[].class);
                int q_index = qids[j % queueIdLength];
                if (queueIdLength > 1)
                    //LOG.DEBUG("Uniformed shoulder, queue index:" + q_index);

//                for (int b = 0; b < batch; b++) {
//                    t[b] = fetchFromqueue((P1C1Queue) getRQ().get(streamId).get(q_index));
//                }
                    return fetchFromqueue(getRQ().get(streamId).get(q_index));
            }
        }
        return null;
    }

    @Override
    public Tuple fetchResults_single() {

        throw new UnsupportedOperationException();
    }

    @Override
    public TransferTuple fetchResults(STAT stat, int batch) {
        return null;
    }
}
