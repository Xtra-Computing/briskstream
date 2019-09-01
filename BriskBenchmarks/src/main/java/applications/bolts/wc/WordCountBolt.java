package applications.bolts.wc;

import applications.constants.WordCountConstants.Field;
import applications.util.Configuration;
import applications.util.OsUtils;
import applications.util.datatypes.StreamValues;
import brisk.components.context.TopologyContext;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class WordCountBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt.class);
    private static final long serialVersionUID = -6454380680803776555L;
    //private int total_thread=context.getThisTaskId();
//    private static final String splitregex = " ";
//    private static LinkedList<String> logger = new LinkedList<String>();
//	private final Map<String, MutableLong> counts = new HashMap<>();
    private final Map<Integer, Long> counts = new HashMap<>();//what if memory is not enough to hold counts?

    public WordCountBolt() {
        super(LOG);
        this.setStateful();
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 80;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
//		LOG.info("PID  = " + pid);

    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        char[] word = input.getCharArray(0);
        int key = Arrays.hashCode(word);
        long v = counts.getOrDefault(key, 0L);
        if (v == 0) {
            counts.put(key, 1L);
            collector.force_emit(0, new StreamValues(word, 1L));
        } else {
            long value = v + 1L;
            counts.put(key, value);
            collector.force_emit(0, new StreamValues(word, value));
        }
    }

    /**
     * @param input
     * @throws InterruptedException
     */
    @Override
    public void execute(TransferTuple input) throws InterruptedException {
        int bound = input.length;
        for (int i = 0; i < bound; i++) {
            char[] word = input.getCharArray(0, i);
            int key = Arrays.hashCode(word);
            long v = counts.getOrDefault(key, 0L);
            if (v == 0) {
                counts.put(key, 1L);
                collector.emit(word, 1L);
            } else {
                long value = v + 1L;
                counts.put(key, value);
                collector.emit(word, value);
            }
        }
    }

    @Override
    public void profile_execute(TransferTuple in) {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] word = in.getCharArray(0, i);
            int key = Arrays.hashCode(word);
            long v = counts.getOrDefault(key, 0L);
            if (v == 0) {
                counts.put(key, 1L);
                collector.emit_nowait(word, 1L);
            } else {
                long value = v + 1L;
                counts.put(key, value);
                collector.emit_nowait(word, value);
            }
        }
    }

    public void display() {
//        double size_state;
//		if (OsUtils.isUnix()) {
//			size_state = MemoryUtil.deepMemoryUsageOf(counts, MemoryUtil.VisibilityFilter.ALL);
//		} else {
//        size_state = counts.size();
//		}
//
//        LOG.info("Num of Tasks:" + this.getContext().getNUMTasks() + ", State size: " + size_state);

//		for (Map.Entry<String, MutableLong> entry : counts.entrySet()) {
//			System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
//		}
    }

}
