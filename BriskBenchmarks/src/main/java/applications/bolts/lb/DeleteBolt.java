package applications.bolts.lb;

import applications.constants.WordCountConstants.Field;
import applications.util.OsUtils;
import applications.util.datatypes.StreamValues;
import brisk.components.context.TopologyContext;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class DeleteBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteBolt.class);
    private static final long serialVersionUID = -119787927095818575L;
    //private int total_thread=context.getThisTaskId();
//    private static final String splitregex = " ";
//    private static LinkedList<String> logger = new LinkedList<String>();
    private final Map<String, MutableLong> counts = new HashMap<>();
    //    long start = 0, end = 0, curr = 0;
    int loop = 1;

    public DeleteBolt() {
        super(LOG);
        int cnt = 0;
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        final long bid = in.getBID();
        String word = in.getStringByField(Field.WORD);
        MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
        count.increment();
        StreamValues value = new StreamValues(word, count.longValue());

        collector.emit(bid, value);
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {

            String word = in.getStringByField(Field.WORD, i);
            MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
            count.increment();
            StreamValues value = new StreamValues(word, count.longValue());
            collector.emit(bid, value);
        }
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
        LOG.info("PID  = " + pid);
    }
}
