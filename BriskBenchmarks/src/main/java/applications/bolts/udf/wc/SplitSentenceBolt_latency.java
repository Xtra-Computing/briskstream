package applications.bolts.udf.wc;

import brisk.components.context.TopologyContext;
import brisk.components.operators.base.splitBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import constants.BaseConstants;
import constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import util.OsUtils;

import java.util.HashMap;

//import static Brisk.applications.utils.Utils.printAddresses;

public class SplitSentenceBolt_latency extends splitBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt_latency.class);
    private static final long serialVersionUID = 8089145995668583749L;
//	long end = 0;
//	boolean update = false;
//	int loop = 1;
//	private int curr = 0, precurr = 0;
//	private int dummy = 0;

    public SplitSentenceBolt_latency() {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 10.0);
        OsUtils.configLOG(LOG);
    }

    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        return numNodes;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
//		LOG.info("PID  = " + pid);
    }


    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//not in use
    }

    public void execute(TransferTuple in) throws InterruptedException {
//		final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {

            long msgID = in.getLong(1, i);
            long TimeStamp;
//			if (msgID != -1) {
            TimeStamp = in.getLong(2, i);
//			} else {
//				TimeStamp = 0;
//			}

            char[] value = in.getCharArray(0, i);
//			LinkedList<char[]> ll = new LinkedList<>();
            int index = 0;
            int length = value.length;
            for (int c = 0; c < length; c++) {
                if (value[c] == ',' || c == length - 1) {//double measure_end.
                    int len = c - index;
                    char[] word = new char[len];
                    System.arraycopy(value, index, word, 0, len);
                    collector.emit_nowait(word, msgID, TimeStamp);
//					ll.add(word);
                    index = c + 1;
                }
            }
        }
    }
}
