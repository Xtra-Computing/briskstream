package applications.general.bolts.sa;

import applications.constants.streamingAnalysisConstants.Field;
import applications.util.datatypes.StreamValues;
import brisk.components.context.TopologyContext;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.JumboTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;

public class WindowMedian extends MapBolt {
    private static final String split_expression = ",";
    private static final Logger LOG = LoggerFactory.getLogger(WindowMedian.class);
    private static final long serialVersionUID = 3686241835639674759L;
    private final int window_size;
    private SlidingWindow<String> map;

    private WindowMedian(int window) {
        super(LOG);
        super.setWindow(window);
        this.window_size = window;
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        map = new SlidingWindow<>(window_size);
    }


    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);

    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIME, Field.VALUE);
    }

    /**
     * Assume sliding window based.
     *
     * @param time  // /number of item/ based, this time is not needed.
     * @param value
     */
    private void update_window(Long time, String value) {
        map.put(value);
    }

    private double get_median() {
        LinkedList<Double> number_list = new LinkedList<>();

        for (int i = 0; i < window_size; i++) {
            final String item = map.get(i);
            String[] split = item.split(split_expression);
            for (String e : split) {
                number_list.add(Double.parseDouble(e));
            }
        }
        Object[] numArray = number_list.toArray();
        Arrays.sort(numArray);
        double median;
        if (numArray.length % 2 == 0) {
            median = ((double) numArray[numArray.length / 2] + (double) numArray[numArray.length / 2 - 1]) / 2;
        } else {
            median = (double) numArray[numArray.length / 2];
        }

        return median;
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
//       not in use.
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            //        if (stat != null) stat.start_measure();
            final Long time = in.getLong(0, i);//input_event time based window. not in discuss this time.
            final String key = in.getString(1, i);
            final String value = in.getString(2, i);
            update_window(time, value);
            if (map.build()) {//skip the first few items.
                double median = get_median();
                final StreamValues objects =
                        new StreamValues(time, key, median);
                //StableValues.create(time, median);//a memory write happens here (could be in cache..)
                collector.emit(bid, objects);
            }
//        if (stat != null) stat.end_measure();
        }
    }
}
