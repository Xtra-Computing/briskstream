package applications.bolts.sa;

import applications.constants.streamingAnalysisConstants.Field;
import applications.util.datatypes.StreamValues;
import brisk.components.context.TopologyContext;
import brisk.components.operators.base.filterBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.JumboTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WindowRank extends filterBolt {
    private static final String split_expression = ",";
    private static final Logger LOG = LoggerFactory.getLogger(WindowRank.class);
    private static final long serialVersionUID = -5810383996336191119L;
    private static final boolean DESC = false;
    public static boolean ASC = true;
    private final int size_tuple;
    private final int window_size;
    private final Map<String, Double> unsortMap = new HashMap<>();
    double cnt = 0;
    double cnt1 = 0;
    private SlidingWindow<String> map;

    private WindowRank(int size_tuple, int window) {
        super();//the branch selectivity is changing by the ``key".
        this.size_tuple = size_tuple;
        super.setWindow(window);
        this.window_size = window;
    }

    private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap) {

        // long start = System.nanoTime();
        List<Map.Entry<String, Double>> list = new LinkedList<>(unsortMap.entrySet());

        // Sorting the list based on values
        list.sort((o1, o2) -> {
            if (WindowRank.DESC) {
                return o1.getValue().compareTo(o2.getValue());
            } else {
                return o2.getValue().compareTo(o1.getValue());

            }
        });

        //long end = System.nanoTime();
        //LOG.info("Operational time in rank:" + (end - start));

        // Maintaining insertion order with the help of LinkedList
        Map<String, Double> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    public static void printMap(Map<String, Integer> map) {
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        map = new SlidingWindow<>(this.window_size);
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
     * @param time
     * @param value
     */
    private void update_window(Long time, String value) {
        map.put(value);
    }

    private double sumInValue(String value) {
        double sum = 0;
        String[] split = value.split(split_expression);
        for (String item : split) {
            sum += Double.parseDouble(item);
        }
        return sum;
    }

    //report top-N by sum of values.
    //currently top-1 only.
    private Map.Entry<String, Double> rank() {

        for (int i = 0; i < this.window_size; i++) {
            final String value = map.get(i);
            unsortMap.put(value, sumInValue(value));
        }


        Map<String, Double> sortedMapDesc = sortByComparator(unsortMap);
        unsortMap.clear();
        return sortedMapDesc.entrySet().iterator().next();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {

        final Long time = in.getLong(0);
        final String key = in.getString(1);
        final String value = in.getString(2);
        final long bid = in.getBID();
//        cnt++;
        if (Integer.parseInt(key) <= size_tuple * branch_selectivity) {//
//            if (stat != null) stat.start_measure();
            update_window(time, value);
            if (map.build()) {
                String rank = rank().getKey();
                final StreamValues objects =
                        new StreamValues(time, key, rank);
                //StableValues.create(time, key, rank);//a memory write happens here (could be in cache..)
                collector.emit(bid, objects);
            }
//            cnt1++;
//            if (stat != null) stat.end_measure();
        }
//        double i = cnt1 / cnt;
//        assert i > 0;
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            final Long time = in.getLong(0, i);
            final String key = in.getString(1, i);
            final String value = in.getString(2, i);
            if (Integer.parseInt(key) <= size_tuple * branch_selectivity) {
                update_window(time, value);
                if (map.build()) {
                    String rank = rank().getKey();
                    final StreamValues objects =
                            new StreamValues(time, key, rank);

                    collector.emit(bid, objects);
                }

            }
        }
    }

}
