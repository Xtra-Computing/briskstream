package spark.applications.jobs.functions.verbose;

import applications.common.bolt.helper.tasks.fully_stateful_task;
import applications.common.spout.helper.Event;
import applications.common.tools.KB_object;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import scala.Tuple2;
import spark.applications.util.data.Event_MB;

import java.util.ArrayList;

import static applications.common.spout.helper.Event.null_expression;

/**
 * Created by I309939 on 8/2/2016.
 */
public class fullyStatefulExecutor implements Function3<String, Optional<Event_MB>,
        State<ArrayList<KB_object>>, Tuple2<String, Event_MB>> {

    private final int size_state;
    private fully_stateful_task mytask;

    public fullyStatefulExecutor(SparkConf config) {
        size_state = config.getInt("size_state", 1);//in s
        int in_core_complexity = config.getInt("I_C", 1);
        mytask = new fully_stateful_task(in_core_complexity, size_state);
    }


    private ArrayList<KB_object> read_state(State<ArrayList<KB_object>> _fully_map) {
        final ArrayList<KB_object> fully_map;
        if (_fully_map.exists())
            fully_map = _fully_map.get();
        else
            fully_map = new ArrayList<>();

        return fully_map;
    }

    @Override
    public Tuple2<String, Event_MB> call(String _key, Optional<Event_MB> _event,
                                         State<ArrayList<KB_object>> _fully_map) throws Exception {
        final Event_MB event = _event.get();
        final Long event_timestamp = event._1();
        final Long receive_timestamp = event._2();
        final String key = _key;
        final String event_state = event._4();

        if (event_state.equals(null_expression)) {
            mytask.setMap(read_state(_fully_map));
            mytask.execute(event_timestamp, key, String.valueOf(0));
            _fully_map.update(mytask.getMap());
            return new Tuple2<>(key, new Event_MB(event_timestamp, receive_timestamp, key, null_expression));
        } else {

            final long read_state = System.nanoTime();
            mytask.setMap(read_state(_fully_map));
            final long read_state_finish = System.nanoTime();

            long stateful_process_start = System.nanoTime();
            mytask.execute(event_timestamp, key, String.valueOf(stateful_process_start));
            long stateful_finish_time = System.nanoTime();

            final long update_state = System.nanoTime();
            _fully_map.update(mytask.getMap());
            final long update_state_finish = System.nanoTime();

            String state = event_state + Event.split_expression
                    + "FF" + Event.split_expression + stateful_process_start + Event.split_expression + stateful_finish_time
                    + "read_state" + Event.split_expression + read_state + Event.split_expression + read_state_finish
                    + "update_state" + Event.split_expression + update_state + Event.split_expression + update_state_finish;
            return new Tuple2<>(key, new Event_MB(event_timestamp, receive_timestamp, key, state));
        }
    }
}
