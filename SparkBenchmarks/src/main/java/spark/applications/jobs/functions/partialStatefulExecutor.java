package spark.applications.jobs.functions;

import applications.common.bolt.helper.tasks.partial_stateful_task;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import scala.Tuple2;
import spark.applications.util.data.Event_MB;

import java.util.HashMap;

/**
 * Created by I309939 on 8/2/2016.
 */
public class partialStatefulExecutor implements Function3<String, Optional<Event_MB>,
        State<HashMap<String, String>>, Tuple2<String, Event_MB>> {

    private final int window;
    private final int in_core_complexity;
    private partial_stateful_task mytask;

    public partialStatefulExecutor(SparkConf config) {
        window = config.getInt("window", 1);//in s
        in_core_complexity = config.getInt("I_C", 1);
        mytask = new partial_stateful_task(in_core_complexity, window);
    }

    private HashMap<String, String> read_state(State<HashMap<String, String>> _partial_map) {
        final HashMap<String, String> partial_map;
        if (_partial_map.exists())
            partial_map = _partial_map.get();
        else
            partial_map = new HashMap<>();

        return partial_map;
    }

    @Override
    public Tuple2<String, Event_MB> call(String _key, Optional<Event_MB> _event, State<HashMap<String, String>> _partial_map) throws Exception {
        final Event_MB event = _event.get();
        final Long event_timestamp = event._1();
        final Long receive_timestamp = event._2();
        final String key = _key;
        final String value = event._4();
        mytask.setMap(read_state(_partial_map));
        mytask.execute(event_timestamp, key, value);
        _partial_map.update(mytask.getPartial_map());
        return new Tuple2<>(key, new Event_MB(event_timestamp, receive_timestamp, key, value));

    }
}
