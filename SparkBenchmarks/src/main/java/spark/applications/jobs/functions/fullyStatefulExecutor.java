package spark.applications.jobs.functions;

import applications.common.bolt.helper.tasks.fully_stateful_task;
import applications.common.tools.KB_object;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import scala.Tuple2;
import spark.applications.util.data.Event_MB;

import java.util.ArrayList;

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
        final String value = event._4();
        mytask.setMap(read_state(_fully_map));
        mytask.execute(event_timestamp, key, value);
        _fully_map.update(mytask.getMap());
        return new Tuple2<>(key, new Event_MB(event_timestamp, receive_timestamp, key, value));
    }

}
