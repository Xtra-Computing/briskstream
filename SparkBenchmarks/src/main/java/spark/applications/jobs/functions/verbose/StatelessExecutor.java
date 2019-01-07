package spark.applications.jobs.functions.verbose;

import applications.common.bolt.helper.tasks.partial_stateful_task_sparkEdition;
import applications.common.bolt.helper.tasks.stateless_task;
import applications.common.bolt.helper.tasks.stateless_taskImpl;
import applications.common.spout.helper.Event;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.applications.util.data.Event_MB;

import java.util.Collections;
import java.util.Iterator;

import static applications.common.spout.helper.Event.null_expression;

/**
 * Created by I309939 on 5/3/2016.
 */
public class StatelessExecutor implements PairFlatMapFunction<Event_MB, String, Event_MB> {
    private static final long serialVersionUID = -2040804835188634635L;
    private static final Logger LOG = LoggerFactory.getLogger(StatelessExecutor.class);
    private final int task_type;
    private final int window;
    private final int batch_duration;
    private int in_core_complexity;
    private int off_core_complexity;
    private stateless_task mystatelesstask = null;
    private partial_stateful_task_sparkEdition mystatefultask;//this is used when batch duration == application windows.

    public StatelessExecutor(SparkConf config) {
        in_core_complexity = config.getInt("I_C", 0);
        off_core_complexity = config.getInt("O_C", 0);
        mystatelesstask = new stateless_taskImpl(in_core_complexity, off_core_complexity);
        task_type = config.getInt("task_type", 0);
        batch_duration = config.getInt("batch_duration", 1000);//in ms
        window = config.getInt("window", 1);//in s
        if (task_type == 1 && window * 1000 == batch_duration)
            mystatefultask = new partial_stateful_task_sparkEdition(config);
    }


    @Override
    public Iterator<Tuple2<String, Event_MB>> call(Event_MB event) throws Exception {

        /**
         * We use event time for window computation.
         */
        final Long event_timestamp = event._1();
        final Long receive_timestamp = event._2();
        final String key = event._3();
        final String value = event._4();

        if (value.equals(null_expression)) {
            mystatelesstask.execute(String.valueOf(0));
            if (mystatefultask != null) {
                mystatefultask.execute(event_timestamp, key, String.valueOf(0));
            }
            return Collections.singletonList(new Tuple2<>(key, new Event_MB(event_timestamp, receive_timestamp, key, null_expression))).iterator();
        } else {
            long function_process_start = System.nanoTime();
            final String stateless_time = String.valueOf(mystatelesstask.execute(String.valueOf(function_process_start)));
            String state;

            if (mystatefultask != null) {
                final long stateful_process_start = System.nanoTime();
                final String stateful_time = String.valueOf(mystatefultask.execute(event_timestamp, key, String.valueOf(stateful_process_start)));
                state = value + Event.split_expression + "stateless" + Event.split_expression + stateless_time + Event.split_expression + "stateful" + Event.split_expression + stateful_time;
            } else
                state = value + Event.split_expression + "stateless" + Event.split_expression + stateless_time;
            return Collections.singletonList(new Tuple2<>(key, new Event_MB(event_timestamp, receive_timestamp, key, state))).iterator();
        }
    }
}
