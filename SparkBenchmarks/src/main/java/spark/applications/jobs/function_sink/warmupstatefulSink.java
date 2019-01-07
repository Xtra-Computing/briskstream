package spark.applications.jobs.function_sink;


import applications.common.sink.helper.warmup_sink_helper;
import com.google.common.base.Optional;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import scala.Tuple2;

/**
 * Created by I309939 on 7/21/2016.
 */
public class warmupstatefulSink implements Function3<String, Optional<Integer>, State<warmup_sink_helper>, Tuple2<String, Integer>> {
    private static final Logger LOG = LogManager.getLogger(warmupstatefulSink.class);

    @Override
    public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<warmup_sink_helper> state) {
        final warmup_sink_helper sink_state = state.get();
        if (sink_state.execute()) {
            LOG.info("Finished Brisk.execution");
            System.exit(0);
        }
        state.update(new warmup_sink_helper(sink_state));
        return new Tuple2<>(null, null);
    }
}
