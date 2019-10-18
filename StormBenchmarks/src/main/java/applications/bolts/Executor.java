package applications.bolts;

import tasks.fully_stateful_task;
import tasks.partial_stateful_task;
import tasks.stateful_task;
import tasks.stateless_taskImpl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Executor extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
    protected tasks.Executor executor;

    public Executor() {
    }


    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        int task_type = config.getInt("task_type");
        int in_core_complexity = config.getInt("I_C");
        int off_core_complexity = config.getInt("O_C");
        int window = config.getInt("window");
        int size_state = config.getInt("size_state");

        stateless_taskImpl mystatelesstask = new stateless_taskImpl(in_core_complexity, off_core_complexity);

        stateful_task mytask;
        if (task_type == 1) {//partial stateful
            mytask = new partial_stateful_task(in_core_complexity, window);
        } else if (task_type == 2) {//fully stateful
            mytask = new fully_stateful_task(in_core_complexity, size_state);
        } else {
            mytask = null;
        }
        executor = new tasks.Executor(mystatelesstask, mytask);
        LOG.info("I'm running with take type:" + task_type);
    }

    @Override
    public Fields getDefaultFields() {
        //return new Fields(microbenchmarkConstants.Field.TIME, microbenchmarkConstants.Field.TEXT, microbenchmarkConstants.Field.STATE);
        return null;
    }


    @Override
    public void execute(Tuple input) {
        final Long time = input.getLong(0);
        final String key = input.getString(1);
        final String value = input.getString(2);
        executor.execute(time, key, value);
        collector.emit(new Values(time, key, value));
    }
}
