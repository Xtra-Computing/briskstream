package applications.spout;

import applications.constants.BaseConstants.BaseConf;
import applications.constants.MicroBenchmarkConstants;
import applications.spout.helper.DataSource;
import brisk.components.operators.api.AbstractSpout;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStateSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStateSpout.class);
    private static final long serialVersionUID = -8358880222505243596L;
    //    private ReceiveParser parser;

    private LocalStateSpout() {
        super(LOG);
        setFields(new Fields(MicroBenchmarkConstants.Field.TIME, MicroBenchmarkConstants.Field.TEXT, MicroBenchmarkConstants.Field.STATE));
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int taskId = getContext().getThisTaskIndex();
        int numTasks = config.getInt(getConfigKey(BaseConf.SPOUT_THREADS));
        int skew = 0;
        int tuple_size = config.getInt("size_tuple");
        DataSource dataSource = new DataSource(skew, false, tuple_size, false);
//        parser = new ReceiveParser();
        LOG.info("Use localSpout now");
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void nextTuple() {

//        final MicroEvent event = dataSource.generateEvent();
//        final String msg = event.getEvent();
//        List<StreamValues> tuples = parser.parse(msg);
//        for (StreamValues values : tuples) {
//            collector.emit(values.getStreamId(), values);
//        }
    }


}
