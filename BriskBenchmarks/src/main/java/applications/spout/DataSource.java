package applications.spout;

import brisk.components.operators.api.AbstractSpout;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Fields;
import constants.BaseConstants;
import constants.BaseConstants.BaseConf;
import constants.streamingAnalysisConstants;
import helper.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.datatypes.StreamValues;

import static Constants.DEFAULT_STREAM_ID;

public class DataSource extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);
    private static final long serialVersionUID = -2735379827145506668L;
    int count = 0;
    private helper.DataSource dataSource;
//    private ReceiveParser parser;

    private DataSource() {
        super(LOG);
        setFields(new Fields(streamingAnalysisConstants.Field.TIME, streamingAnalysisConstants.Field.VALUE));
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int taskId = getContext().getThisTaskIndex();
        int numTasks = config.getInt(getConfigKey(BaseConf.SPOUT_THREADS));
        int skew = 0;
        int cnt = 0;
        int tuple_size = config.getInt("size_tuple");
        LOG.info("TransferTuple fieldSize to emit:" + tuple_size);
        String Wrapper = config.getString(getConfigKey(BaseConstants.BaseConf.SPOUT_Wrapper));//config.getString("spout_parser");
        dataSource = new helper.DataSource(Wrapper, skew, false, tuple_size, false);
//        this.parser = new ReceiveParser();
//        Constants.application.SOURCE_RATE = loadTargetHz / 1000000000.0;
//        LOG.info("Use DataSource now");
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void nextTuple() throws InterruptedException {
//        if (cnt < queue_size) {
        final Event event = dataSource.generateEvent();
        final String msg = event.getEvent();
        final StreamValues objects =
                new StreamValues(msg);
        collector.emit_bid(DEFAULT_STREAM_ID, objects);
//        try {
//            Thread.sleep(1);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}
