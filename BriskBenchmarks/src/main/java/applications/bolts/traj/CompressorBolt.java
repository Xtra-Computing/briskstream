package applications.bolts.traj;

import applications.constants.WordCountConstants.Field;
import applications.util.Configuration;
import applications.util.OsUtils;
import applications.util.datatypes.StreamValues;
import brisk.components.context.TopologyContext;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.JumboTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import core.Compressor;
import core.UniformSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CompressorBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CompressorBolt.class);
    Compressor compressor = new UniformSampler();//implement different compression algorithm.

    public CompressorBolt() {
        super(LOG);
        this.setStateful();
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 80;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
        LOG.info("PID  = " + pid);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }


    /**
     *
     * @param input contains a trajectory point.
     * @throws InterruptedException
     */
    @Override
    public void execute(Tuple input) throws InterruptedException {


    }
}
