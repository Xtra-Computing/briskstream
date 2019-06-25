package applications.bolts.traj;

import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import compress.Compressor;
import compress.UniformSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.TrajConstants.Field.POINT;

public class CompressorBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CompressorBolt.class);
    Compressor compressor = new UniformSampler();//implement different compression algorithm.

    public CompressorBolt() {
        super(LOG);
        this.setStateful();
    }

    /**
     * @param input contains a trajectory point.
     * @throws InterruptedException
     */
    @Override
    public void execute(Tuple input) throws InterruptedException {
    

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
        LOG.info("PID  = " + pid);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(POINT);
    }



}
