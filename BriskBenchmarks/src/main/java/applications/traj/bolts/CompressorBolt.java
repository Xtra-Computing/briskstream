package applications.traj.bolts;

import applications.util.OsUtils;
import applications.util.datatypes.StreamValues;
import brisk.components.context.TopologyContext;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import compress.Compressor;
import compress.OPW;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import struct.Point;

import static applications.constants.TrajConstants.Field.POINT;

public class CompressorBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CompressorBolt.class);
    //    Compressor compressor = new UniformSampler(5);//implement different compression algorithm.
    Compressor compressor = new OPW(0.0001);//implement different compression algorithm.

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
        Point point = input.getPoint();
        Point result = compressor.compress(point);
        if (result != null)
            this.collector.emit(new StreamValues(result));
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
