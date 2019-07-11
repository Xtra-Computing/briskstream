package applications.traj.topology;


import applications.traj.bolts.CompressorBolt;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.TrajConstants.*;
import static applications.constants.TrajConstants.Conf.COMPRESSOR_THREADS;

public class TrajCompress extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TrajCompress.class);

    public TrajCompress(String topologyName, Configuration config) {
        super(topologyName, config);
//        initilize_parser();
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);
            builder.setBolt(Component.COMPRESSOR, new CompressorBolt()
                    , config.getInt(COMPRESSOR_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.COMPRESSOR));

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
