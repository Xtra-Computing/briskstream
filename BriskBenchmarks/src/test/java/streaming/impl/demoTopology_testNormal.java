package streaming.impl;


import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streaming.impl.normal.bolt;
import streaming.impl.normal.sink;
import streaming.impl.normal.spout;

/**
 * Created by shuhaozhang on 13/7/16.
 */
public class demoTopology_testNormal {
    private final static Logger LOG = LoggerFactory.getLogger(demoTopology_testNormal.class);
    private Topology topo;

    public demoTopology_testNormal() {
        TopologyBuilder tb = new TopologyBuilder();
        try {
            tb.setSpout("randomWord", new spout(), 1);
            tb.setBolt("shuffleIdentity", new bolt(), 1
                    , new ShuffleGrouping("randomWord")
            );
            tb.setSink("sink", new sink(), 1

                    , new ShuffleGrouping("shuffleIdentity")
            );
            tb.setGlobalScheduler(new SequentialScheduler());
            this.topo = tb.createTopology();
        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
    }

    public Topology getTopo() {
        if (topo.getScheduler() == null) {
            LOG.info("TupleImpl scheduler is not set, use default global tuple scheduler instead!");
            topo.setScheduler(new SequentialScheduler());
        }
        return topo;
    }

}
