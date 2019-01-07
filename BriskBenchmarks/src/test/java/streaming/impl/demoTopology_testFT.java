package streaming.impl;


import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streaming.impl.ft.bolt_ft;
import streaming.impl.ft.sink_ft;
import streaming.impl.ft.spout_ft;

/**
 * Created by shuhaozhang on 13/7/16.
 */
public class demoTopology_testFT {
    private final static Logger LOG = LoggerFactory.getLogger(demoTopology_testFT.class);
    private Topology topo;

    public demoTopology_testFT() {
        TopologyBuilder tb = new TopologyBuilder();
        try {
            tb.setSpout("spout", new spout_ft(), 1);
            tb.setBolt("bolt", new bolt_ft(), 20
                    , new ShuffleGrouping("spout")
            );
            tb.setBolt("bolt2", new bolt_ft(), 20
                    , new ShuffleGrouping("bolt")
            );

            tb.setSink("sink", new sink_ft(), 10
                    , new ShuffleGrouping("bolt2")
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
