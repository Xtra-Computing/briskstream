package applications.topology.transactional;


import applications.constants.S_STOREConstants.Component;
import applications.topology.transactional.initializer.MBInitializer;
import applications.topology.transactional.initializer.TableInitilizer;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.topology.TransactionTopology;
import engine.common.PartitionedOrderLock;
import engine.common.SpinLock;
import engine.profiler.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static applications.constants.S_STOREConstants.PREFIX;
import static utils.PartitionHelper.setPartition_interval;


public class S_STORE extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(S_STORE.class);

    public S_STORE(String topologyName, Configuration config) {
        super(topologyName, config);

    }

    public static String getPrefix() {
        return PREFIX;
    }

    static int GenerateInteger(final int min, final int max) {
        Random r = new Random();
        return r.nextInt(max) + min;
    }

    /**
     * Load Data Later by Executors.
     *
     * @param spinlock_
     * @return TableInitilizer
     */
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {

        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        setPartition_interval((int) (Math.ceil(Metrics.NUM_ITEMS / (double) tthread)), tthread);

        TableInitilizer ini = new MBInitializer(db, scale_factor, theta, tthread, config);

        ini.creates_Table(config);

        if (config.getBoolean("partition", false)) {

            for (int i = 0; i < tthread; i++)
                spinlock_[i] = new SpinLock();

//            ini.loadDB(scale_factor, theta, getPartition_interval(), spinlock_);

            //initilize order locks.
            PartitionedOrderLock.getInstance().initilize(tthread);
        } else {
//            ini.loadDB(scale_factor, theta);
        }

        return ini;
    }

    @Override
    public Topology buildTopology() {
        try {

            builder.setSpout(Component.SPOUT, spout, spoutThreads);

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology(db, this);
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
