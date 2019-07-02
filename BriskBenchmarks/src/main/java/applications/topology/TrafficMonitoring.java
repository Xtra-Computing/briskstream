package applications.topology;

import applications.bolts.comm.GeneralParserBolt;
import applications.bolts.tm.MapMatchingBolt;
import applications.bolts.tm.SpeedCalculatorBolt;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.TrafficMonitoringConstants.*;

/**
 * https://github.com/whughchen/RealTimeTraffic
 *
 * @author Chen Guanghua <whughchen@gmail.com>
 */
public class TrafficMonitoring extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);
    private final int mapMatcherThreads;
    private final int speedCalcThreads;

    public TrafficMonitoring(String topologyName, Configuration config) {
        super(topologyName, config);
        initilize_parser();
        mapMatcherThreads = config.getInt(Conf.MAP_MATCHER_THREADS, 1);
        speedCalcThreads = config.getInt(Conf.SPEED_CALCULATOR_THREADS, 1);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
        // sink.loops = 20;//TM is too slow. TODO: disabled for upper bound test purpose
    }

    @Override
    public Topology buildTopology() {
        int batch = config.getInt("batch");

        try {
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);

            builder.setBolt(Component.PARSER, new GeneralParserBolt(parser,
                            new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                                    Field.BEARING, Field.LATITUDE, Field.LONGITUDE, Field.ROAD_ID))
                    , config.getInt(Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

//            builder.setBolt(Component.FORWARD, new ForwardBolt(), forwardThreads,
//                    new ShuffleGrouping(Component.PARSER));

            builder.setBolt(Component.MAP_MATCHER, new MapMatchingBolt(), mapMatcherThreads, //
                    new ShuffleGrouping(Component.PARSER));

            builder.setBolt(Component.SPEED_CALCULATOR, new SpeedCalculatorBolt(), speedCalcThreads,
//                    new ShuffleGrouping(Component.PARSER));
                    new FieldsGrouping(Component.MAP_MATCHER, new Fields(Field.ROAD_ID)));

            builder.setSink(Component.SINK, sink, sinkThreads,
                    new ShuffleGrouping(Component.SPEED_CALCULATOR));

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
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
