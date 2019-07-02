package applications.topology.transactional;


import applications.bolts.acep.EsperBolt;
import applications.constants.GrepSumConstants.Component;
import applications.topology.transactional.initializer.MBInitializer;
import applications.topology.transactional.initializer.TableInitilizer;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.topology.TransactionTopology;
import engine.common.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static applications.constants.GrepSumConstants.Conf.INSERTOR_THREADS;
import static applications.constants.GrepSumConstants.PREFIX;


public class ACEP extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ACEP.class);
    private static final String LITERAL_SYMBOL = "symbol";
    private static final String LITERAL_PRICE = "price";
    private static final String LITERAL_RETURN_OBJ = "Result";
    private static final String LITERAL_AVG = "avg";
    private static final String LITERAL_ESPER = "esper";
    private static final String LITERAL_QUOTES = "quotes";

    public ACEP(String topologyName, Configuration config) {
        super(topologyName, config);

    }

    public static String getPrefix() {
        return PREFIX;
    }

    static int GenerateInteger(final int min, final int max) {
        Random r = new Random();
        return r.nextInt(max) + min;
    }

    public TableInitilizer initializeDB(SpinLock[] spinlock) {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");

        TableInitilizer ini = new MBInitializer(db, scale_factor, theta, tthread, config);
        ini.creates_Table(config);

        ini.loadData_Central(scale_factor, theta);
        return ini;
    }

    @Override
    public Topology buildTopology() {
        try {

            //should say fieldsTypes, maybe with object/component prefix
            Map<String, Object> eventTypes = new HashMap<>();
            eventTypes.put(LITERAL_SYMBOL, String.class);
            eventTypes.put(LITERAL_PRICE, Integer.class);


            builder.setSpout(Component.SPOUT, spout, spoutThreads);


            switch (config.getInt("CCOption", 0)) {

                case 0: {//no-order

                    break;
                }

                case 1: {//LOB

                    break;
                }

                case 2: {//LWM

                    break;
                }

                case 3: {//T-Stream
                    builder.setBolt(LITERAL_ESPER, new EsperBolt(1)
                                    .addEventTypes(eventTypes)
                                    .addOutputTypes(Collections.singletonMap(LITERAL_RETURN_OBJ, Arrays.asList(LITERAL_AVG, LITERAL_PRICE)))
                                    .addStatements(Collections.singleton("put into Result "
                                            + "select avg(price) as avg, price from "
                                            + "quotes_default(symbol='A').win:length(2) "
                                            + "having avg(price) > 60.0"))
                            , config.getInt(INSERTOR_THREADS, 2)
                            , new ShuffleGrouping(LITERAL_QUOTES))
                    ;

//                    builder.setBolt(Component.INSERTOR, new WriteBolt_ts(0)//
//                            , config.getInt(INSERTOR_THREADS, 2)
//                            , new ShuffleGrouping(Component.SPOUT, "WriteStream"));
                    break;
                }

            }

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.SELECTOR)
//                    , new ShuffleGrouping(Component.INSERTOR)
            );

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
