package applications.general.topology.transactional;

import applications.general.bolts.lb.VoteBolt;
import applications.constants.LeaderboardConstants;
import applications.constants.LeaderboardConstants.Component;
import applications.constants.LeaderboardConstants.Field;
import applications.constants.VoterSStoreExampleConstants;
import applications.general.topology.transactional.initializer.LBInitializer;
import applications.general.topology.transactional.initializer.TableInitilizer;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.TransactionTopology;
import engine.common.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.VoterSStoreExampleUtil;

import static applications.constants.LeaderboardConstants.PREFIX;

public class LeaderBoard extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderBoard.class);

    public LeaderBoard(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    /**
     * Return the scale factor for this benchmark instance
     *
     * @return
     */
    private double getScaleFactor() {
        return (config.getDouble("scale_factor"));
    }

    public TableInitilizer initializeDB(SpinLock[] spinlock) {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        TableInitilizer ini = new LBInitializer(db, scale_factor, theta, tthread, config);
        ini.creates_Table(config);
        int numContestants = VoterSStoreExampleUtil.getScaledNumContestants(this.getScaleFactor());
        ini.loadDB(numContestants, VoterSStoreExampleConstants.CONTESTANT_NAMES_CSV);
        return ini;
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT, Field.SYSTEMTIMESTAMP));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);

//			builder.setBolt(Component.PARSER, new GeneralParserBolt(parser, new Fields(Field.WORD, Field.SYSTEMTIMESTAMP))
//					, config.getInt(LeaderboardConstants.Conf.PARSER_THREADS, 1)
//					, new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.VOTER, new VoteBolt()//validate and put
                    , config.getInt(LeaderboardConstants.Conf.VOTER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));
//
//			builder.setBolt(Component.MAINTAINER, new MaintainBolt()//maintain the leaderboard
//					, config.getInt(LeaderboardConstants.Conf.MAINTAINER_THREADS, 1)
//					, new ShuffleGrouping(Component.VOTER));
//
//			//CREATE WINDOW trending_leaderboard ON proc_one_out ROWS 100 SLIDE 10;
//			//this window-bolt will simulate the window.
//			builder.setBolt(Component.WINDOW, new trendingLeaderboardBolt(100)
//							.withWindow(new BaseWindowedBolt.Count(100), new BaseWindowedBolt.Count(10))
//					, 1//this is going to be a global window.
//					, new GlobalGrouping(Component.MAINTAINER));
//
//
//			builder.setBolt(Component.WINDOW_TRIGGER, new trendingLeaderboard_triggerBolt()
//					, config.getInt(LeaderboardConstants.Conf.WINDOW_TRIGGER_THREADS, 1)
//					, new FieldsGrouping(Component.WINDOW, new Fields(contestantNumber)));
//
//			builder.setBolt(Component.LeaderBoard, new LeaderboardBolt()
//					, config.getInt(LeaderboardConstants.Conf.Leaderboard_THREADS, 1)
//					, new ShuffleGrouping(Component.WINDOW_TRIGGER));
//
//
//			builder.setBolt(Component.DELETER, new DeleteBolt()//delete a candidate if necessary
//					, config.getInt(LeaderboardConstants.Conf.DELETER_THREADS, 1)
//					, new ShuffleGrouping(Component.MAINTAINER));
//
//
            builder.setSink(Component.SINK, sink, sinkThreads
//					, new ShuffleGrouping(Component.DELETER)
                    , new ShuffleGrouping(Component.VOTER));

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
