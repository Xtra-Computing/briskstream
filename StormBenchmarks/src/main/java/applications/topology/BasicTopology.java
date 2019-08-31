package applications.topology;

import applications.constants.BaseConstants;
import applications.sink.BaseSink;
import applications.spout.AbstractSpout;
import applications.spout.helper.parser.Parser;
import applications.util.ClassLoaderUtils;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BasicTopology.class);
    protected AbstractSpout spout;
    protected Parser parser;
    //    protected applications.spout.myKafkaSpout kspout;
//    protected applications.spout.verbose.myKafkaSpout kspout_verbose;
    protected BaseSink sink;
    protected int spoutThreads;
    protected int sinkThreads;


    public BasicTopology(String topologyName, Config config) {
        super(topologyName, config);

        spoutThreads = (int) config.get(BaseConstants.BaseConf.SPOUT_THREADS);//now read from parameters.
//        forwardThreads = config.getInt("SPOUT_THREADS", 1);//now read from parameters.
        sinkThreads = (int) config.get(BaseConstants.BaseConf.SINK_THREADS);

    }

    @Override
    public void initialize() {
        config.setConfigPrefix(getConfigPrefix());
        spout = loadSpout();
        initilize_parser();
    }

    protected void initilize_parser() {
        String parserClass = config.getString(getConfigKey(BaseConstants.BaseConf.SPOUT_PARSER), null);
        if (parserClass != null) {
            parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
            parser.initialize(config);
        } else {
            LOG.info("No parser is initialized");
        }
        parser.initialize(config);
    }

}
