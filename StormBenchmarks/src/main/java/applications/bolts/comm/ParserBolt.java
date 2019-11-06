package applications.bolts.comm;


import applications.bolts.AbstractBolt;
import helper.parser.Parser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.datatypes.StreamValues;

import java.util.List;
import java.util.Map;

/**
 * Created by tony on 5/5/2017.
 */
public class ParserBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ParserBolt.class);
    Fields default_fields;
    int loop;
    int cnt;
    private Parser parser;

    public ParserBolt(Parser parser, Fields fields) {
        this.parser = parser;
        this.default_fields = fields;
        cnt = 0;
        loop = 1;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        cnt = 0;
    }

    @Override
    public Fields getDefaultFields() {
        return default_fields;
    }

    @Override
    public void execute(Tuple input) {
//		long start = System.nanoTime();
        String value = input.getString(0);
        List<StreamValues> tuples = parser.parse(value);
        for (StreamValues values : tuples) {
            collector.emit(values.getStreamId(), values);
        }
//		long end = System.nanoTime();
//		LOG.info("Parser:" + (end - start));
    }
}
