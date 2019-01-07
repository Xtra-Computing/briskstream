package spark.applications.jobs.function_spout;

import applications.common.spout.helper.wrapper.ReceiveParser;
import applications.common.util.data.StreamValues;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import spark.applications.util.data.Event_MB;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by I309939 on 8/1/2016.
 */
public class parser implements FlatMapFunction<Tuple2<String, String>, Event_MB> {
    private static final Logger LOG = LogManager.getLogger(parser.class);
    private final ReceiveParser parser;

    public parser() {
        parser = new ReceiveParser();
    }

    public String getConfigKey(String template, String configPrefix) {
        return String.format(template, configPrefix);
    }

    /**
     * @param msg come from Kafka or Local queue stream.
     * @return
     * @throws Exception
     */
    @Override
    public Iterator<Event_MB> call(Tuple2<String, String> msg) throws Exception {
        /**
         * TODO:Use event time or `system time`? Now, we measure by using `system time`, that's the time when event is received.
         * Let's keep both!
         */
        long receive_time = System.nanoTime();
        final List<StreamValues> parse = parser.parse(msg._2());
        final StreamValues values = parse.get(0);
        // final long parse_finish_time = System.nanoTime();
        // final String state = values.get(2) + Event.split_expression + receive_time + Event.split_expression + parse_finish_time;
        return Arrays.asList(new Event_MB((Long) values.get(0), receive_time, (String) values.get(1), (String) values.get(2))).iterator();
    }
}
