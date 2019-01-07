package spark.applications.jobs.function_spout;

import applications.common.spout.helper.Event;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by szhang026 on 5/8/2016.
 */
public class SplitPair implements PairFunction<String, String, String> {

    private static final long serialVersionUID = -5155794207058968030L;

    @Override
    public Tuple2<String, String> call(String s) throws Exception {
        final String[] split = s.split(Event.split_expression);
        return new Tuple2<>(split[Event.pos_key], s);
    }
}
