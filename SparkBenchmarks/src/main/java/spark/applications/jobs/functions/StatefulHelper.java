package spark.applications.jobs.functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.applications.util.data.Event_MB;

/**
 * Created by I309939 on 8/2/2016.
 */
public class StatefulHelper implements PairFunction<Tuple2<String, Event_MB>, String, Event_MB> {

    @Override
    public Tuple2<String, Event_MB> call(Tuple2<String, Event_MB> stringEvent_mbTuple2) throws Exception {
        return stringEvent_mbTuple2;
    }
}
