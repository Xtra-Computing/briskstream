package applications.bolts.mb;

import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import org.slf4j.Logger;

import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.*;

public abstract class Bolt_LA extends GSBolt {


    public Bolt_LA(Logger log, int fid) {
        super(log, fid);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        //pre stream processing phase..

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        Long timestamp;//in.getLong(1);
        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        long _bid = in.getBID();

        END_PREPARE_TIME_MEASURE(thread_Id);


        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);//need to amortize.

        LAL_process(_bid);

        PostLAL_process(_bid);

        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE(thread_Id);


        post_process(_bid, timestamp);

        END_TOTAL_TIME_MEASURE_ACC(thread_Id);

    }

}
