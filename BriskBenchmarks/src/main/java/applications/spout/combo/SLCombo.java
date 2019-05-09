package applications.spout.combo;

import applications.bolts.sl.SLBolt_lwm;
import applications.bolts.sl.SLBolt_olb;
import applications.bolts.sl.SLBolt_sstore;
import applications.bolts.sl.SLBolt_ts;
import brisk.execution.ExecutionGraph;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.*;
import static engine.content.Content.*;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class SLCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(SLCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;

    public SLCombo() {
        super(LOG, 0);
        this.scalable = false;
        state = new ValueState();
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        super.initialize(thread_Id, thisTaskId, graph);

        _combo_bid_size = combo_bid_size;

        switch (config.getInt("CCOption", 0)) {
//            case CCOption_LOCK: {//no-order
//                bolt = new SL(0);
//                break;
//            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new SLBolt_olb(0);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new SLBolt_lwm(0);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new SLBolt_ts(0);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new SLBolt_sstore(0);
                _combo_bid_size = 1;
                break;
            }
        }

        //do preparation.
        bolt.prepare(config, context, collector);
        if (enable_shared_state)
            bolt.loadDB(config, context, collector);

    }
}