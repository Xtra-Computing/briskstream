package applications.spout.combo;

import applications.bolts.lr.txn.TPBolt_SSTORE;
import applications.bolts.lr.txn.TPBolt_lwm;
import applications.bolts.lr.txn.TPBolt_olb;
import applications.bolts.lr.txn.TPBolt_ts;
import brisk.execution.ExecutionGraph;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.*;
import static engine.content.Content.*;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class TPCombo extends Combo {
    private static final Logger LOG = LoggerFactory.getLogger(TPCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;

    public TPCombo() {
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
                bolt = new TPBolt_olb(0);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new TPBolt_lwm(0);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new TPBolt_ts(0);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new TPBolt_SSTORE(0);
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