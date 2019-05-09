package applications.spout.combo;

import applications.bolts.gs.*;
import brisk.execution.ExecutionGraph;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.*;
import static engine.content.Content.*;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class GSCombo extends Combo {
    private static final Logger LOG = LoggerFactory.getLogger(GSCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;


    public GSCombo() {
        super(LOG, 0);
        this.scalable = false;
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        super.initialize(thread_Id, thisTaskId, graph);

        switch (config.getInt("CCOption", 0)) {
            case CCOption_LOCK: {//no-order
                bolt = new GSBolt_nocc(0);
                break;
            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new GSBolt_olb(0);
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new GSBolt_lwm(0);
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new GSBolt_ts(0);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new GSBolt_sstore(0);
                break;
            }
        }

        //do preparation.
        bolt.prepare(config, context, collector);
        if (enable_shared_state)
            bolt.loadDB(config, context, collector);



    }
}