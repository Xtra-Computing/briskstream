package streaming.impl.normal;


import applications.util.datatypes.StreamValues;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shuhaozhang on 17/7/16.
 */
public class sink extends MapBolt {
    private final static Logger LOG = LoggerFactory.getLogger(sink.class);
    private static final long serialVersionUID = -8260187705836234624L;

    public sink() {
        super(LOG);
//		state = new ValueState<String>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

    }

    public void execute(Tuple in) throws InterruptedException {
//        if (in != null) {
        String value = (String) in.getValueByField("word");
//            System.out.println("bolt execute:"+Thread.currentThread().getName()+"context"+ this.getContext());
        final long bid = in.getBID();
//
        LOG.info("Timestamp: " + System.currentTimeMillis() + "\tbolt:"
                + this.getContext().getThisTaskId()
                + " receives:" + value
                + " from:" + in.getSourceComponent());

        //state.update(value_list);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
           /* if(this.getContext().getThisTaskId()==1){
                System.out.print(".");
            }*/
        this.collector.emit(bid, new StreamValues(value));
//        }
//		return false;
    }

    @Override
    public void execute(TransferTuple in) {
        final long bid = in.getBID();
        final int bound = in.length;
        for (int i = 0; i < bound; i++) {
            int value = (int) in.getValueByField("word", i);
//            System.out.println("bolt execute:"+Thread.currentThread().getName()+"context"+ this.getContext());

            System.out.println("Timestamp: " + System.currentTimeMillis() + "\tbolt:"
                    + this.getContext().getThisTaskId()
                    + " receives: value_list " + value + "\t bid: (" + bid + ")"
                    + " from:" + in.getSourceComponent());

            //state.update(value_list);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}