package streaming.impl.ft;


import brisk.components.operators.api.Checkpointable;
import brisk.components.operators.base.MapBolt;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.datatypes.StreamValues;

/**
 * Created by shuhaozhang on 17/7/16.
 */
public class bolt_ft extends MapBolt implements Checkpointable {
    private final static Logger LOG = LoggerFactory.getLogger(bolt_ft.class);
    private static final long serialVersionUID = -122187786830196468L;
    String current_value;

    public bolt_ft() {
        super(LOG);
        state = new ValueState<String>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


    public void execute(Tuple in) {
//        if (in != null) {
        String value = (String) in.getValueByField("word");
//            System.out.println("bolt execute:"+Thread.currentThread().getName()+"context"+ this.getContext());

//
//		LOG.info("Timestamp: " + System.currentTimeMillis() + "\tbolt:"
//				+ this.getContext().getThisTaskId()
//				+ " receives:" + value_list
//				+ " from:" + in.getSourceComponent());

        //state.update(value_list);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
           /* if(this.getContext().getThisTaskId()==1){
                System.out.print(".");
            }*/

//        }
//		return false;
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        final long bid = in.getBID();
        final int bound = in.length;
        for (int i = 0; i < bound; i++) {
//			try {
//				Thread.sleep(100);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}

            String value = (String) in.getValueByField("word", i);
//            System.out.println("bolt execute:"+Thread.currentThread().getName()+"context"+ this.getContext());

//
            //Re enable when we need to test FF
            final Marker marker = in.getMarker(i);
            if (marker != null) {
                forward_checkpoint(in.getSourceTask(), bid, marker);
            } else {
//				System.out.println("Timestamp: " + System.currentTimeMillis() + "\tbolt:"
//						+ this.getContext().getThisTaskId()
//						+ " receives:" + value_list
//						+ " from:" + input.getSourceComponent());

                //state.update(value_list);

           /* if(this.getContext().getThisTaskId()==1){
                System.out.print(".");
            }*/
                current_value = value;
                this.collector.emit(bid, new StreamValues(value));
            }
        }

    }

    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException {
        final boolean check = checkpoint_store(current_value, sourceId, marker);//call forward_checkpoint.
        if (check) {
            this.collector.broadcast_marker(bid, marker);//bolt needs to broadcast_marker
            //LOG.DEBUG(this.getContext().getThisComponentId() + this.getContext().getThisTaskId() + " broadcast marker with id:" + marker.msgId + "@" + DateTime.now());
        }
    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) {

    }

    @Override
    public void ack_checkpoint(Marker marker) {
//		//LOG.DEBUG("Received ack from all consumers.");

        //Do something to clear past state. (optional)

//		//LOG.DEBUG("Broadcast ack to all producers.");
        this.collector.broadcast_ack(marker);//bolt needs to broadcast_marker

    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }
}