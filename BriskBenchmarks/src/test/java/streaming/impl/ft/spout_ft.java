package streaming.impl.ft;

import brisk.components.operators.api.AbstractSpout;
import brisk.components.operators.api.Checkpointable;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shuhaozhang on 17/7/16.
 */
public class spout_ft extends AbstractSpout implements Checkpointable {
    private static final Logger LOG = LoggerFactory.getLogger(spout_ft.class);
    private static final long serialVersionUID = -6323303964544173816L;
    private int count = 0;

    public spout_ft() {
        super(LOG);
        state = new ValueState();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));
        // declarer.declareStream("backup",new Fields("word"));
    }


    @Override
    public void cleanup() {

    }


    @Override
    public void nextTuple() {
//        String[] words = {"1", "2", "3", "4", "5"};
        // Random rn = new Random();

        //System.out.println("Spout nextTuple:"+Thread.currentThread().getName()+"context"+ this.getContext());
        final String value = String.valueOf(count++);
//		LOG.info("Spout:" + this.getContext().getThisTaskId() + " send:" + value_list);
//		final StreamValues streamValues = new StreamValues(value_list);
//		final Marker marker = this.collector.emit_single(streamValues);
//		if (marker != null) {
//			//LOG.DEBUG(this.getContext().getThisComponentId() + this.getContext().getThisTaskId() + " broadcast marker with id:" + marker.msgId + "@" + marker.timeStampNano);
//			forward_checkpoint(this.getContext().getThisTaskId(), bid, marker);
//		}
    }
    @Override
    public void forward_checkpoint_single(int sourceId, long bid, Marker marker) {

    }

    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) {

    }

    @Override
    public boolean checkpoint(int counter) {
        return false;
    }

    /**
     * spout has updated its state for every emit, so we don't need to GetAndUpdate it further here.
     */
    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker) {
        checkpoint_forward(sourceId);//call forward_checkpoint
    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) {

    }

    /**
     * clear stored messages before marker
     *
     * @param marker
     */
    @Override
    public void ack_checkpoint(Marker marker) {
        //LOG.DEBUG("Received ack from all consumers.");

        //Do something to clear past state. (optional)

//		//LOG.DEBUG("Broadcast ack to all producers.");
//		this.collector.broadcast_ack(marker);//bolt needs to broadcast_marker
        success = true;//I can emit next marker.
    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }
}