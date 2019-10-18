package streaming.impl.normal;

import brisk.components.operators.api.AbstractSpout;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static Constants.DEFAULT_STREAM_ID;

/**
 * Created by shuhaozhang on 17/7/16.
 */
public class spout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(spout.class);
    private static final long serialVersionUID = -2678855055460084440L;
    private int count = 0;

    public spout() {
        super(LOG);
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
    public void nextTuple() throws InterruptedException {

//        String[] words = {"1", "2", "3", "4", "5"};
        // Random rn = new Random();

//        String word = words[count++ % words.length];
//        System.out.println("Emitting from spout:" + word);

        //System.out.println("Spout nextTuple:"+Thread.currentThread().getName()+"context"+ this.getContext());
//		final String value_list = String.valueOf(count++);
//		//LOG.DEBUG("Spout:" + this.getContext().getThisTaskId() + " send:" + value_list);
        this.collector.emit_bid(DEFAULT_STREAM_ID, count++);
        //this.collector.emit("backup",new StreamValues(word));
    }
}