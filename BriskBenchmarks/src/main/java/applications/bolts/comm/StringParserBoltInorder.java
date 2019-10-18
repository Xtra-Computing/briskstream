package applications.bolts.comm;

import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import helper.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parser.StringParser;
import util.Configuration;

import java.util.LinkedList;

/**
 * Created by tony on 5/5/2017.
 * Use char[] to represent string!
 */
public class StringParserBoltInorder extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(StringParserBoltInorder.class);
    private static final long serialVersionUID = 7613878877612069900L;
    final StringParser parser;
    private final Fields fields;
    public LinkedList<Long> gap;

    public StringParserBoltInorder(Parser parser, Fields fields) {
        super(LOG);
        this.parser = (StringParser) parser;
        this.fields = fields;
    }

    public Integer default_scale(Configuration conf) {
//		int numNodes = conf.getInt("num_socket", 1);
        return 2;
    }

    @Override
    public Fields getDefaultFields() {
        return fields;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not in use
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] string = in.getCharArray(0, i);
            char[] emit = parser.parse(string);
            collector.emit_inorder(bid
                    , gap
                    , emit);

            clean_gap(gap);
        }
//		//LOG.DEBUG("Parser(" + this.getContext().getThisTaskId() + ") emit:" + bid);
//		this.collector.try_fill_gap();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        gap = new LinkedList<>();
    }
}
