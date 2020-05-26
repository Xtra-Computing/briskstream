package applications.bolts.udf.comm;

import brisk.components.operators.base.MapBolt;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import helper.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parser.StringParser;
import util.Configuration;

/**
 * Created by tony on 5/5/2017.
 * Use char[] to represent string!
 */
public class StringParserBolt extends MapBolt {
    private static final long serialVersionUID = 7613878877612069900L;
    private static final Logger LOG = LoggerFactory.getLogger(StringParserBolt.class);
    final StringParser parser;
    private final Fields fields;

    public StringParserBolt(Parser parser, Fields fields) {
        super(LOG);
        this.parser = (StringParser) parser;
        this.fields = fields;
    }

    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 4;
        } else {
            return 1;
        }


    }

    @Override
    public Fields getDefaultFields() {
        return fields;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//		String string = in.getString(0);
//		List<StreamValues> emit = parser.parse(string);
//		for (StreamValues values : emit) {
//			collector.force_emit(values.get(0));
//		}
        char[] string = in.getCharArray(0);
        char[] emit = parser.parse(string);
        collector.force_emit(emit);

    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
//		final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {

			/*
			String string = in.getString(0, i);
			List<StreamValues> emit = parser.parse(string);
			for (StreamValues values : emit) {
				collector.emit(-1L, values.get(0));
			}
			*/
            char[] string = in.getCharArray(0, i);
            char[] emit = parser.parse(string);
            collector.emit(0, emit);

        }
//		//LOG.DEBUG("Parser(" + this.getContext().getThisTaskId() + ") emit:" + bid);
//		this.collector.try_fill_gap();
    }

    @Override
    public void profile_execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] string = in.getCharArray(0, i);
            char[] emit = parser.parse(string);
            collector.emit_nowait(emit);
        }

    }

}
