package applications.topology.benchmarks;

import applications.bolts.comm.ParserBolt;
import applications.bolts.vs.*;
import applications.topology.BasicTopology;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.VoIPSTREAMConstants.*;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoIPSTREAM extends BasicTopology {
	private static final Logger LOG = LoggerFactory.getLogger(VoIPSTREAM.class);
	private int varDetectThreads;
	private int ecrThreads;
	private int rcrThreads;
	private int encrThreads;
	private int ecr24Threads;
	private int ct24Threads;
	private int fofirThreads;
	private int urlThreads;
	private int acdThreads;
	private int scorerThreads;
	private int batch;
	private int vardetectsplitThreads;

	public VoIPSTREAM(String topologyName, Config config) {
		super(topologyName, config);
		varDetectThreads = (int) config.get(Conf.VAR_DETECT_THREADS);
		vardetectsplitThreads = 1;//(int)config.get (Conf.VAR_DETECT_Split_THREADS );
		ecrThreads = (int) config.get(Conf.ECR_THREADS);
		rcrThreads = (int) config.get(Conf.RCR_THREADS);
		encrThreads = (int) config.get(Conf.ENCR_THREADS);
		ecr24Threads = (int) config.get(Conf.ECR24_THREADS);
		ct24Threads = (int) config.get(Conf.CT24_THREADS);
		fofirThreads = (int) config.get(Conf.FOFIR_THREADS);
		urlThreads = (int) config.get(Conf.URL_THREADS);
		acdThreads = (int) config.get(Conf.ACD_THREADS);
		scorerThreads = (int) config.get(Conf.SCORER_THREADS);
	}

	public void initialize() {
		super.initialize();
		sink = loadSink();
	}

	@Override
	public StormTopology buildTopology() {
		batch = config.getInt("batch", 100);
		builder.setSpout(Component.SPOUT, spout, spoutThreads);
		spout.setFields(new Fields(Field.TEXT));

		builder.setBolt(Component.PARSER
				, new ParserBolt(parser
						, new Fields(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.RECORD)
				)
				, config.getInt(Conf.PARSER_THREADS, 1)).shuffleGrouping(Component.SPOUT);

		builder.setBolt(Component.VOICE_DISPATCHER, new VoiceDispatcherBolt(), varDetectThreads).
				fieldsGrouping(Component.PARSER, new Fields(Field.CALLING_NUM, Field.CALLED_NUM));
//
		builder.setBolt(Component.RCR, new RCRBolt(), rcrThreads)
				.fieldsGrouping(Component.VOICE_DISPATCHER, new Fields(Field.CALLING_NUM))
				.fieldsGrouping(Component.VOICE_DISPATCHER, Stream.BACKUP, new Fields(Field.CALLED_NUM));

		builder.setBolt(Component.ECR, new ECRBolt("ecr"), ecrThreads).
				fieldsGrouping(Component.VOICE_DISPATCHER, new Fields(Field.CALLING_NUM));

		builder.setBolt(Component.ENCR, new ENCRBolt(), encrThreads).
				fieldsGrouping(Component.VOICE_DISPATCHER, new Fields(Field.CALLING_NUM));


		builder.setBolt(Component.CT24, new CTBolt("ct24"), ct24Threads).
				fieldsGrouping(Component.VOICE_DISPATCHER, new Fields(Field.CALLING_NUM));

		builder.setBolt(Component.ECR24, new ECRBolt("ecr24"), ecr24Threads).
				fieldsGrouping(Component.VOICE_DISPATCHER, new Fields(Field.CALLING_NUM));

		builder.setBolt(Component.GLOBAL_ACD, new GlobalACDBolt(), 1).
				globalGrouping(Component.VOICE_DISPATCHER);


		builder.setBolt(Component.FOFIR, new FoFiRBolt(), fofirThreads).
				fieldsGrouping(Component.RCR, new Fields(Field.CALLING_NUM)).
				fieldsGrouping(Component.ECR, new Fields(Field.CALLING_NUM));

		builder.setBolt(Component.URL, new URLBolt(), urlThreads)
				.fieldsGrouping(Component.ENCR, new Fields(Field.CALLING_NUM)).
				fieldsGrouping(Component.ECR, new Fields(Field.CALLING_NUM));

		builder.setBolt(Component.ACD, new ACDBolt(), acdThreads).
				fieldsGrouping(Component.CT24, new Fields(Field.CALLING_NUM)).
				fieldsGrouping(Component.ECR24, new Fields(Field.CALLING_NUM)).
				allGrouping(Component.GLOBAL_ACD);

		builder.setBolt(Component.SCORER, new ScoreBolt(), scorerThreads)
				.fieldsGrouping(Component.FOFIR, new Fields(Field.CALLING_NUM))
				.fieldsGrouping(Component.URL, new Fields(Field.CALLING_NUM))
				.fieldsGrouping(Component.ACD, new Fields(Field.CALLING_NUM));

		builder.setBolt(Component.SINK, sink, sinkThreads)
				.shuffleGrouping(Component.SCORER);//SCORER

		return builder.createTopology();
	}

	@Override
	public Logger getLogger() {
		return LOG;
	}

	@Override
	public String getConfigPrefix() {
		return PREFIX;
	}
}
