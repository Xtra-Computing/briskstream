package applications.general.topology;

import applications.constants.BaseConstants.BaseConf;
import applications.general.sink.BaseSink;
import applications.general.spout.AbstractSpout;
import applications.util.ClassLoaderUtils;
import applications.util.Configuration;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public abstract class AbstractTopology {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractTopology.class);
	public int sink_id = 0;
	protected TopologyBuilder builder;
	protected String topologyName;
	protected Configuration config;

	public AbstractTopology(String topologyName, Config config) {
		this.topologyName = topologyName;
		this.config = Configuration.fromMap(config);
		this.builder = new TopologyBuilder();
	}

	public String getTopologyName() {
		return topologyName;
	}

	protected AbstractSpout loadSpout() {
		return loadSpout(BaseConf.SPOUT_CLASS, getConfigPrefix());
	}

	protected AbstractSpout loadSpout(String name) {
		return loadSpout(BaseConf.SPOUT_CLASS, String.format("%s.%s", getConfigPrefix(), name));
	}

	protected AbstractSpout loadSpout(String configKey, String configPrefix) {

		String spoutClass = config.getString(String.format(configKey, configPrefix));//config.getString("spout_class", null);

		if (config.getBoolean("verbose")) {
			final String[] split = spoutClass.split("\\.");
			spoutClass = "applications.general.spout." + "verbose." + split[2];
			LOG.info("spout class:" + spoutClass);
		}
		AbstractSpout spout;

		spout = (AbstractSpout) ClassLoaderUtils.newInstance(spoutClass, "spout", getLogger());
		spout.setConfigPrefix(configPrefix);

		return spout;
	}

	protected BaseSink loadSink() {
		return loadSink(BaseConf.SINK_CLASS, getConfigPrefix());
	}

	protected BaseSink loadSink(String name) {
		return loadSink(BaseConf.SINK_CLASS, String.format("%s.%s", getConfigPrefix(), name));
	}

	protected BaseSink loadSink(String configKey, String configPrefix) {

		String sinkClass = config.getString("sink_class", null);

		if (config.getBoolean("verbose")) {
			final String[] split = sinkClass.split("\\.");
			sinkClass = "applications.general.sink." + "verbose." + split[2];
			LOG.info("sink class:" + sinkClass);
		}

		if (sinkClass == null) {
			sinkClass = config.getString(String.format(configKey, configPrefix));
		}


		BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
		sink.setConfigPrefix(configPrefix);

		return sink;
	}

//    /**
//     * Utility method to parse a configuration key with the application prefix and
//     * component prefix.
//     *
//     * @param key  The configuration key to be parsed
//     * @param name The name of the component
//     * @return The formatted configuration key
//     */
//    protected String getConfigKey(String key, String name) {
//        return String.format(key, String.format("%s.%s", getConfigPrefix(), name));
//    }

	/**
	 * Utility method to parse a configuration key with the application prefix..
	 *
	 * @param key The configuration key to be parsed
	 * @return
	 */
	protected String getConfigKey(String key) {
		return config.getConfigKey(key);
	}

	/**
	 * Utility method to parse a configuration key with the application prefix and
	 * component prefix.
	 *
	 * @param key  The configuration key to be parsed
	 * @param name The name of the component
	 * @return The formatted configuration key
	 */
	protected String getConfigKey(String key, String name) {
		return String.format(key, String.format("%s.%s", getConfigPrefix(), name));
	}

	public abstract void initialize();

	public LinkedList ConfigAllocation(int allocation_opt) {

		return null;
	}

	public abstract StormTopology buildTopology();

	public abstract Logger getLogger();

	public abstract String getConfigPrefix();

}