package applications.sink;

import applications.StormRunner;
import applications.bolts.AbstractBolt;
import constants.BaseConstants.BaseConf;
import applications.sink.formatter.BasicFormatter;
import applications.sink.formatter.Formatter;
import util.ClassLoaderUtils;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public abstract class BaseSink extends AbstractBolt {
    protected Formatter formatter;

    protected BaseSink() {

    }

    @Override
    public void initialize() {
        String formatterClass = config.getString(getConfigKey(BaseConf.SINK_FORMATTER), null);

        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }

        formatter.initialize(config, context);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields("");
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    protected abstract Logger getLogger();

    protected void killTopology() {

        if (((String) config.get("mode")).equalsIgnoreCase("local")) {
            StormRunner.cluster.shutdown();
            StormRunner.cluster = null;
        } else {
            Map conf = Utils.readStormConfig();
            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
            try {
                List<TopologySummary> topologyList = client.getClusterInfo().get_topologies();
                while (topologyList.size() == 0) {
                    topologyList = client.getClusterInfo().get_topologies();
                    TimeUnit.SECONDS.sleep(1);
                }
                KillOptions killOpts = new KillOptions();
                killOpts.set_wait_secs(5); // time to wait before killing
                while (topologyList.size() != 0) {
                    client.killTopologyWithOpts(topologyList.get(0).get_name(), killOpts); //provide Brisk.topology name
                    TimeUnit.SECONDS.sleep(1);
                }
                throw new NotAliveException();
            } catch (TException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
