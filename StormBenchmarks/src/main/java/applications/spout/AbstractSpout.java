package applications.spout;

//import applications.utils.Configuration;

import constants.BaseConstants;
import constants.BaseConstants.BaseStream;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import util.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractSpout extends BaseRichSpout {
    protected String configPrefix = BaseConstants.BASE_PREFIX;

    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;
    protected Map<String, Fields> fields;

    public AbstractSpout() {
        fields = new HashMap<>();
    }

    public void setFields(Fields fields) {
        this.fields.put(BaseStream.DEFAULT, fields);
    }

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.config = Configuration.fromMap(conf);
        this.collector = collector;
        this.context = context;
        initialize();
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    protected abstract void initialize();
}
