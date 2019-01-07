package applications.bolts;

//import applications.utils.Configuration;

import applications.constants.BaseConstants;
import applications.constants.BaseConstants.BaseStream;
import applications.util.Configuration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractBolt extends BaseRichBolt {
    protected final String splitregex = ",";
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    protected Map<String, Fields> fields;

    public AbstractBolt() {
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
        if (fields.isEmpty()) {
            if (getDefaultFields() != null)
                fields.put(BaseStream.DEFAULT, getDefaultFields());

            if (getDefaultStreamFields() != null)
                fields.putAll(getDefaultStreamFields());
        }

        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    public Fields getDefaultFields() {//@define the output fields
        return null;
    }

    public Map<String, Fields> getDefaultStreamFields() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.config = Configuration.fromMap(stormConf);
        this.context = context;
        this.collector = collector;
        initialize();
    }

    public void initialize() {

    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

}
