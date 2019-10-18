package applications.sink;

import applications.bolts.AbstractBolt;
import constants.BaseConstants.BaseConf;
import applications.sink.formatter.BasicFormatter;
import applications.sink.formatter.Formatter;
import util.ClassLoaderUtils;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;


public abstract class BaseSink extends AbstractBolt {
    protected Formatter formatter;

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
        System.exit(0);
    }
}
