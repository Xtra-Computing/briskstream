package applications.general.sink;

import applications.general.bolts.AbstractBolt;
import applications.constants.BaseConstants.BaseConf;
import applications.general.sink.formatter.BasicFormatter;
import applications.general.sink.formatter.Formatter;
import applications.util.ClassLoaderUtils;
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
