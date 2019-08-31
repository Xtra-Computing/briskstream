package applications.bolts.comm;

import applications.bolts.AbstractBolt;
import applications.util.hash.ODTDBloomFilter;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.VoIPSTREAMConstants.Conf;
import static applications.constants.VoIPSTREAMConstants.Field;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractFilterBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFilterBolt.class);

    protected ODTDBloomFilter filter;

    protected String configPrefix;
    protected String outputField;
    protected String outputkeyField;

    public AbstractFilterBolt(String configPrefix, String outputField, String outputkeyField) {
        this.configPrefix = configPrefix;
        this.outputField = outputField;
        this.outputkeyField = outputkeyField;
    }


    @Override
    public Fields getDefaultFields() {
        if (this.outputkeyField == null) {
            return new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD);
        } else {
            return new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD, outputkeyField);
        }
    }

    @Override
    public void initialize() {
        int numElements = config.getInt(String.format(Conf.FILTER_NUM_ELEMENTS, configPrefix));
        int bucketsPerElement = config.getInt(String.format(Conf.FILTER_BUCKETS_PEL, configPrefix));
        int bucketsPerWord = config.getInt(String.format(Conf.FILTER_BUCKETS_PWR, configPrefix));
        double beta = config.getDouble(String.format(Conf.FILTER_BETA, configPrefix));

        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }
}