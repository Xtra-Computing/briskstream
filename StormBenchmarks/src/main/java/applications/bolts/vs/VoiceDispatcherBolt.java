package applications.bolts.vs;

import applications.bolts.AbstractBolt;
import applications.model.cdr.CallDetailRecord;
import applications.util.OsUtils;
import applications.util.hash.BloomFilter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

import static applications.constants.VoIPSTREAMConstants.*;

public class VoiceDispatcherBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VoiceDispatcherBolt.class);
    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;


    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();

        Fields fields = new Fields(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.NEW_CALLEE, Field.RECORD);

        streams.put(Stream.DEFAULT, fields);
        streams.put(Stream.BACKUP, fields);

        return streams;
    }

    private void VD_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        LOG.info("JVM PID  = " + pid);

        FileWriter fw;
        BufferedWriter writer;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output")
                    + OsUtils.OS_wrapper("vd_threadId.txt")));
            writer = new BufferedWriter(fw);
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void initialize() {
        approxInsertSize = config.getInt(Conf.VAR_DETECT_APROX_SIZE);
        falsePostiveRate = config.getDouble(Conf.VAR_DETECT_ERROR_RATE);

        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner = new BloomFilter<>(falsePostiveRate, approxInsertSize);

        cycleThreshold = detector.size() / Math.sqrt(2);

        // initMetrics(context);
        //VD_pid();
    }


    @Override
    public void execute(Tuple input) {
//        if (stat != null) stat.start_measure();
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        String key = String.format("%s:%s", cdr.getCallingNumber(), cdr.getCalledNumber());
        boolean newCallee = false;

        // add pair to learner
        learner.add(key);

        // check if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }

        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }

        Values values = new Values(cdr.getCallingNumber(), cdr.getCalledNumber(),
                cdr.getAnswerTime(), newCallee, cdr);

        collector.emit(values);
        collector.emit(Stream.BACKUP, values);
//        if (stat != null) stat.end_measure();
    }

    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}