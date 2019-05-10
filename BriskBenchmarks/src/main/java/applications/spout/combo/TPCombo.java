package applications.spout.combo;

import applications.bolts.tp.TPBolt_SSTORE;
import applications.bolts.tp.TPBolt_lwm;
import applications.bolts.tp.TPBolt_olb;
import applications.bolts.tp.TPBolt_ts;
import applications.constants.BaseConstants;
import applications.datatype.AbstractLRBTuple;
import applications.datatype.PositionReport;
import applications.param.lr.LREvent;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.faulttolerance.impl.ValueState;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static applications.CONTROL.*;
import static engine.content.Content.*;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class TPCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(TPCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;

    public TPCombo() {
        super(LOG, 0);
        this.scalable = false;
        state = new ValueState();
    }

    HashMap<Short, Integer> keys = new HashMap();

    DescriptiveStatistics stats = new DescriptiveStatistics();

    protected void show_stats() {

        for (Object myevent : myevents) {

            Short segment = ((LREvent) myevent).getPOSReport().getSegment();

            stats.addValue(segment);

            boolean containsKey = keys.containsKey(segment);
            if (containsKey) {
                keys.put(segment, keys.get(segment) + 1);
            } else {
                keys.put(segment, 1);
            }
        }


        for (Map.Entry<Short, Integer> entry : keys.entrySet()) {
            LOG.info("SEGMENT:" + entry.getKey() + " " + "Counter:" + entry.getValue());
        }
        LOG.info(stats.toString());

    }

    protected Object create_new_event(String record, int bid) {

        String[] token = record.split(" ");

        short type = Short.parseShort(token[0]);
        Short time = Short.parseShort(token[1]);
        Integer vid = Integer.parseInt(token[2]);

        if (type == AbstractLRBTuple.position_report) {
            return
                    new LREvent(new PositionReport(//
                            time,//
                            vid,//
                            Integer.parseInt(token[3]), // speed
                            Integer.parseInt(token[4]), // xway
                            Short.parseShort(token[5]), // lane
                            Short.parseShort(token[6]), // direction
                            Short.parseShort(token[7]), // segment
                            Integer.parseInt(token[8])),
                            tthread,
                            bid)

                    ;
        } else {
            //ignore, not used in this experiment.
            return null;
        }
    }


    @Override
    public void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) {


        int i = 0;
        int bid = 0;
        Scanner sc = null;
        try {
            sc = new Scanner(new File(file_name));


            for (int j = 0; j < taskId; j++) {
                sc.nextLine();
            }

            while (sc.hasNextLine() && bid < NUM_EVENTS) {

                String record = sc.nextLine();

                Object event = create_new_event(record, bid);
                if (event == null) {
                } else {
                    myevents[i++] = event;
                    bid++;
                    if (i == num_events_per_thread) break;

                    for (int j = 0; j < (tthread - 1) * combo_bid_size; j++) {
                        if (sc.hasNextLine())
                            sc.nextLine();//skip un-related.
                    }
                }
            }

            if (enable_debug)
                show_stats();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        super.initialize(thread_Id, thisTaskId, graph);

        _combo_bid_size = combo_bid_size;

        switch (config.getInt("CCOption", 0)) {
//            case CCOption_LOCK: {//no-order
//                bolt = new SL(0);
//                break;
//            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new TPBolt_olb(0);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new TPBolt_lwm(0);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new TPBolt_ts(0);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new TPBolt_SSTORE(0);
                _combo_bid_size = 1;
                break;
            }
        }

        //do preparation.
        bolt.prepare(config, context, collector);
        if (enable_shared_state)
            bolt.loadDB(config, context, collector);


        String OS_prefix = null;

        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }
        String path;

        if (OsUtils.isMac()) {
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_TEST_PATH)));
        } else {
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
        }

        String file = System.getProperty("user.home").concat("/data/app/").concat(path);

        loadEvent(file, config, context, collector);
    }
}