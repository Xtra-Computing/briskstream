package applications.spout.combo;

import applications.bolts.gs.*;
import applications.param.mb.MicroEvent;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

import static applications.CONTROL.*;
import static applications.Constants.Event_Path;
import static engine.content.Content.*;
import static engine.profiler.Metrics.NUM_ACCESSES;
import static engine.profiler.Metrics.NUM_ITEMS;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class GSCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(GSCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;


    public GSCombo() {
        super(LOG, 0);
        this.scalable = false;
        state = new ValueState();
    }

    public void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) {
        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);
        int number_partitions = Math.min(tthread, config.getInt("number_partitions"));
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);

        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + String.valueOf(enable_states_partition))
                + OsUtils.OS_wrapper("NUM_EVENTS=" + String.valueOf(NUM_EVENTS))
                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + String.valueOf(ratio_of_multi_partition))
                + OsUtils.OS_wrapper("number_partitions=" + String.valueOf(number_partitions))
                + OsUtils.OS_wrapper("ratio_of_read=" + String.valueOf(ratio_of_read))
                + OsUtils.OS_wrapper("NUM_ACCESSES=" + String.valueOf(NUM_ACCESSES))
                + OsUtils.OS_wrapper("theta=" + String.valueOf(config.getDouble("theta", 1)))
                + OsUtils.OS_wrapper("NUM_ITEMS=" + String.valueOf(NUM_ITEMS));

        if (Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file_name))))
            throw new UnsupportedOperationException();

        Scanner sc;
        try {
            sc = new Scanner(new File(event_path + OsUtils.OS_wrapper(file_name)));
            int i = 0;
            Object event = null;
            for (int j = 0; j < taskId * num_events_per_thread; j++) {
                sc.nextLine();//skip un-related.
            }
            while (sc.hasNextLine()) {

                String read = sc.nextLine();
                String[] split = read.split(split_exp);

                event = new MicroEvent(
                        Integer.parseInt(split[0]), //bid
                        Integer.parseInt(split[1]), //pid
                        split[2], //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//key_array
                        Boolean.parseBoolean(split[6])//flag
                );

                myevents[i++] = event;
                //db.eventManager.put(event, Integer.parseInt(split[0]));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        super.initialize(thread_Id, thisTaskId, graph);

        switch (config.getInt("CCOption", 0)) {
            case CCOption_LOCK: {//no-order
                bolt = new GSBolt_nocc(0);
                break;
            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new GSBolt_olb(0);
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new GSBolt_lwm(0);
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new GSBolt_ts(0);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new GSBolt_sstore(0);
                break;
            }
        }

        //do preparation.
        bolt.prepare(config, context, collector);
        if (enable_shared_state)
            bolt.loadDB(config, context, collector);

        loadEvent("MB_events", config, context, collector);

    }
}