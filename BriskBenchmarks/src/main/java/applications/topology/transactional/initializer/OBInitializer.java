package applications.topology.transactional.initializer;

import applications.param.ob.AlertEvent;
import applications.param.ob.BuyingEvent;
import applications.param.ob.OBParam;
import applications.param.ob.ToppingEvent;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import engine.Database;
import engine.DatabaseException;
import engine.common.SpinLock;
import engine.storage.SchemaRecord;
import engine.storage.TableRecord;
import engine.storage.datatype.DataBox;
import engine.storage.datatype.IntDataBox;
import engine.storage.datatype.LongDataBox;
import engine.storage.table.RecordSchema;
import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static applications.CONTROL.enable_states_partition;
import static applications.Constants.Event_Path;
import static applications.constants.OnlineBidingSystemConstants.Constant.*;
import static brisk.controller.affinity.SequentialBinding.next_cpu_for_db;
import static engine.profiler.Metrics.NUM_ITEMS;
import static utils.PartitionHelper.getPartition_interval;
import static xerial.jnuma.Numa.setLocalAlloc;

public class OBInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(OBInitializer.class);
    //triple decisions
    protected int[] triple_decision = new int[]{0, 0, 0, 0, 0, 0, 1, 2};//6:1:1 buy, alert, topping_handle.

    public OBInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
    }

    @Override
    public void loadDB(int thread_id, TopologyContext context) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == context.getNUMTasks() - 1) {//last executor need to handle left-over
            right_bound = NUM_ITEMS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }

        for (int key = left_bound; key < right_bound; key++) {
            insertItemRecords(key, 100);
        }

        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);

    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, TopologyContext context) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == context.getNUMTasks() - 1) {//last executor need to handle left-over
            right_bound = NUM_ITEMS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }

        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);


            insertItemRecords(key, 100, pid, spinlock);
        }

        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    /**
     * TODO: be aware, scale_factor is not in use now.
     *
     * @param scale_factor
     * @param theta
     * @param partition_interval
     * @param spinlock_
     */
    public void loadData_Central(double scale_factor, double theta, int partition_interval, SpinLock[] spinlock_) {
        int elements = (int) (NUM_ITEMS * scale_factor);
        int elements_per_socket;

        setLocalAlloc();

        if (OsUtils.isMac())
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock_ratio to 0.
        else
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock_ratio to 0.

        if (OsUtils.isMac())
            elements_per_socket = elements;
        else
            elements_per_socket = elements / 4;

        int i = 0;
        for (int key = 0; key < elements; key++) {
            int pid = get_pid(partition_interval, key);
            insertItemRecords(key, 100, pid, spinlock_);
            i++;
            if (i == elements_per_socket) {
                AffinityLock.reset();
                if (OsUtils.isMac())
                    AffinityLock.acquireLock(next_cpu_for_db());
                else
                    AffinityLock.acquireLock(next_cpu_for_db());
                i = 0;
            }
        }
    }

    @Override
    public void loadData_Central(double scale_factor, double theta) {
        int elements = (int) (NUM_ITEMS * scale_factor);
        int elements_per_socket;


        setLocalAlloc();

        if (OsUtils.isMac())
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock_ratio to 0.
        else
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock_ratio to 0.


        if (OsUtils.isMac())
            elements_per_socket = elements;
        else
            elements_per_socket = elements / 4;

        int i = 0;
        for (int key = 0; key < elements; key++) {
            insertItemRecords(key, 100);
            i++;
            if (i == elements_per_socket) {
                AffinityLock.reset();
                if (OsUtils.isMac())
                    AffinityLock.acquireLock(next_cpu_for_db());
                else
                    AffinityLock.acquireLock(next_cpu_for_db());
                i = 0;
            }
        }
    }

    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertItemRecords(int key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("goods", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertItemRecords(int key, long value, int pid, SpinLock[] spinlock_) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("goods", new TableRecord(schemaRecord, pid, spinlock_));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }


    private RecordSchema Goods() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new LongDataBox());

        fieldNames.add("ID");//PK
        fieldNames.add("Price");
        fieldNames.add("Qty");

        return new RecordSchema(fieldNames, dataBoxes);
    }


    /**
     * OB
     *
     * @param partition_id
     * @param bid_array
     * @param number_of_partitions
     * @param bid
     * @param rnd
     * @return
     */
    protected BuyingEvent randomBuyEvents(int partition_id, long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {

        int pid = partition_id;
        OBParam param = new OBParam(NUM_ACCESSES_PER_BUY);

        Set keys = new HashSet();
        int access_per_partition = (int) Math.ceil(NUM_ACCESSES_PER_BUY / (double) number_of_partitions);

        int counter = 0;

        randomkeys(pid, param, keys, access_per_partition, counter, NUM_ACCESSES_PER_BUY);

        if (enable_states_partition)
            assert verify(keys, partition_id, number_of_partitions);

        return new BuyingEvent(param.keys(), rnd, partition_id, bid_array, bid, number_of_partitions);
    }


    protected AlertEvent randomAlertEvents(int partition_id, long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {
        int pid = partition_id;

        int num_access = rnd.nextInt(NUM_ACCESSES_PER_ALERT) + 5;
        OBParam param = new OBParam(num_access);

        Set keys = new HashSet();
        int access_per_partition = (int) Math.ceil(num_access / (double) number_of_partitions);

        int counter = 0;

        randomkeys(pid, param, keys, access_per_partition, counter, num_access);

        assert verify(keys, partition_id, number_of_partitions);


        return new AlertEvent(
                num_access,
                param.keys(),
                rnd,
                partition_id, bid_array, bid, number_of_partitions
        );
    }

    protected ToppingEvent randomToppingEvents(int partition_id, long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {

        int pid = partition_id;

        int num_access = rnd.nextInt(NUM_ACCESSES_PER_TOP) + 5;

        OBParam param = new OBParam(num_access);

        Set keys = new HashSet();
        int access_per_partition = (int) Math.ceil(num_access / (double) number_of_partitions);

        int counter = 0;

        randomkeys(pid, param, keys, access_per_partition, counter, num_access);

        assert verify(keys, partition_id, number_of_partitions);

        return new ToppingEvent(
                num_access,
                param.keys(),
                rnd,
                partition_id, bid_array, bid, number_of_partitions
        );

    }

    private int i = 0;

    protected int next_decision3() {
        int rt = triple_decision[i];
        i++;
        if (i == 8)
            i = 0;
        return rt;

    }

    @Override
    protected Object create_new_event(int num_p, int bid) {
        int flag = next_decision3();
        if (flag == 0) {
            return randomBuyEvents(p, p_bid.clone(), num_p, bid, rnd);
        } else if (flag == 1) {
            return randomAlertEvents(p, p_bid.clone(), num_p, bid, rnd);//(AlertEvent) in.getValue(0);
        } else {
            return randomToppingEvents(p, p_bid.clone(), num_p, bid, rnd);//(AlertEvent) in.getValue(0);
        }
    }


    @Override
    protected boolean load(String file) throws IOException {
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + String.valueOf(enable_states_partition));

        if (Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file))))
            return false;

        Scanner sc;
        sc = new Scanner(new File(event_path + OsUtils.OS_wrapper(file)));

        Object event;
        while (sc.hasNextLine()) {
            String read = sc.nextLine();
            String[] split = read.split(split_exp);

            if (split[4].endsWith("BuyingEvent")) {//BuyingEvent
                event = new BuyingEvent(
                        Integer.parseInt(split[0]), //bid
                        split[2], //bid_array
                        Integer.parseInt(split[1]),//pid
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//key_array
                        split[6],//price_array
                        split[7]  //qty_array
                );
            } else if (split[4].endsWith("AlertEvent")) {//AlertEvent
                event = new AlertEvent(
                        Integer.parseInt(split[0]), //bid
                        split[2], // bid_array
                        Integer.parseInt(split[1]),//pid
                        Integer.parseInt(split[3]),//num_of_partition
                        Integer.parseInt(split[5]), //num_access
                        split[6],//key_array
                        split[7]//price_array
                );
            } else {
                event = new ToppingEvent(
                        Integer.parseInt(split[0]), //bid
                        split[2], Integer.parseInt(split[1]), //pid
                        //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        Integer.parseInt(split[5]), //num_access
                        split[6],//key_array
                        split[7]  //top_array
                );
            }
            db.eventManager.put(event, Integer.parseInt(split[0]));

        }

        return true;
    }

    @Override
    protected void store(String file_name) throws IOException {
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + String.valueOf(enable_states_partition));

        File file = new File(event_path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist

        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(event_path + OsUtils.OS_wrapper(file_name))));

        for (Object event : db.eventManager.input_events) {

            StringBuilder sb = new StringBuilder();
            if (event instanceof BuyingEvent) {
                sb.append(((BuyingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).num_p());//3
                sb.append(split_exp);
                sb.append("BuyingEvent");//event types.
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getItemId()));//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidPrice()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidQty()));//7

            } else if (event instanceof AlertEvent) {
                sb.append(((AlertEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((AlertEvent) event).num_p());
                sb.append(split_exp);
                sb.append("AlertEvent");//event types.
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getAsk_price()));

            } else if (event instanceof ToppingEvent) {
                sb.append(((ToppingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).num_p());
                sb.append(split_exp);
                sb.append("ToppingEvent");//event types.
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemTopUp()));
            }

            w.write(sb.toString() + "\n");
        }
        w.close();
    }

    public void creates_Table() {
        RecordSchema s = Goods();
        db.createTable(s, "goods");
        try {
            prepare_input_events("OB_events");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
