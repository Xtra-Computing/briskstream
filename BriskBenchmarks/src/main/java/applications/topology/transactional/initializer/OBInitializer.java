package applications.topology.transactional.initializer;

import applications.param.ob.AlertEvent;
import applications.param.ob.BuyingEvent;
import applications.param.ob.OBParam;
import applications.param.ob.ToppingEvent;
import applications.tools.FastZipfGenerator;
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

import static applications.Constants.Event_Path;
import static applications.constants.OnlineBidingSystemConstants.Constant.*;
import static applications.topology.transactional.State.partioned_store;
import static brisk.controller.affinity.SequentialBinding.next_cpu_for_db;
import static engine.profiler.Metrics.NUM_ITEMS;
import static utils.PartitionHelper.getPartition_interval;
import static utils.PartitionHelper.key_to_partition;
import static xerial.jnuma.Numa.setLocalAlloc;

public class OBInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(OBInitializer.class);
    Random r = new Random(1234);

    private final String split_exp = ";";

    @Override
    public void loadData(int thread_id, TopologyContext context) {
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
    public void loadData(int thread_id, SpinLock[] spinlock, TopologyContext context) {
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
    public void loadData(double scale_factor, double theta, int partition_interval, SpinLock[] spinlock_) {
        int elements = (int) (NUM_ITEMS * scale_factor);
        int elements_per_socket;

        setLocalAlloc();

        if (OsUtils.isMac())
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock to 0.
        else
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock to 0.

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
    public void loadData(double scale_factor, double theta) {
        int elements = (int) (NUM_ITEMS * scale_factor);
        int elements_per_socket;


        setLocalAlloc();

        if (OsUtils.isMac())
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock to 0.
        else
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock to 0.


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
        values.add(new LongDataBox(r.nextInt(MAX_Price)));//random price goods.
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
        values.add(new LongDataBox(r.nextInt(MAX_Price)));//random price goods.
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

    //triple decisions
    protected int[] triple_decision = new int[]{0, 0, 0, 0, 0, 0, 1, 2};//6:1:1 buy, alert, topping_handle.
    protected long[] p_bid;//used for partition.
    protected transient FastZipfGenerator p_generator;
    protected int number_partitions;
    protected boolean[] multi_partion_decision;
    SplittableRandom rnd = new SplittableRandom(1234);
    int i = 0;
    int j = 0;
    int p;

    public OBInitializer(Database db, double scale_factor, double theta, int tthread, int number_partitions, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        floor_interval = (int) Math.floor(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
        p_generator = new FastZipfGenerator(NUM_ITEMS, theta, 0);
        this.number_partitions = Math.min(tthread, number_partitions);

        p_bid = new long[tthread];

        for (int i = 0; i < tthread; i++) {
            p_bid[i] = 0;
        }


        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);

        if (ratio_of_multi_partition == 0) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, false, false};// all single.
        } else if (ratio_of_multi_partition == 0.125) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, false, true};//75% single, 25% multi.
        } else if (ratio_of_multi_partition == 0.25) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, true, true};//75% single, 25% multi.
        } else if (ratio_of_multi_partition == 0.5) {
            multi_partion_decision = new boolean[]{false, false, false, false, true, true, true, true};//equal ratio.
        } else if (ratio_of_multi_partition == 0.75) {
            multi_partion_decision = new boolean[]{false, false, true, true, true, true, true, true};//25% single, 75% multi.
        } else if (ratio_of_multi_partition == 0.875) {
            multi_partion_decision = new boolean[]{false, true, true, true, true, true, true, true};//25% single, 75% multi.
        } else if (ratio_of_multi_partition == 1) {
            multi_partion_decision = new boolean[]{true, true, true, true, true, true, true, true};// all multi.
        } else {
            throw new UnsupportedOperationException();
        }

        LOG.info("ratio_of_multi_partition: " + ratio_of_multi_partition + "\tDECISIONS: " + Arrays.toString(multi_partion_decision));

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

        for (int access_id = 0; access_id < NUM_ACCESSES_PER_BUY; ++access_id) {
            FastZipfGenerator generator = partioned_store[pid];
            int res = generator.next();
            //should not have duplicate keys.
            while (keys.contains(res) && !Thread.currentThread().isInterrupted()) {
//                res++;//speed up the search for non-duplicate key.
//                if (res == NUM_ITEMS) {
//                    res = partition_id * interval;
//                }
                res = generator.next();
            }

            keys.add(res);
            param.set_keys(access_id, res);
            counter++;
            if (counter == access_per_partition) {
//                pointer++;
                pid++;
                if (pid == tthread)
                    pid = 0;

                counter = 0;
            }
        }

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

        for (int access_id = 0; access_id < num_access; ++access_id) {
            FastZipfGenerator generator = partioned_store[pid];
            int res = generator.next();
            //should not have duplicate keys.
            while (keys.contains(res) && !Thread.currentThread().isInterrupted()) {
//                res++;//speed up the search for non-duplicate key.
//                if (res == NUM_ITEMS) {
//                    res = partition_id * interval;
//                }
                res = generator.next();
            }

            keys.add(res);
            param.set_keys(access_id, res);
            counter++;
            if (counter == access_per_partition) {
//                pointer++;
                pid++;
                if (pid == tthread)
                    pid = 0;

                counter = 0;
            }
        }

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

        for (int access_id = 0; access_id < num_access; ++access_id) {
            FastZipfGenerator generator = partioned_store[pid];
            int res = generator.next();
            //should not have duplicate keys.
            while (keys.contains(res) && !Thread.currentThread().isInterrupted()) {
//                res++;//speed up the search for non-duplicate key.
//                if (res == NUM_ITEMS) {
//                    res = partition_id * interval;
//                }
                res = generator.next();
            }

            keys.add(res);
            param.set_keys(access_id, res);
            counter++;
            if (counter == access_per_partition) {
//                pointer++;
                pid++;
                if (pid == tthread)
                    pid = 0;

                counter = 0;
            }
        }

        assert verify(keys, partition_id, number_of_partitions);

        return new ToppingEvent(
                num_access,
                param.keys(),
                rnd,
                partition_id, bid_array, bid, number_of_partitions
        );

    }

    protected int next_decision3() {

        int rt = triple_decision[i];

        i++;
        if (i == 8)
            i = 0;

        return rt;

    }

    private Object create_new_event(int num_p, int i) {
        int flag = next_decision3();
        if (flag == 0) {
            return randomBuyEvents(p, p_bid.clone(), num_p, i, rnd);
        } else if (flag == 1) {
            return randomAlertEvents(p, p_bid.clone(), num_p, i, rnd);//(AlertEvent) in.getValue(0);
        } else {
            return randomToppingEvents(p, p_bid.clone(), num_p, i, rnd);//(AlertEvent) in.getValue(0);
        }
    }

    public void prepare_input_events() throws IOException {

        db.eventManager.ini(num_events);

        //try to read from file.
        if (!load("OB_events" + tthread)) {
            //if failed, create new one.
            Object event;
            for (int i = 0; i < num_events; i++) {
                boolean flag2 = multi_partion_decision[j];
                j++;
                if (j == 8)
                    j = 0;

                if (flag2) {//multi-partition
                    p = key_to_partition(p_generator.next());//randomly pick a starting point.
                    event = create_new_event(number_partitions, i);
                    for (int k = 0; k < number_partitions; k++) {
                        p_bid[p]++;
                        p++;
                        if (p == tthread)
                            p = 0;
                    }
                } else {
                    event = create_new_event(1, i);
                    p_bid[p]++;
                    p++;
                    if (p == tthread)
                        p = 0;
                }


                db.eventManager.put(event, i);


            }
            dump("OB_events" + tthread);
        }
    }

    private boolean load(String file) throws IOException {

        if (Files.notExists(Paths.get(Event_Path + OsUtils.OS_wrapper(file))))
            return false;

        Scanner sc;
        sc = new Scanner(new File(Event_Path + OsUtils.OS_wrapper(file)));

        Object event;
        while (sc.hasNextLine()) {
            String read = sc.nextLine();
            String[] split = read.split(split_exp);

            if (split[4].endsWith("BuyingEvent")) {//BuyingEvent
                event = new BuyingEvent(
                        Integer.parseInt(split[0]), //bid
                        split[2], Integer.parseInt(split[1]), //pid
                        //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//key_array
                        split[6],//price_array
                        split[7]  //qty_array
                );
            } else if (split[4].endsWith("AlertEvent")) {//AlertEvent
                event = new AlertEvent(
                        Integer.parseInt(split[0]), //bid
                        split[2], Integer.parseInt(split[1]), //pid
                        //bid_array
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

    private void dump(String file_name) throws IOException {

        File file = new File(Event_Path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist

        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(Event_Path + OsUtils.OS_wrapper(file_name))));

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
            prepare_input_events();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
