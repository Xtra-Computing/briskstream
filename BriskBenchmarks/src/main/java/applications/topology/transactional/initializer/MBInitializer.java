package applications.topology.transactional.initializer;

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
import engine.storage.datatype.StringDataBox;
import engine.storage.table.RecordSchema;
import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static applications.constants.MicroBenchmarkConstants.Constant.VALUE_LEN;
import static applications.param.MicroEvent.GenerateValue;
import static brisk.controller.affinity.SequentialBinding.next_cpu_for_db;
import static engine.profiler.Metrics.NUM_ITEMS;
import static utils.PartitionHelper.getPartition_interval;
import static xerial.jnuma.Numa.setLocalAlloc;

public class MBInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(MBInitializer.class);

    public MBInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        floor_interval = (int) Math.floor(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
    }

    /**
     * "INSERT INTO MicroTable (key, value_list) VALUES (?, ?);"
     */
    private void insertMicroRecord(int key, String value, int pid, SpinLock[] spinlock_) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new StringDataBox(value, value.length()));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("MicroTable", new TableRecord(schemaRecord, pid, spinlock_));

        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertMicroRecord(int key, String value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new StringDataBox(value, value.length()));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("MicroTable", new TableRecord(schemaRecord));

        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

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
            String value = GenerateValue(key);
            assert value.length() == VALUE_LEN;
            insertMicroRecord(key, value);
        }

        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);

    }

    @Override
    public void loadData(int thread_id, SpinLock[] spinlock_, TopologyContext context) {

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
            String value = GenerateValue(key);
            assert value.length() == VALUE_LEN;
            insertMicroRecord(key, value, pid, spinlock_);
        }

        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);


    }

    /**
     * Centrally load data.
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

            String value = GenerateValue(key);
            assert value.length() == VALUE_LEN;
            insertMicroRecord(key, value, pid, spinlock_);
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
     * Centrally load data.
     *
     * @param scale_factor
     * @param theta
     */
    public void loadData(double scale_factor, double theta) {
        int elements = (int) (NUM_ITEMS * scale_factor);
        int elements_per_socket;

        setLocalAlloc();

        if (OsUtils.isMac())
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock to 0.
        else
            AffinityLock.acquireLock(next_cpu_for_db());//same as lock to 0.

//        if (OsUtils.isMac())
//            elements_per_socket = elements;
//        else
        elements_per_socket = elements / 4;

        int i = 0;
        for (int key = 0; key < elements; key++) {


            String value = GenerateValue(key);
            assert value.length() == VALUE_LEN;
            insertMicroRecord(key, value);
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
    protected boolean load(String file) throws IOException {
        return false;
    }

    @Override
    protected void dump(String file_path) throws IOException {

    }

    @Override
    protected Object create_new_event(int number_partitions, int bid) {
        return null;
    }


    private RecordSchema MicroTableSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new StringDataBox());

        fieldNames.add("Key");//PK
        fieldNames.add("Value");

        return new RecordSchema(fieldNames, dataBoxes);
    }

    public void creates_Table() {
        RecordSchema s = MicroTableSchema();
        db.createTable(s, "MicroTable");
    }
}
