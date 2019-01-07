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
import engine.storage.datatype.LongDataBox;
import engine.storage.datatype.StringDataBox;
import engine.storage.table.RecordSchema;
import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static applications.constants.CrossTableConstants.Constant.*;
import static brisk.controller.affinity.SequentialBinding.next_cpu_for_db;
import static utils.PartitionHelper.getPartition_interval;
import static xerial.jnuma.Numa.setLocalAlloc;

public class CTInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(CTInitializer.class);

    public CTInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        floor_interval = (int) Math.floor(NUM_ACCOUNTS / (double) tthread);//NUM_ITEMS / tthread;
    }


    @Override
    public void loadData(int thread_id, TopologyContext context) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == context.getNUMTasks() - 1) {//last executor need to handle left-over
            right_bound = NUM_ACCOUNTS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }

        for (int key = left_bound; key < right_bound; key++) {

            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, 0);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, 0);
        }

        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);

    }

    @Override
    public void loadData(int thread_id, SpinLock[] spinlock, TopologyContext context) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == context.getNUMTasks() - 1) {//last executor need to handle left-over
            right_bound = NUM_ACCOUNTS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }

        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);

            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, 0, pid, spinlock);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, 0, pid, spinlock);
        }

        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAccountRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("accounts", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertAccountRecord(String key, long value, int pid, SpinLock[] spinlock_) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("accounts", new TableRecord(schemaRecord, pid, spinlock_));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private SchemaRecord AssetRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        return new SchemaRecord(values);
    }

    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAssetRecord(String key, long value) {

        try {
            db.InsertRecord("bookEntries", new TableRecord(AssetRecord(key, value)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }


    private void insertAssetRecord(String key, long value, int pid, SpinLock[] spinlock_) {

        try {
            db.InsertRecord("bookEntries", new TableRecord(AssetRecord(key, value), pid, spinlock_));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    //    private String rightpad(String text, int length) {
//        return String.format("%-" + length + "." + length + "s", text);
//    }
//
    private String GenerateKey(String prefix, int key) {
//        return rightpad(prefix + String.valueOf(key), VALUE_LEN);
        return prefix + String.valueOf(key);
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
        int elements = (int) (NUM_ACCOUNTS * scale_factor);
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

            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);

            insertAccountRecord(_key, 0, pid, spinlock_);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);

            insertAssetRecord(_key, 0, pid, spinlock_);


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
        int elements = (int) (NUM_ACCOUNTS * scale_factor);
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

            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, 0);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, 0);

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

    private RecordSchema AccountsScheme() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new LongDataBox());

        fieldNames.add("Key");//PK
        fieldNames.add("Value");

        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema BookEntryScheme() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new LongDataBox());

        fieldNames.add("Key");//PK
        fieldNames.add("Value");

        return new RecordSchema(fieldNames, dataBoxes);
    }


    public void creates_Table() {
        RecordSchema s = AccountsScheme();
        db.createTable(s, "accounts");

        RecordSchema b = BookEntryScheme();
        db.createTable(b, "bookEntries");
    }

}
