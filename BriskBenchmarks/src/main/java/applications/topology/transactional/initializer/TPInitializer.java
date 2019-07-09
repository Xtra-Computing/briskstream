package applications.topology.transactional.initializer;

import applications.util.Configuration;
import brisk.components.context.TopologyContext;
import engine.Database;
import engine.DatabaseException;
import engine.common.SpinLock;
import engine.storage.SchemaRecord;
import engine.storage.TableRecord;
import engine.storage.datatype.DataBox;
import engine.storage.datatype.DoubleDataBox;
import engine.storage.datatype.HashSetDataBox;
import engine.storage.datatype.StringDataBox;
import engine.storage.table.RecordSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static applications.constants.TP_TxnConstants.Constant.NUM_SEGMENTS;
import static utils.PartitionHelper.getPartition_interval;

public class TPInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TPInitializer.class);


    public TPInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, TopologyContext context) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == context.getNUMTasks() - 1) {//last executor need to handle left-over
            right_bound = NUM_SEGMENTS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }

        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);
            String _key = String.valueOf(key);
            insertSpeedRecord(_key, 0, pid, spinlock);
            insertCntRecord(_key, 0, pid, spinlock);
        }

        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }


    @Override
    public void loadDB(int thread_id, TopologyContext context) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == context.getNUMTasks() - 1) {//last executor need to handle left-over
            right_bound = NUM_SEGMENTS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }

        for (int key = left_bound; key < right_bound; key++) {

            String _key = String.valueOf(key);
            insertSpeedRecord(_key, 0);
            insertCntRecord(_key);
        }

        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);

    }


    private void insertCntRecord(String key, int value, int pid, SpinLock[] spinlock) {

        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new HashSetDataBox());
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_cnt", new TableRecord(schemaRecord, pid, spinlock));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }


    }

    private void insertSpeedRecord(String key, int value, int pid, SpinLock[] spinlock) {

        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new DoubleDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_speed", new TableRecord(schemaRecord, pid, spinlock));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }

    }


    private void insertCntRecord(String key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new HashSetDataBox());
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_cnt", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertSpeedRecord(String key, int value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new DoubleDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_speed", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }


    /**
     * not in use.
     *
     * @param scale_factor
     * @param theta
     * @param partition_interval
     * @param spinlock_
     */
    public void loadData_Central(double scale_factor, double theta, int partition_interval, SpinLock[] spinlock_) {

    }


    @Override
    public void loadData_Central(double scale_factor, double theta) {

    }

    private RecordSchema SpeedScheme() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new DoubleDataBox());

        fieldNames.add("Key");//PK
        fieldNames.add("Value");

        return new RecordSchema(fieldNames, dataBoxes);
    }


    private RecordSchema CntScheme() {

        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new HashSetDataBox());

        fieldNames.add("Key");//PK
        fieldNames.add("Value");

        return new RecordSchema(fieldNames, dataBoxes);
    }


    @Override
    protected boolean Prepared(String file) {
        return true;
    }

    @Override
    protected void store(String file_name) {

    }


    @Override
    protected Object create_new_event(int num_p, int bid) {
        return null;
    }


    protected String getConfigKey(String template) {
        return String.format(template, "tptxn");//TODO: make it flexible in future.
    }




    public void creates_Table(Configuration config) {
        RecordSchema s = SpeedScheme();
        db.createTable(s, "segment_speed");

        RecordSchema b = CntScheme();
        db.createTable(b, "segment_cnt");


//        try {
//            prepare_input_events(file, true);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}
