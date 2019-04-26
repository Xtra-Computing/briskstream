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
import engine.storage.datatype.IntDataBox;
import engine.storage.datatype.ListDoubleDataBox;
import engine.storage.table.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static applications.constants.PositionKeepingConstants.Constant.MOVING_AVERAGE_WINDOW;
import static applications.constants.PositionKeepingConstants.Constant.NUM_MACHINES;


public class PKInitializer extends TableInitilizer {


    public PKInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
    }


    private SchemaRecord PK_Event(int key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new ListDoubleDataBox(MOVING_AVERAGE_WINDOW));
        values.add(new DoubleDataBox());

        return new SchemaRecord(values);
    }

    /**
     * "INSERT INTO MicroTable (key, value_list) VALUES (?, ?);"
     */
    private void insertRecord(int key) {

        try {
            db.InsertRecord("machine", new TableRecord(PK_Event(key)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }


    /**
     * "INSERT INTO MicroTable (key, value_list) VALUES (?, ?);"
     */
    private void insertRecord(int key, int pid, SpinLock[] spinlock_) {

        try {
            db.InsertRecord("machine", new TableRecord(PK_Event(key), pid, spinlock_));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    public void loadData_Central(double scale_factor, double theta) {

        for (int key = 0; key < NUM_MACHINES * scale_factor; key++) {
            insertRecord(key);
        }
    }

    @Override
    protected boolean load(String file) throws IOException {
        return false;
    }

    @Override
    protected void store(String file_path) throws IOException {

    }

    @Override
    protected Object create_new_event(int number_partitions, int bid) {
        return null;
    }

    @Override
    public void loadData_Central(double scale_factor, double theta, int partition_interval, SpinLock[] spinlock_) {

        for (int key = 0; key < NUM_MACHINES; key++) {
            int pid = get_pid(partition_interval, key);
            insertRecord(key, pid, spinlock_);
        }
    }

    private RecordSchema MachineTableSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new ListDoubleDataBox(MOVING_AVERAGE_WINDOW));
        dataBoxes.add(new DoubleDataBox());

        fieldNames.add("Key");//PK:  device ID.
        fieldNames.add("Value");// list of machine value_list temperature.
        fieldNames.add("Sum");// sum of up-to-date value_list.


        return new RecordSchema(fieldNames, dataBoxes);
    }

    public void creates_Table() {
        RecordSchema s = MachineTableSchema();
        db.createTable(s, "machine");
    }

    @Override
    public void loadData(int thread_id, TopologyContext context) {

    }

    @Override
    public void loadData(int thread_id, SpinLock[] spinlock, TopologyContext context) {
        throw new UnsupportedOperationException();
    }


}
