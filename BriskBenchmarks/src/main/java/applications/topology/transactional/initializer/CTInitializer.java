package applications.topology.transactional.initializer;

import applications.param.DepositEvent;
import applications.param.TransactionEvent;
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
import org.jetbrains.annotations.NotNull;
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
import static applications.constants.CrossTableConstants.Constant.*;
import static applications.topology.transactional.State.partioned_store;
import static brisk.controller.affinity.SequentialBinding.next_cpu_for_db;
import static utils.PartitionHelper.getPartition_interval;
import static xerial.jnuma.Numa.setLocalAlloc;

public class CTInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(CTInitializer.class);


    public CTInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
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

    @NotNull
    private RecordSchema getRecordSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new LongDataBox());

        fieldNames.add("Key");//PK
        fieldNames.add("Value");

        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema AccountsScheme() {
        return getRecordSchema();
    }


    private RecordSchema BookEntryScheme() {
        return getRecordSchema();
    }


    @Override
    protected boolean load(String file) throws IOException {

        if (Files.notExists(Paths.get(Event_Path + OsUtils.OS_wrapper(file))))
            return false;

        Scanner sc;
        sc = new Scanner(new File(Event_Path + OsUtils.OS_wrapper(file)));

        Object event = null;
        while (sc.hasNextLine()) {
            String read = sc.nextLine();
            String[] split = read.split(split_exp);

            if (split[4].endsWith("DepositEvent")) {//DepositEvent
                event = new DepositEvent(
                        Integer.parseInt(split[0]), //bid
                        Integer.parseInt(split[1]), //pid
                        split[2], //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//getAccountId
                        split[6],//getBookEntryId
                        Integer.parseInt(split[7]),  //getAccountTransfer
                        Integer.parseInt(split[8])  //getBookEntryTransfer
                );
            } else if (split[4].endsWith("TransactionEvent")) {//TransactionEvent
                event = new TransactionEvent(
                        Integer.parseInt(split[0]), //bid
                        Integer.parseInt(split[1]), //pid
                        split[2], //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//getSourceAccountId
                        split[6],//getSourceBookEntryId
                        split[7],//getTargetAccountId
                        split[8],//getTargetBookEntryId
                        Integer.parseInt(split[9]),  //getAccountTransfer
                        Integer.parseInt(split[10])  //getBookEntryTransfer
                );
            }
            db.eventManager.put(event, Integer.parseInt(split[0]));
        }
        return true;
    }

    @Override
    protected void dump(String file_name) throws IOException {
        File file = new File(Event_Path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist

        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(Event_Path + OsUtils.OS_wrapper(file_name))));

        for (Object event : db.eventManager.input_events) {

            StringBuilder sb = new StringBuilder();
            if (event instanceof DepositEvent) {
                sb.append(((DepositEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((DepositEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((DepositEvent) event).num_p());////3 num of p
                sb.append(split_exp);
                sb.append("DepositEvent");//event types.
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getAccountId());//5
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getBookEntryId());//6
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getAccountTransfer());//7
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getBookEntryTransfer());//8

            } else if (event instanceof TransactionEvent) {
                sb.append(((TransactionEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getPid());//1 -- pid.
                sb.append(split_exp);
                sb.append(Arrays.toString(((TransactionEvent) event).getBid_array()));//2 -- bid array
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).num_p());//3 num of p
                sb.append(split_exp);
                sb.append("TransactionEvent");//event types.
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getSourceAccountId());//5
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getSourceBookEntryId());//6
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getTargetAccountId());//7
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getTargetBookEntryId());//8
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getAccountTransfer());//9
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getBookEntryTransfer());//10
            }

            w.write(sb.toString() + "\n");
        }
        w.close();
    }






    private Object randomTransactionEvent(int partition_id, long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {
        final long accountsTransfer = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long transfer = rnd.nextLong(MAX_BOOK_TRANSFER);

        while (!Thread.currentThread().isInterrupted()) {
            int _pid = partition_id;

            final int sourceAcct = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;

            if (number_of_partitions > 1) {//multi-partition
                _pid++;
                if (_pid == tthread)
                    _pid = 0;
            }

            final int targetAcct = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;

            if (number_of_partitions > 1) {//multi-partition
                _pid++;
                if (_pid == tthread)
                    _pid = 0;
            }

            final int sourceBook = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;

            if (number_of_partitions > 1) {//multi-partition
                _pid++;
                if (_pid == tthread)
                    _pid = 0;
            }

            final int targetBook = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;

            if (sourceAcct == targetAcct || sourceBook == targetBook) {
                continue;
            }
            return new TransactionEvent(
                    bid,
                    partition_id,
                    bid_array,
                    number_of_partitions,
                    ACCOUNT_ID_PREFIX + sourceAcct,
                    BOOK_ENTRY_ID_PREFIX + sourceBook,
                    ACCOUNT_ID_PREFIX + targetAcct,
                    BOOK_ENTRY_ID_PREFIX + targetBook,
                    accountsTransfer,
                    transfer,
                    MIN_BALANCE);
        }

        return null;
    }

    private Object randomDepositEvent(int partition_id,
                                      long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {


        int _pid = partition_id;

        //key
        final int account = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;

        if (number_of_partitions > 1) {//multi-partition
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }

        final int book = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;


        //value_list
        final long accountsDeposit = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long deposit = rnd.nextLong(MAX_BOOK_TRANSFER);

        return new DepositEvent(
                bid,
                partition_id,
                bid_array,
                number_of_partitions,
                ACCOUNT_ID_PREFIX + account,
                BOOK_ENTRY_ID_PREFIX + book,
                accountsDeposit,
                deposit);
    }


    @Override
    protected Object create_new_event(int num_p, int bid) {
        int flag = next_decision2();
        if (flag == 0) {

            if (num_p != 1)
                return randomDepositEvent(p, p_bid.clone(), 2, bid, rnd);
            return randomDepositEvent(p, p_bid.clone(), 1, bid, rnd);
        } else if (flag == 1) {
            if (num_p != 1)
                return randomTransactionEvent(p, p_bid.clone(), 4, bid, rnd);
            return randomTransactionEvent(p, p_bid.clone(), 1, bid, rnd);
        }
        return null;
    }


    public void creates_Table() {
        RecordSchema s = AccountsScheme();
        db.createTable(s, "accounts");

        RecordSchema b = BookEntryScheme();
        db.createTable(b, "bookEntries");

        try {
            prepare_input_events("CT_Events");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
