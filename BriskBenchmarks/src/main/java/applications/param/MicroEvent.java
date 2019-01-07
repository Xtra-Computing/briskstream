package applications.param;

import engine.profiler.Metrics;
import engine.storage.SchemaRecordRef;
import engine.storage.datatype.DataBox;
import engine.storage.datatype.IntDataBox;
import engine.storage.datatype.StringDataBox;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

import static applications.constants.MicroBenchmarkConstants.Constant.VALUE_LEN;

/**
 * Support Multi workset since 1 SEP 2018.
 */
public class MicroEvent {


    private final int[] keys;
    private final SchemaRecordRef[] record_refs;//this is essentially the place-holder..
    public double[] enqueue_time = new double[1];
    //    public double[] useful_time = new double[1];
    public double[] index_time = new double[1];
    public int sum;
    long emit_timestamp = 0;
    long bid;//event sequence, shall be set by event sequencer.
    boolean flag;//true: write, false: read.
    private List<DataBox>[] value = new ArrayList[Metrics.NUM_ACCESSES];//Note, it should be arraylist instead of linkedlist as there's no add/remove later.


    public MicroEvent(boolean flag, int[] keys, int numAccess) {
        this.flag = flag;
        this.keys = keys;
        record_refs = new SchemaRecordRef[numAccess];
        for (int i = 0; i < numAccess; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        setValues(getKeys());
    }

    public void setEmit_timestamp(long emit_timestamp) {
        this.emit_timestamp = emit_timestamp;
    }
//    private final List<DataBox>[] values;

    public long getEmit_timestamp() {
        return emit_timestamp;
    }

    public int[] getKeys() {
        return keys;
    }

    public List<DataBox>[] getValues() {
        return value;
    }

    public SchemaRecordRef[] getRecord_refs() {
        return record_refs;
    }

    public long getBid() {
        return bid;//act as bid..
    }

    public void setBid(long bid) {
        this.bid = bid;
    }

    public boolean isFlag() {
        return flag;
    }

    private static String rightpad(@NotNull String text, int length) {
        return StringUtils.leftPad(text, length); // Returns "****foobar"
//        return String.format("%-" + length + "." + length + "s", text);
    }

    public static String GenerateValue(int key) {
        return rightpad(String.valueOf(key), VALUE_LEN);
    }

    private void set_values(int access_id, int key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));//key
        values.add(1, new StringDataBox(GenerateValue(key), VALUE_LEN));//value_list
        value[access_id] = values;
    }


    public void setValues(int[] keys) {
        for (int access_id = 0; access_id < Metrics.NUM_ACCESSES; ++access_id) {
            set_values(access_id, keys[access_id]);
        }
    }

}