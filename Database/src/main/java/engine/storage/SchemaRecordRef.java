package engine.storage;

/**
 * A hack ref to SchemaRecord, simulating C++ pointer.
 */
public class SchemaRecordRef {
    private volatile SchemaRecord record;
    public int cnt = 0;
    private String name;

    public void setRecord(SchemaRecord record) {
        this.record = record;
        cnt++;
    }

    public SchemaRecord getRecord() {
        if (cnt == 0) {
            System.out.println("The record has not being assigned yet!");
            System.exit(-1);
        }
        return record;
    }


    /**
     * Read how many times.
     * @param name
     */
//    public void inc(String name) {
//        cnt++;
//
//        if (cnt != 1) {
//            System.nanoTime();
//        }
//        this.name = name;
//    }
}
