package engine.storage;

/**
 * A hack ref to SchemaRecord, simulating C++ pointer.
 */
public class SchemaRecordRef {
    volatile public SchemaRecord record;
//    public int cnt = 0;
    private String name;

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
