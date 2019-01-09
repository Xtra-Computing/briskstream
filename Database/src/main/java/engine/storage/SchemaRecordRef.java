package engine.storage;

/**
 * A hack ref to SchemaRecord, simulating C++ pointer.
 */
public class SchemaRecordRef {
    volatile public SchemaRecord record;
}
