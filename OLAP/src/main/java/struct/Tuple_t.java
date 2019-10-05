package struct;

/**
 * 8 byte tuple
 */
public class Tuple_t {

    public int key;//4 byte key
    public int payload;//4 byte value

    public static int size() {
        return 8;
    }
}
