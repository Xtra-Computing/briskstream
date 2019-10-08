package struct;

import static parameters.NPJ_params.BUCKET_SIZE;

public class NPJ_typles {


    /**
     * TODO: be very careful about this..
     * The original method requires unsight int, but Java does not support it.
     *
     * @param key
     * @param hashmask
     * @param skipbits
     * @return
     */
    public static int HASH(int key, int hashmask, int skipbits) {
        return (((key) & hashmask) >> skipbits);
    }

    public static class Hashtable_t {
        public int num_buckets;
        public Bucket_t[] buckets;
        public int skip_bits;
        public int hash_mask;


        public void initialize_buckets() {
            buckets = new Bucket_t[num_buckets];

            /* TODO: allocate hashtable buckets cache line aligned */

            /* TODO: if numa localize*/

//            memset(ht->buckets, 0, ht->num_buckets * sizeof(bucket_t)); inits everything to 0. seems not needed.

            /**
             * Skipbits parameter defines number of least-significant bits which
             * will be discarded before computing the hash.
             */
            this.skip_bits = 0;/* the default for modulo hash */
            this.hash_mask = (this.num_buckets - 1) << this.skip_bits;
        }
    }

    /**
     * Normal hashtable buckets.
     * <p>
     * if KEY_8B then key is 8B and sizeof(bucket_t) = 48B
     * else key is 16B and sizeof(bucket_t) = 32B
     */
    public static class Bucket_t {
        volatile char latch;
        public int count;
        public Tuple_t[] tuples = new Tuple_t[BUCKET_SIZE];
        public Bucket_t next;//next bucket.
    }
}
