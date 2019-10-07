package struct;

import static parameters.NPJ_params.BUCKET_SIZE;

public class NPJ_typles {

    public static class Hashtable_t {
        public int num_buckets;
        public Bucket_t[] buckets;
        private int skip_bits;
        private int hash_mask;


        public void initialize_buckets() {
            buckets = new Bucket_t[num_buckets];

            /* TODO: allocate hashtable buckets cache line aligned */

            /* TODO: if numa localize*/

//            memset(ht->buckets, 0, ht->num_buckets * sizeof(bucket_t)); inits everything to 0. seems not needed.

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
    private static class Bucket_t {
        volatile char latch;
        int count;
        Tuple_t[] tuples = new Tuple_t[BUCKET_SIZE];
        Bucket_t next;//next bucket.
    }
}
