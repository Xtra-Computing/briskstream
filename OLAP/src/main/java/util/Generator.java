package util;

import struct.Relation_t;
import struct.Tuple_t;

import java.util.Random;

public class Generator {


    static Random r;
    private static int seedValue;
    private static boolean seeded = false;

    public static void seed_generator(int seed) {

        srand(seed);
        seedValue = seed;
        seeded = true;

    }


    public static void create_relation_nonunique(Relation_t relation, int num_tuples, int maxid) {

        check_seed();
        relation.num_tuples = num_tuples;
        relation.tuples = new Tuple_t[num_tuples];

        random_gen(relation, maxid);

    }

    /**
     * Generate tuple IDs -> random distribution
     * relation must have been allocated
     *
     * @param relation
     * @param maxid
     */
    private static void random_gen(Relation_t relation, int maxid) {
        int i;
        for (i = 0; i < relation.num_tuples; i++) {
            relation.tuples[i].key = r.nextInt(maxid);
            relation.tuples[i].payload = i;
        }
    }

    /**
     * Check wheter seeded, if not seed the generator with current time
     */
    private static void check_seed() {
        if (!seeded) {
            seedValue = (int) System.currentTimeMillis();
            srand(seedValue);
            seeded = true;
        }

    }

    public static void load_relation(Relation_t relation, String filename, int num_tuples) {

        relation.num_tuples = num_tuples;

        relation.tuples = new Tuple_t[num_tuples];//relation->tuples = (tuple_t*) MALLOC(num_tuples * sizeof(tuple_t));

        read_relation(relation, filename);

    }

    /**
     * TODO
     *
     * @param relation
     * @param filename
     */
    private static void read_relation(Relation_t relation, String filename) {

        throw new UnsupportedOperationException();
    }

    private static void srand(int seed) {
        r = new Random(seed);
    }

}
