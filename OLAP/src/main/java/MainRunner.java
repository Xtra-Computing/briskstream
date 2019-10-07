import algo.st.RJ;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import struct.Algo_t;
import struct.Relation_t;
import struct.Result_t;
import struct.Tuple_t;

import static util.Generator.*;


/**
 * - NPO:    No Partitioning Join Optimized (Hardware-oblivious algo. in paper)
 * - PRO:    Parallel Radix Join Optimized (Hardware-conscious algo. in paper)
 * - PRH:    Parallel Radix Join Histogram-based
 * - PRHO:   Parallel Radix Join Histogram-based Optimized
 * - RJ:     Radix Join (single-threaded)
 * - NPO_st: No Partitioning Join Optimized (single-threaded)
 */
public class MainRunner {
    private static final Logger LOG = LoggerFactory.getLogger(MainRunner.class);
    @Parameter(names = {"--algo"}, description = "which algorithm to use")
    public static String algo_name = "RJ";

    @Parameter(names = {"--nthreads"}, description = "number of threads")
    public static int nthreads = 2;

    @Parameter(names = {"--rsize"}, description = "number of tuples in R")
    public static int rsize = 128_000;

    @Parameter(names = {"--ssize"}, description = "number of tuples in S")
    public static int ssize = 128_000;

    @Parameter(names = {"--rseed"}, description = "generate seed in R")
    public static int rseed = 12345;

    @Parameter(names = {"--sseed"}, description = "generate seed in S")
    public static int sseed = 54321;

    @Parameter(names = {"--skew"}, description = "key skew")
    public static float skew = 0.0f;

    @Parameter(names = {"--nonunique"}, description = "non-unique keys allowed?")
    public static boolean nonunique = true;

    @Parameter(names = {"--fullrange"}, description = "keys covers full int range?")
    public static boolean fullrange = false;

    @Parameter(names = {"--loadR"}, description = "filename, which load R from")
    public static String loadR = null;


    @Parameter(names = {"--loadS"}, description = "filename, which load S from")
    public static String loadS = null;

    public static void main(String[] args) {

        MainRunner runner = new MainRunner();
        JCommander cmd = new JCommander(runner);

        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
        }

        /**
         * Initialize join algorithm
         */


        Algo_t algo = null;

        switch (algo_name) {
            case "RJ": {
                algo = new Algo_t(new RJ());
                LOG.info("Selected algorithm:" + algo.getName());
                break;
            }
            default:
                throw new UnsupportedOperationException();
        }


        Relation_t relR = new Relation_t();
        Relation_t relS = new Relation_t();
        Result_t results;


        /**
         * TODO: schedule main thread to CPU-0.
         */

        /* create relation R */
        LOG.info(String.format("%s relation R with size = %.3f MiB, #tuples = %d : ", (loadR != null) ? ("Loading") : ("Creating"), (double) Tuple_t.size() * rsize / 1024.0 / 1024.0, rsize));

        seed_generator(rseed);

        create_relation(relR, loadR, rsize);

        LOG.info("Creating relation R finished.");


        /* create relation S */
        LOG.info(String.format("%s relation S with size = %.3f MiB, #tuples = %d : ", (loadS != null) ? ("Loading") : ("Creating"), (double) Tuple_t.size() * ssize / 1024.0 / 1024.0, ssize));

        seed_generator(sseed);

        create_relation(relS, loadS, ssize);

        LOG.info("Creating relation S finished.");

        /* Run the selected join algorithm */
        LOG.info("Running join algorithm %s ...\n", algo);

        results = algo.join(relR, relS, nthreads);

        LOG.info("Results = %d. Done.", results.totalresults);


    }

    private static void create_relation(Relation_t relation, String load_relation, int relation_size) {
        if (load_relation != null) {
            /* load relation from file */
            load_relation(relation, load_relation, relation_size);
        } else if (fullrange) {
            create_relation_nonunique(relation, relation_size, Integer.MAX_VALUE);
        } else if (nonunique) {
            create_relation_nonunique(relation, relation_size, relation_size);
        } else {
            throw new UnsupportedOperationException();//parallel_create_relation(&relR, cmd_params.r_size, nthreads, cmd_params.r_size);
        }
    }


}
