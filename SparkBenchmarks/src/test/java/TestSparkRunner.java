import applications.SparkRunner;
import applications.common.tools.OsUtils;
import org.junit.Test;

/**
 * Created by I309939 on 8/4/2016.
 */
public class TestSparkRunner {
    @Test
    public void Test() {
        String[] args;
        if (OsUtils.isWindows()) {
            args = new String[]{
                    "-m",
                    "local",
                    "-bt",
                    "1000",//mock number of RDD per batch
                    "-bt_duration",
                    "100",
                    "--runtime",
                    "60",
                    "-log",
                    "standout",
                    "-ct1",
                    "4",
                    "--task_type",
                    "0",
                    "--size_state",
                    "8",
            };
        } else {
            args = new String[]{
                    "-m",
                    "local",
                    "-bt_duration",
                    "1000",
                    "--runtime",
                    "20",
                    "-log",
                    "standout",
                    "-ct1",
                    "32",
                    "--task_type",
                    "0",
                    "--size_state",
                    "8",
            };
        }


        try {
            SparkRunner.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//
//    @Test
//    public void Test_verbose() {
//        String[] args;
//        if (OsUtils.isWindows()) {
//            args = new String[]{
//                    "-m",
//                    "local",
//                    "-bt",
//                    "1000",//mock number of RDD per batch
//                    "-bt_duration",
//                    "100",
//                    "--runtime",
//                    "60",
//                    "-log",
//                    "standout",
//                    "-ct1",
//                    "4",
//                    "--task_type",
//                    "0",
//                    "--size_state",
//                    "8",
//                    "--verbose"
//            };
//
//            try {
//                SparkRunner.main(args);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
}
