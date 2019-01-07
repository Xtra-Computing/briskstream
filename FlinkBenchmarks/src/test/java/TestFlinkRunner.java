import applications.FlinkRunner;
import org.junit.Test;

/**
 * Created by I309939 on 8/4/2016.
 */
public class TestFlinkRunner {
    private String topic = "topic";

    @Test
    public void Test() {
        String[] args;
        args = new String[]{
                "  --microbenchmark",
                "-a",
                "WordCount",
                "-tt",
                "25"
        };

        try {
            FlinkRunner.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
