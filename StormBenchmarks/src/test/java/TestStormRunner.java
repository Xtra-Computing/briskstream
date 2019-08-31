import applications.StormRunner;
import org.junit.Test;

/**
 * Created by I309939 on 8/4/2016.
 */
public class TestStormRunner {
    private String topic = "topic";

    @Test
    public void Test() {
        System.out.println("Start test clean;");
        String[] args;
        args = new String[]{
                "-a",
                "LogProcessing",
                "-tt",
                "25"
        };
        try {
            StormRunner.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
