package streaming;

import com.javamex.classmexer.MemoryUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Random;

/**
 * Created by shuhaozhang on 10/7/16.
 */

public class Tests {
    private final static Logger LOG = LoggerFactory.getLogger(Tests.class);

//

    private byte[] convertToBytes(Object object) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            return bos.toByteArray();
        }
    }

    @Test
    public void TestMeasure() throws IOException {
        Random r = new Random();
        char[] string = new char[1];
        for (int i = 0; i < 1; i++) {
            string[i] = ' ';
        }

        LOG.info("1 char deep size:" + String.valueOf(MemoryUtil.deepMemoryUsageOf(string, MemoryUtil.VisibilityFilter.ALL)));

        long[] number = new long[100];

        for (int i = 0; i < 100; i++) {
            number[i] = r.nextLong();
        }
        LOG.info("100 long deep size:" + String.valueOf(MemoryUtil.deepMemoryUsageOf(number, MemoryUtil.VisibilityFilter.ALL)));
        LOG.info("100 long deep size:" + convertToBytes(number).length);

    }

//	@Test
//	public void TestTopology() {
//		LOG.info("==========TEST simple topology execution================");
//		applications.Platform p = new HUAWEI_Machine();
//		Configuration conf = new Configuration();
//		conf.put("Fault_tolerance", false);
//		conf.put("batch", 2);
//		conf.put("machine", 0);
//		conf.put("shared", true);
//		conf.put("common", false);
//		conf.put("num_cpu", 8);
//		conf.put("num_socket", 1);
//		conf.put("NAV", true);
//		Topology tp = new demoTopology_testNormal().getTopo();
//		tp.addMachine(p);
//		TopologySubmitter submit = new TopologySubmitter();
//
//		submit.submitTopology(tp, conf);
//		try {
//			Thread.sleep((long) 1000 * 1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//
//	@Test
//	public void TestFTTopology() {
//		LOG.info("==========TEST Fault tolerant topology execution================");
//		applications.Platform p = new HUAWEI_Machine();
//		Configuration conf = new Configuration();
//		conf.put("Fault_tolerance", true);
//		conf.put("batch", 5);
//		conf.put("machine", 0);
//		conf.put("shared", true);
//		conf.put("common", false);
//		conf.put("num_cpu", 8);
//		conf.put("num_socket", 1);
//		Topology tp = new demoTopology_testFT().getTopo();
//		tp.addMachine(p);
//		TopologySubmitter submit = new TopologySubmitter();
//
//		submit.submitTopology(tp, conf);
//		try {
//			Thread.sleep((long) 1000 * 1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}

//	@Test
//	public void TestAffinity1() {
//		BasicConfigurator.configure();
//		applications.Platform p = new HUAWEI_Machine();
//		Configuration conf = new Configuration();
//		conf.put("Fault_tolerance", true);
//		conf.put("batch", 5);
//		conf.put("machine", 0);
//
//		for (int j = 0; j < 1; j++) {//repeat
//			for (int i = 3; i <= 3; i++) {
//				AffinityLock.reset();
//				conf.put("which_queue", i);
//				AffinityTests sa;
//				for (int w = 0; w < 3; w++) {
//					sa = new AffinityTests(conf);
//					sa.warmup();
//				}
//				sa = new AffinityTests(conf);
//				sa.case1();
//			}
//		}
//	}
//
//	@Test
//	public void TestAffinity2() {
//		BasicConfigurator.configure();
//		applications.Platform p = new HUAWEI_Machine();
//		Configuration conf = new Configuration();
//		conf.put("Fault_tolerance", true);
//		conf.put("batch", 5);
//		conf.put("machine", 0);
//
//		for (int j = 0; j < 1; j++) {//repeat
//			for (int i = 3; i <= 3; i++) {
//				AffinityLock.reset();
//				conf.put("which_queue", i);
//				AffinityTests sa;
//				for (int w = 0; w < 3; w++) {
//					sa = new AffinityTests(conf);
//					sa.warmup();
//				}
//				if (!OsUtils.isMac()) {
//					sa = new AffinityTests(conf);
//					sa.case2();
//				}
//			}
//		}
//	}


//    @Test
//    public void TestAddresser() {
//        try {
//            Addresser.main();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}

