package applications;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.applications.jobs.AbstractJob;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mayconbordin
 */
public class AppDriver {
    private static final Logger LOG = LoggerFactory.getLogger(AppDriver.class);
    private final Map<String, AppDescriptor> applications;

    public AppDriver() {
        applications = new HashMap<>();
    }

    public void addApp(String name, Class<? extends AbstractJob> cls) {
        applications.put(name, new AppDescriptor(cls));
    }

    public AppDescriptor getApp(String name) {
        return applications.get(name);
    }

    public static class AppDescriptor {
        private final Class<? extends AbstractJob> cls;

        public AppDescriptor(Class<? extends AbstractJob> cls) {
            this.cls = cls;
        }

        public AbstractJob getJob(JavaStreamingContext ssc, String topologyName, SparkConf config) {
            try {
                Constructor c = cls.getConstructor(String.class, SparkConf.class);
                LOG.info("Loaded Brisk.topology {}", cls.getCanonicalName());

                AbstractJob topology = (AbstractJob) c.newInstance(topologyName, config);
                topology.initialize(ssc);
                return topology.buildJob(ssc);
            } catch (ReflectiveOperationException ex) {
                LOG.error("Unable to load Brisk.topology class", ex);
            }
            return null;
        }
    }
}
