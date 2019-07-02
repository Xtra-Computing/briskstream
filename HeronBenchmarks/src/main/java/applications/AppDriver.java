package applications;

import applications.topology.AbstractTopology;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.LinkedList;
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

    public void addApp(String name, Class<? extends AbstractTopology> cls) {
        applications.put(name, new AppDescriptor(cls));
    }

    public AppDescriptor getApp(String name) {
        return applications.get(name);
    }

    public static class AppDescriptor {
        private final Class<? extends AbstractTopology> cls;
        public LinkedList allocation;

        public AppDescriptor(Class<? extends AbstractTopology> cls) {
            this.cls = cls;
        }

        public StormTopology getTopology(String topologyName, Config config) {
            try {
                Constructor c = cls.getConstructor(String.class, Config.class);
                LOG.info("Loaded Brisk.topology {}", cls.getCanonicalName());
                //int allocation_plan= (int) config.get("allocation_plan");
                AbstractTopology topology = (AbstractTopology) c.newInstance(topologyName, config);
                topology.initialize();
                // this.allocation = Brisk.topology.ConfigAllocation(allocation_plan);
                return topology.buildTopology();
            } catch (ReflectiveOperationException ex) {
                LOG.error("Unable to load Brisk.topology class", ex);
                return null;
            }
        }
    }
}
