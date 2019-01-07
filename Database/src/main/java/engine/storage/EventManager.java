package engine.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventManager {
    private transient final static Logger LOG = LoggerFactory.getLogger(EventManager.class);
    public Object[] input_events;
    private int num_events;

    public void ini(int num_events) {
        input_events = new Object[num_events];
        this.num_events = num_events;
    }

    public Object get(int bid) {
        return input_events[bid];
    }

    public void put(Object event, int i) {
        input_events[i] = event;
    }
}
