package engine.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventManager {
    public Object[] input_events;
    private int total_num_events = 0;

    public void ini(int num_events) {
        input_events = new Object[num_events];
        this.total_num_events = num_events;
    }

    public Object get(int bid) {
        return input_events[bid % total_num_events];
    }

    public void put(Object event, int i) {
        input_events[i] = event;
    }
}
