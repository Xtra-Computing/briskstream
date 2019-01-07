package engine.transaction.dedicated.ordered;

import java.util.concurrent.ConcurrentSkipListSet;

public class MyList<O> extends ConcurrentSkipListSet<O> {
    public int current_item = 0;
}
