package applications.spout;

public class MemFileSpout_latency extends MemFileSpout {
    private long msgID_counter = 0;

    @Override
    public void nextTuple() {

        counter++;
        if (counter == array_array.length) {
            counter = 0;
        }
        collector.emit_nowait(array_array[counter], msgID_counter++, System.nanoTime());
    }

}

