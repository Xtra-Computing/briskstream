package brisk.components.windowing;

import brisk.execution.runtime.tuple.impl.Tuple;

public interface TimestampExtractor {
    long extractTimestamp(Tuple tuple);
}
