package brisk.components.windowing;


/**
 * Watermark event used for tracking progress of time when
 * processing event based ts.
 */
public class WaterMarkEvent<T> extends BasicEvent<T> {
    public WaterMarkEvent(long ts) {
        super(null, ts);
    }

    @Override
    public boolean isWatermark() {
        return true;
    }

    @Override
    public String toString() {
        return "WaterMarkEvent{} " + super.toString();
    }
}
