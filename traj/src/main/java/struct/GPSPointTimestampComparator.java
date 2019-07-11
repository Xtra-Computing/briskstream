package struct;

import java.util.Comparator;

public class GPSPointTimestampComparator implements Comparator<GPSPoint> {
    @Override
    public int compare(GPSPoint o1, GPSPoint o2) {
        Long diff = (o1.time.getTime() - o2.time.getTime());
        return diff.intValue();
    }
}
