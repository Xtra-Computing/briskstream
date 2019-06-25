package compress;

import struct.Point;

public class UniformSampler extends Compressor {
    private final int skip_gap;//sampling interval.
    int cnt = 0;

    public UniformSampler(int skip_gap) {
        this.skip_gap = skip_gap;
    }

    @Override
    public Point compress(Point point) {
        if (cnt == skip_gap) {
            cnt = 0;
            return point;
        }
        cnt++;
        return null;//skip
    }
}
