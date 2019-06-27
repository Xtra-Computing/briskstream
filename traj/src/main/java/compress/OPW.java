package compress;

import struct.Point;

import java.util.ArrayList;
import java.util.List;

import static java.lang.StrictMath.abs;
import static java.lang.StrictMath.sqrt;

/**
 * https://github.com/uestc-db/traj-compression/blob/master/online/OPW/OPW.cpp
 * OPW [32] is a very early algorithm designed for online simpliﬁcation.
 * As a new point arrives in the buﬀer, it uses the new point and the ﬁrst point to build an anchor segment
 * and calculates the PED distance for all the points in the buﬀer.
 * If the maximum distance is larger, the new point is sampled; otherwise, the algorithm proceeds to the next incoming point.
 */
public class OPW extends Compressor {

    private final double epsilon;

    public OPW(double epsilon) {
        this.epsilon = epsilon;
    }

    int cnt = 0;
    int originalIndex = 0;


    List<Point> buffer = new ArrayList<>();

    @Override
    public Point compress(Point point) {
        if (cnt == 0) {
            buffer.add(point);
            cnt++;
            return point;//keep the first point.
        }

        int i = originalIndex + 1;
        boolean condOPW = true;
        while (i < cnt && condOPW){
            if (cacl_PED(buffer.get(originalIndex), buffer.get(i), buffer.get(cnt)) > epsilon)
                condOPW = false;
            else
                i++;
        }
        if (!condOPW){
            cnt = originalIndex + 2;
            return buffer.get(i);
        }
        else
            cnt++;
        return null;
    }

    double cacl_PED(Point s, Point m, Point e) {
        double A = e.lon - s.lon;
        double B = s.lat - e.lat;
        double C = e.lat * s.lon - s.lat * e.lon;
        if (A == 0 && B == 0)
            return 0;
        double shortDist = abs((A * m.lat + B * m.lon + C) / sqrt(A * A + B * B));
        return shortDist;
    }

}
