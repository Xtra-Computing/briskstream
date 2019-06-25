package compress;

import struct.Point;

/**
 *  https://github.com/uestc-db/traj-compression/blob/master/online/OPW/OPW.cpp
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
//
//    vector<int> OPW(double eplision){
//        int e = 0;
//        int originalIndex = 0;
//        vector<int> simplified;
//        simplified.push_back(originalIndex);
//        e = originalIndex + 2;
//        while (e < points.size()){
//            int i = originalIndex + 1;
//            bool condOPW = true;
//            while (i < e && condOPW){
//                if (cacl_PED(points[originalIndex], points[i], points[e]) > eplision)
//                    condOPW = false;
//                else
//                    i++;
//            }
//            if (!condOPW){
//                originalIndex = i;
//                simplified.push_back(originalIndex);
//                e = originalIndex + 2;
//            }
//            else
//                e++;
//        }
//        simplified.push_back(points.size() - 1);
//        return simplified;
//    }

    @Override
    public Point compress(Point point) {
        return null;
    }
}
