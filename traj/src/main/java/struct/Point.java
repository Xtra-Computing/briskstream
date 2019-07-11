package struct;

/**
 * Represents a spatial point.
 */
public class Point {

    // X is longitude and Y is latitude.
    final public double lon;
    final public double lat;

    public Point(double lon, double lat) {
        this.lon = lon;
        this.lat = lat;
    }

    @Override
    public String toString() {
        return "Point [lon=" + lon + ", lat=" + lat + "]";
    }

    public boolean isInvalid() {
        if (this.lon == -1 && this.lat == -1) {
            return true;
        }

        return false;
    }
}
