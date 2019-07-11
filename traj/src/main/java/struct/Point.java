package struct;

/**
 * Represents a spatial point.
 */
public class Point {

    // X is longitude and Y is latitude.
    final public double x;
    final public double y;

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return "Point [x=" + x + ", y=" + y + "]";
    }

    public boolean isInvalid() {
        if (this.x == -1 && this.y == -1) {
            return true;
        }

        return false;
    }
}
