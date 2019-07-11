package struct;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Trajectory implements Serializable {
    private String taxi_id;
    private String taxi_sub_id;
    private long timestamp_start;
    private long timestamp_end;
    public List<GPSPoint> points;

    public Trajectory(String taxi_id, String taxi_sub_id, long timestamp_start, long timestamp_end, List<GPSPoint> points) {

        this.taxi_id = taxi_id;
        this.taxi_sub_id = taxi_sub_id;
        this.timestamp_start = timestamp_start;
        this.timestamp_end = timestamp_end;
        this.points = points;
    }

    public Trajectory(String taxi_id, String taxi_sub_id, List<GPSPoint> points) {
        this(taxi_id, taxi_sub_id, 0L, 0L, points);
    }

    public Trajectory(Long taxi_id, int taxi_sub_id, List<GPSPoint> points) {
        this(String.valueOf(taxi_id), String.valueOf(taxi_sub_id), points);
    }

    public Trajectory(String taxi_id, String taxi_sub_id) {
        this(taxi_id, taxi_sub_id, new ArrayList<>());
    }

    public Trajectory(String taxi_id, List<GPSPoint> points) {
        this(taxi_id, String.valueOf(0), points);
    }

    public Trajectory(Long taxi_id, List<GPSPoint> points) {
        this(String.valueOf(taxi_id), points);
    }

    public Trajectory(List<GPSPoint> points) {
        this("-1", "1", points);
    }

    public Trajectory(Trajectory trajectory) {
        this(trajectory.getTaxi_id(), trajectory.getTaxi_sub_id(), trajectory.getTimestamp_start(), trajectory.getTimestamp_end(), new ArrayList<>(trajectory.points));
    }

    public String getID() {
        return taxi_id + "_" + taxi_sub_id;
    }

    public void AddPoint(GPSPoint gpsPoint) {
        points.add(gpsPoint);
    }

    public List<GPSPoint> getPoints() {
        return points;
    }

    public void addPoint(GPSPoint point) {
        points.add(point);
    }

    public String getTaxi_sub_id() {
        return taxi_sub_id;
    }

    public void setTaxi_sub_id(Long taxi_sub_id) {
        this.taxi_sub_id = String.valueOf(taxi_sub_id);
    }

    public void setTaxi_id(String taxi_id) {
        this.taxi_id = taxi_id;
    }

    public String getTaxi_id() {
        return taxi_id;
    }

    public long getTimestamp_start() {
        return timestamp_start;
    }

    public long getTimestamp_end() {
        return timestamp_end;
    }
}
