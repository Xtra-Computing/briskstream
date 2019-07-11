package main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import struct.GPSPoint;
import struct.GPSPointTimestampComparator;
import struct.Trajectory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        ParseSGTrajectoryData("");
    }


    /**
     * Function to parse SG Trajectoy Data. The data is in csv file with the following format:
     * index,driver_id,booking_code,pick_up_time,close_time,Latitude,Longitude,Timestamp
     *
     * @param data_location : The location of the data csv files.
     * @return : The generated trajectory map.
     */
    public static void ParseSGTrajectoryData(String data_location) {
        Map<String, Trajectory> trajectory_map = new HashMap<>();

        //Building the Trajectory Map
        Path dir = Paths.get(data_location);

        int count = 0;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path child : ds) {
                if (count % 10000 == 0) {
                    LOG.info(count + " trajectories read.");
                }
                count++;
                if (!Files.isDirectory(child)) {

                    Scanner sc = new Scanner(child.toFile());
                    List<GPSPoint> trajectory_points = new ArrayList<>();
                    FileWriter csvWriter = new FileWriter(data_location + "sorted/" + child.getFileName());
                    while (sc.hasNextLine()) {
                        String[] strings = sc.nextLine().split(",");
                        double lon = Double.parseDouble(strings[0]);
                        double lat = Double.parseDouble(strings[1]);
                        Long timestamp = Long.parseLong(strings[2]) * 1000;

                        trajectory_points.add(new GPSPoint(timestamp, lon, lat));
                    }

                    Collections.sort(trajectory_points, new GPSPointTimestampComparator());
                    for (GPSPoint point : trajectory_points) {
                        csvWriter.append(point.getTimeStamp() + "," + point.getLon() + "," + point.getLat() + "\n");
                    }

                    csvWriter.flush();
                    csvWriter.close();

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
