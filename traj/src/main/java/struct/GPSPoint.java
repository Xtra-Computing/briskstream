/**
 * Copyright (C) 2015, BMW Car IT GmbH
 * Author: Stefan Holder (stefan.holder@bmw.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package struct;

import java.io.Serializable;
import java.util.Date;

/**
 * Example type for location coordinates.
 */
public class GPSPoint implements Comparable<GPSPoint>, Serializable {

    public int index;

    public final Date time;
    
    public Point position;

    public GPSPoint(Date time, Point position) {
        this(-1, time, position);
    }

    public GPSPoint(int index, Date time, double lon, double lat) {
        this(index, time, new Point(lon, lat));
    }

    public GPSPoint(int index, Date time, Point position) {
        this.index = index;
        this.time = time;
        this.position = position;
    }

    public GPSPoint(Date time, double lon, double lat) {
        this(-1, time, new Point(lon, lat));
    }

    public GPSPoint(int index, Point position) {
        this(index, new Date(System.nanoTime()), position);
    }

    public GPSPoint(Point point) {
        this(-1, new Date(0), point);
    }

    public GPSPoint(Long time, double lon, double lat) {
        this(new Date(time), new Point(lon, lat));
    }

    @Override
    public String toString() {
        return "GpsMeasurement [index=" + index + ", time=" + time + ", position=" + position + "]";
    }

    @Override
    public int compareTo(GPSPoint p) {
        return (int) (this.time.getTime() - p.time.getTime());
    }

    public Long getTimeStamp() {
        return time.getTime();
    }

    public double getLat() {
        return position.lon;
    }

    public double getLon() {
        return position.lat;
    }

    public boolean isInvalid() {
        return this.time == new Date(0) && this.position.isInvalid();

    }

    public int GetTrajectoryHash() {
        String hash = String.valueOf(this.position.lon) + String.valueOf(this.position.lat);
        return hash.hashCode();
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
