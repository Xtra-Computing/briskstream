/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */

package applications.datatypes.internal;

import applications.datatypes.util.ISegmentIdentifier;
import applications.datatypes.util.TopologyControl;
import applications.util.Time;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * {@link AvgVehicleSpeedTuple} represents an intermediate result tuple; the average speed of an vehicle in a segment
 * within a 'minute number' time frame (see {@link Time#getMinute(short)}).<br />
 * <br />
 * It has the following attributes: VID, MINUTE, XWAY, SEGMENT, DIR, AVGS
 * <ul>
 * <li>VID: the vehicle id</li>
 * <li>MINUTE: the 'minute number' of the speed average</li>
 * <li>XWAY: the expressway the vehicle is on</li>
 * <li>SEGMENT: the segment number the vehicle is in</li>
 * <li>DIR: the vehicle's driving direction</li>
 * <li>AVGS: the average speed of the vehicle</li>
 * </ul>
 *
 * @author mjsax
 */
public final class AvgVehicleSpeedTuple extends Values implements ISegmentIdentifier {
    /**
     * The index of the VID attribute.
     */
    public final static int VID_IDX = 0;

    // attribute indexes
    /**
     * The index of the MINUTE attribute.
     */
    public final static int MINUTE_IDX = 1;
    /**
     * The index of the XWAY attribute.
     */
    public final static int XWAY_IDX = 2;
    /**
     * The index of the SEGMENT attribute.
     */
    public final static int SEG_IDX = 3;
    /**
     * The index of the DIR attribute.
     */
    public final static int DIR_IDX = 4;
    /**
     * The index of the AVGS attribute.
     */
    public final static int AVGS_IDX = 5;
    private static final long serialVersionUID = 9178312919140032032L;


    public AvgVehicleSpeedTuple() {
    }

    /**
     * Instantiates a new {@link AvgVehicleSpeedTuple} for the given attributes.
     *
     * @param vid      the vehicle id
     * @param minute   the 'minute number' of the speed average
     * @param xway     the expressway the vehicle is on
     * @param segment  the segment number the vehicle is in
     * @param diretion the vehicle's driving direction
     * @param avgSpeed the average speed of the vehicle
     */
    public AvgVehicleSpeedTuple(Integer vid, Short minute, Integer xway, Short segment, Short diretion, Integer avgSpeed) {
        assert (vid != null);
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (diretion != null);
        assert (avgSpeed != null);

        super.add(VID_IDX, vid);
        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, diretion);
        super.add(AVGS_IDX, avgSpeed);
    }

    public AvgVehicleSpeedTuple(Integer vid, Short minute, Integer xway, Short segment, Short diretion, Integer avgSpeed, Long msgId, Long sysStamp) {
        assert (vid != null);
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (diretion != null);
        assert (avgSpeed != null);

        super.add(VID_IDX, vid);
        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, diretion);
        super.add(AVGS_IDX, avgSpeed);
        super.add(msgId);//6
        super.add(sysStamp);//7

    }

    /**
     * Returns the schema of a {@link AvgVehicleSpeedTuple}.
     *
     * @return the schema of a {@link AvgVehicleSpeedTuple}
     */
    public static Fields getSchema() {
        return new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.MINUTE_FIELD_NAME,
                TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
                TopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME);
    }

    public static Fields getLatencySchema() {
        return new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.MINUTE_FIELD_NAME,
                TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
                TopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME, MSG_ID, SYSTEMTIMESTAMP);
    }

    /**
     * Returns the vehicle ID of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    public final Integer getVid() {
        return (Integer) super.get(VID_IDX);
    }

    /**
     * Returns the 'minute number' of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final Short getMinute() {
        return (Short) super.get(MINUTE_IDX);
    }

    /**
     * Returns the expressway ID of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the segment of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }

    /**
     * Returns the vehicle's direction of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }

    /**
     * Returns the vehicle's average speed of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the average speed of this tuple
     */
    public final Integer getAvgSpeed() {
        return (Integer) super.get(AVGS_IDX);
    }

}
