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

package applications.datatype.internal;

import applications.datatype.util.ISegmentIdentifier;
import applications.datatype.util.LRTopologyControl;
import applications.util.Time;
import brisk.execution.runtime.tuple.impl.Fields;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;


/**
 * {@link LavTuple} represents an intermediate result tuple; the "latest average velocity" (LAV) of a segment within the
 * last 5 minutes (ie, 'minute number'; see {@link Time#getMinute(short)}).<br />
 * <br />
 * It has the following attributes: MINUTE, XWAY, SEGMENT, DIR, LAV
 * <ul>
 * <li>MINUTE: the 'minute number' of the LAV value_list</li>
 * <li>XWAY: the expressway for the LAV value_list</li>
 * <li>SEGMENT: the segment number for the LAV value_list</li>
 * <li>DIR: the direction for the LAV value_list</li>
 * <li>LAV: the latest average velocity of the segment identified by XWAY, SEGMENT, DIR</li>
 * </ul>
 *
 * @author mjsax
 */
public final class LavTuple extends applications.util.datatypes.StreamValues implements ISegmentIdentifier {
    /**
     * The index of the MINUTE attribute.
     */
    private final static int MINUTE_IDX = 0;

    // attribute indexes
    /**
     * The index of the XWAY attribute.
     */
    private final static int XWAY_IDX = 1;
    /**
     * The index of the SEGMENT attribute.
     */
    private final static int SEG_IDX = 2;
    /**
     * The index of the DIR attribute.
     */
    private final static int DIR_IDX = 3;
    /**
     * The index of the LAV attribute.
     */
    private final static int LAV_IDX = 4;
    private static final long serialVersionUID = 1726682629621494657L;


    public LavTuple() {
    }

    /**
     * Instantiates a new {@link LavTuple} for the given attributes.
     *
     * @param minute    the 'minute number' of the speed average
     * @param xway      the expressway the vehicle is on
     * @param segment   the segment number the vehicle is in
     * @param direction the vehicle's driving direction
     * @param lav       the latest average velocity of a segment
     */
    public LavTuple(Short minute, Integer xway, Short segment, Short direction, Integer lav) {
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (direction != null);
        assert (lav != null);

        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, direction);
        super.add(LAV_IDX, lav);
    }

    public LavTuple(Short minute, Integer xway, Short segment, Short direction, Integer lav, long msgID, long timeStamp) {

        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (direction != null);
        assert (lav != null);

        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, direction);
        super.add(LAV_IDX, lav);


        super.add(msgID);//5
        super.add(timeStamp);//6


    }

    /**
     * Returns the schema of a {@link LavTuple}.
     *
     * @return the schema of a {@link LavTuple}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.MINUTE_FIELD_NAME, LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.SEGMENT_FIELD_NAME, LRTopologyControl.DIRECTION_FIELD_NAME,
                LRTopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME);
    }

    public static Fields getLatencySchema() {
        return new Fields(
                LRTopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME, MSG_ID, SYSTEMTIMESTAMP);
    }

    /**
     * Returns the 'minute number' of this {@link LavTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final Short getMinuteNumber() {
        return (Short) super.get(MINUTE_IDX);
    }

    /**
     * Returns the expressway ID of this {@link LavTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the segment of this {@link LavTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }

    /**
     * Returns the vehicle's direction of this {@link LavTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }

    /**
     * Returns the latest average velocity (LAV) of this {@link LavTuple}.
     *
     * @return the latest average velocity (LAV) of this tuple
     */
    public final Integer getLav() {
        return (Integer) super.get(LAV_IDX);
    }

    public long getMsgID() {
        return (long) super.get(5);
    }

    public long getTimeStamp() {
        return (long) super.get(6);
    }

}
