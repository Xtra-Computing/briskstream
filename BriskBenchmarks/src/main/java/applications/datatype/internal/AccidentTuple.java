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

import applications.datatype.PositionReport;
import applications.datatype.util.ISegmentIdentifier;
import applications.datatype.util.LRTopologyControl;
import applications.util.Time;
import brisk.execution.runtime.tuple.impl.Fields;

/**
 * {@link AccidentTuple} represents an intermediate result tuple; and reports and accident that occurred in a specific
 * segment and minute (ie, 'minute number'; see {@link Time#getMinute(short)}).<br />
 * <br />
 * It has the following attributes: MINUTE, XWAY, SEGMENT, DIR
 * <ul>
 * <li>MINUTE: the 'minute number' of the accident</li>
 * <li>XWAY: the expressway in which the accident happened</li>
 * <li>SEGMENT: in which the accident happened</li>
 * <li>DIR: the direction in which the accident happened</li>
 * </ul>
 *
 * @author mjsax
 */
public final class AccidentTuple extends applications.util.datatypes.StreamValues implements ISegmentIdentifier {
    /**
     * The index of the positionReport attribute.
     */
    private final static int POS_IDX = 0;

    // attribute indexes
    /**
     * The index of the MINUTE attribute.
     */
    private final static int MINUTE_IDX = 1;
    /**
     * The index of the XWAY attribute.
     */
    private final static int XWAY_IDX = 2;
    /**
     * The index of the SEGMENT attribute.
     */
    private final static int SEG_IDX = 3;
    /**
     * The index of the DIR attribute.
     */
    private final static int DIR_IDX = 4;
    private static final long serialVersionUID = -7848916337473569028L;


    public AccidentTuple() {
    }

    /**
     * Instantiates a new <em>dummy</em> {@link AccidentTuple} for the given minute. This dummy tuple does not report an
     * accident but is used as a "time progress tuple" to unblock downstream operators.
     *
     * @param minute the 'minute number' of the new minute that starts
     */

    public AccidentTuple(PositionReport PR, Short minute) {
        super(PR, minute, null, null, null);
//        assert (minute != null);
//        super.add(POS_IDX, PR);
//        super.add(MINUTE_IDX, minute);
//        super.add(XWAY_IDX, null);
//        super.add(SEG_IDX, null);
//        super.add(DIR_IDX, null);
    }

    /**
     * Instantiates a new {@link AccidentTuple} for the given attributes.
     *
     * @param minute    the 'minute number' of the speed average
     * @param xway      the expressway the vehicle is on
     * @param segment   the segment number the vehicle is in
     * @param direction the vehicle's driving direction
     */
    public AccidentTuple(PositionReport PR, Short minute, Integer xway, Short segment, Short direction) {
        super(PR, minute, xway, segment, direction);
//		assert (minute != null);
//		assert (xway != null);
//		assert (segment != null);
//		assert (direction != null);
//		super.add(POS_IDX, PR);
//		super.add(MINUTE_IDX, minute);
//		super.add(XWAY_IDX, xway);
//		super.add(SEG_IDX, segment);
//		super.add(DIR_IDX, direction);
    }

    /**
     * Returns the schema of a {@link AccidentTuple}.
     *
     * @return the schema of a {@link AccidentTuple}
     */
    public static Fields getSchema() {
        return new Fields(
                LRTopologyControl.POS_REPORT_FIELD_NAME,
                LRTopologyControl.MINUTE_FIELD_NAME,
                LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.SEGMENT_FIELD_NAME,
                LRTopologyControl.DIRECTION_FIELD_NAME);
    }


    /**
     * Returns the 'minute number' of this {@link AccidentTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final Short getMinuteNumber() {
        return (Short) super.get(MINUTE_IDX);
    }

    /**
     * Returns the expressway ID of this {@link AccidentTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the segment of this {@link AccidentTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }

    /**
     * Returns the vehicle's direction of this {@link AccidentTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }

    /**
     * Returns {@code true} if this tuple does not report an accident but only carries the next 'minute number'.
     *
     * @return {@code true} if this tuple does not report an accident but only carries the next 'minute number' --
     * {@code false} otherwise
     */
    public final boolean isProgressTuple() {
        return super.get(XWAY_IDX) == null;
    }

}
