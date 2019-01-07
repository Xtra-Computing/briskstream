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

package applications.bolts.lr;


import applications.bolts.lr.datatype.PositionReport;
import applications.bolts.lr.datatype.internal.AvgVehicleSpeedTuple;
import applications.bolts.lr.datatype.util.AvgValue;
import applications.bolts.lr.datatype.util.LRTopologyControl;
import applications.bolts.lr.datatype.util.SegmentIdentifier;
import applications.util.Time;
import brisk.components.operators.base.filterBolt;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * {@link AverageVehicleSpeedBolt} computes the average speed of a vehicle within an express way-segment (partition
 * direction) every minute. The input is expected to be of type {@link PositionReport}, to be ordered by timestamp, and
 * must be grouped by vehicle. A new average speed computation is trigger each time a vehicle changes the express way,
 * segment or direction as well as each 60 seconds (ie, changing 'minute number' [see {@link Time#getMinute(long)}]).<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link AvgVehicleSpeedTuple}
 *
 * @author msoyka
 * @author mjsax
 */
public class AverageVehicleSpeedBolt extends filterBolt {
    private final static long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AverageVehicleSpeedBolt.class);
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each vehicle to its average speed value_list that corresponds to the current 'minute number' and specified
     * segment.
     */
    private final Map<Integer, Pair<AvgValue, SegmentIdentifier>> avgSpeedsMap = new HashMap<>();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private PositionReport inputPositionReport = new PositionReport();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;

    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public AverageVehicleSpeedBolt() {
        super(LOGGER, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, 1.0);
        this.setStateful();
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {

        this.inputPositionReport = (PositionReport) in.getValue(0);
        LOGGER.trace(this.inputPositionReport.toString());

        Integer vid = this.inputPositionReport.getVid();
        short minute = this.inputPositionReport.getMinuteNumber();
        int speed = this.inputPositionReport.getSpeed();
        this.segment.set(this.inputPositionReport);

        for (Entry<Integer, Pair<AvgValue, SegmentIdentifier>> entry : this.avgSpeedsMap.entrySet()) {
            Pair<AvgValue, SegmentIdentifier> value = entry.getValue();
            SegmentIdentifier segId = value.getRight();

            this.collector.force_emit(
                    LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
                    -1, new AvgVehicleSpeedTuple(entry.getKey(),
                            this.currentMinute, segId
                            .getXWay(), segId.getSegment(), segId.getDirection(), value.getLeft().getAverage()));
        }

        this.avgSpeedsMap.clear();
        this.currentMinute = minute;


        Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);
        if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {
            SegmentIdentifier segId = vehicleEntry.getRight();
            cnt2++;
            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
            this.collector.force_emit(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
                    -1, new AvgVehicleSpeedTuple
                            (
                                    vid,
                                    this.currentMinute, segId.getXWay(), segId
                                    .getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage()));

            vehicleEntry = null;
        }

        if (vehicleEntry == null) {
            vehicleEntry = new MutablePair<>(new AvgValue(speed), this.segment.copy());
            this.avgSpeedsMap.put(vid, vehicleEntry);
        } else {
            vehicleEntry.getLeft().updateAverage(speed);
        }

    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {


        int bound = in.length;
        final long bid = in.getBID();
        cnt += bound;
        for (int i = 0; i < bound; i++) {

//			this.inputPositionReport.clear();
//			Collections.addAll(this.inputPositionReport, in.getMsg(i));
            this.inputPositionReport = (PositionReport) in.getMsg(i).getValue(0);
            LOGGER.trace(this.inputPositionReport.toString());

            Integer vid = this.inputPositionReport.getVid();
            short minute = this.inputPositionReport.getMinuteNumber();
            int speed = this.inputPositionReport.getSpeed();
            this.segment.set(this.inputPositionReport);

//			assert (minute >= this.currentMinute);
//			if (minute < this.currentMinute) {
//				//restart..
//				currentMinute = minute;
//			}

//			if (minute > this.currentMinute) {
            // emit all values for last minute
            // (because input tuples are ordered by ts, we can relax_reset the last minute safely)
            for (Entry<Integer, Pair<AvgValue, SegmentIdentifier>> entry : this.avgSpeedsMap.entrySet()) {
                Pair<AvgValue, SegmentIdentifier> value = entry.getValue();
                SegmentIdentifier segId = value.getRight();

                cnt1++;
                // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
                this.collector.emit(
                        LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
                        bid, new AvgVehicleSpeedTuple(entry.getKey(),
                                this.currentMinute, segId
                                .getXWay(), segId.getSegment(), segId.getDirection(), value.getLeft().getAverage()));
            }

            this.avgSpeedsMap.clear();
            this.currentMinute = minute;
//			}

            Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);
            if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {
                SegmentIdentifier segId = vehicleEntry.getRight();
                cnt2++;
                // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
                this.collector.emit(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
                        bid, new AvgVehicleSpeedTuple
                                (
                                        vid,
                                        this.currentMinute, segId.getXWay(), segId
                                        .getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage()));

                // set to null to get new vehicle entry below
                vehicleEntry = null;
            }

            if (vehicleEntry == null) {
                vehicleEntry = new MutablePair<>(new AvgValue(speed), this.segment.copy());
                this.avgSpeedsMap.put(vid, vehicleEntry);
            } else {
                vehicleEntry.getLeft().updateAverage(speed);
            }

        }

    }


    public void display() {
        LOGGER.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\tcnt2:" + cnt2 + "\toutput selectivity:" + ((cnt1 + cnt2) / cnt));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, AvgVehicleSpeedTuple.getSchema());
    }

}
