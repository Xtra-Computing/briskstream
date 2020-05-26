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

package applications.bolts.udf.lr;

import applications.datatype.internal.AvgSpeedTuple;
import applications.datatype.internal.AvgVehicleSpeedTuple;
import applications.datatype.internal.LavTuple;
import applications.datatype.util.LRTopologyControl;
import applications.datatype.util.SegmentIdentifier;
import brisk.components.operators.base.filterBolt;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * {@link LatestAverageVelocityBolt} computes the "latest average velocity" (LAV), ie, the average speed over all
 * vehicle within an express way-segment (partition direction), over the last five minutes (see Time.getMinute(short)]).
 * The input is expected to be of type {@link AvgSpeedTuple}, to be ordered by timestamp, and must be grouped by
 * {@link SegmentIdentifier}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link AvgSpeedTuple}<br />
 * <strong>Output schema:</strong> {@link LavTuple} ( (stream: {@link LRTopologyControl#LAVS_STREAM_ID})
 *
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class LatestAverageVelocityBolt extends filterBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(LatestAverageVelocityBolt.class);

    /**
     * Holds the (at max) last five average speed value_list for each segment.
     */
    private final Map<SegmentIdentifier, List<Integer>> averageSpeedsPerSegment = new HashMap<>();

    /**
     * Holds the (at max) last five minute numbers for each segment.
     */
    private final Map<SegmentIdentifier, List<Short>> minuteNumbersPerSegment = new HashMap<>();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segmentIdentifier = new SegmentIdentifier();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private AvgVehicleSpeedTuple inputTuple = new AvgVehicleSpeedTuple();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;

    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public LatestAverageVelocityBolt() {
        super(LOGGER, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.LAVS_STREAM_ID, 1.0);
        this.setStateful();
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
        this.inputTuple = (AvgVehicleSpeedTuple) in.getValue(0);
        LOGGER.trace(this.inputTuple.toString());

        Short minuteNumber = this.inputTuple.getMinute();
        short m = minuteNumber;

        this.segmentIdentifier.set(this.inputTuple);
        List<Integer> latestAvgSpeeds = this.averageSpeedsPerSegment.get(this.segmentIdentifier);
        List<Short> latestMinuteNumber = this.minuteNumbersPerSegment.get(this.segmentIdentifier);

        if (latestAvgSpeeds == null) {
            latestAvgSpeeds = new LinkedList<>();
            this.averageSpeedsPerSegment.put(this.segmentIdentifier.copy(), latestAvgSpeeds);
            latestMinuteNumber = new LinkedList<>();
            this.minuteNumbersPerSegment.put(this.segmentIdentifier.copy(), latestMinuteNumber);
        }
        latestAvgSpeeds.add(this.inputTuple.getAvgSpeed());
        latestMinuteNumber.add(minuteNumber);

        while (latestAvgSpeeds.size() > 1) {
            if (latestMinuteNumber.get(0) < m - 4) {
                latestAvgSpeeds.remove(0);
                latestMinuteNumber.remove(0);
            } else {
                break;
            }
        }


        Integer lav = this.computeLavValue(latestAvgSpeeds);
        this.collector.force_emit(
                LRTopologyControl.LAVS_STREAM_ID,
                -1, new LavTuple((short) (m + 1), this.segmentIdentifier.getXWay(), this.segmentIdentifier
                        .getSegment(), this.segmentIdentifier.getDirection(), lav));

    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        cnt += bound;
        for (int i = 0; i < bound; i++) {
//			this.inputTuple.clear();
//			Collections.addAll(this.inputTuple, in.getMsg(i));
            this.inputTuple = (AvgVehicleSpeedTuple) in.getMsg(i).getValue(0);
            LOGGER.trace(this.inputTuple.toString());

            Short minuteNumber = this.inputTuple.getMinute();
            short m = minuteNumber;

//			assert (m >= this.currentMinute);

//			if (m > this.currentMinute) {
//				// each time we step from one minute to another, we need to measure_end the previous time range for "unfinished"
//				// open windows; this can happen, if there is not AvgSpeedTuple for a segment in the minute before the
//				// current one; we need to truncate all open windows and compute LAV values for each open segment
//
//				short nextMinute = this.currentMinute;
//				// a segment can have multiple consecutive missing AvgSpeedTuple
//				// (for example, if no more cars drive on a segment)
//				while (nextMinute++ < m) {
//					Iterator<Entry<SegmentIdentifier, List<Short>>> it = this.minuteNumbersPerSegment.entrySet().iterator();
//					while (it.hasNext()) {
//						Entry<SegmentIdentifier, List<Short>> e = it.next();
//						SegmentIdentifier sid = e.getKey();
//						List<Short> latestMinuteNumber = e.getValue();
//
//						if (latestMinuteNumber.get(latestMinuteNumber.size() - 1) < nextMinute - 1) {
//							List<Integer> latestAvgSpeeds = this.averageSpeedsPerSegment.get(sid);
//
//							// truncate window if entry is more than 5 minutes older than nextMinute
//							// (can be at max one entry)
//							if (latestMinuteNumber.get(0) < nextMinute - 5) {
//								latestAvgSpeeds.remove(0);
//								latestMinuteNumber.remove(0);
//							}
//
//							if (latestAvgSpeeds.size() > 0) {
//								cnt1++;
//								Integer lav = this.computeLavValue(latestAvgSpeeds);
//								this.collector.emit(LRTopologyControl.LAVS_STREAM_ID,
//										bid, new LavTuple(nextMinute, sid.getXWay(), sid.getSegment(),
//												sid.getDirection(), lav));
//							} else {
//								// remove empty window completely
//								it.remove();
//								this.averageSpeedsPerSegment.remove(sid);
//							}
//						}
//					}
//				}
//				this.currentMinute = m;
//			}
            this.segmentIdentifier.set(this.inputTuple);
            List<Integer> latestAvgSpeeds = this.averageSpeedsPerSegment.get(this.segmentIdentifier);
            List<Short> latestMinuteNumber = this.minuteNumbersPerSegment.get(this.segmentIdentifier);

            if (latestAvgSpeeds == null) {
                latestAvgSpeeds = new LinkedList<>();
                this.averageSpeedsPerSegment.put(this.segmentIdentifier.copy(), latestAvgSpeeds);
                latestMinuteNumber = new LinkedList<>();
                this.minuteNumbersPerSegment.put(this.segmentIdentifier.copy(), latestMinuteNumber);
            }
            latestAvgSpeeds.add(this.inputTuple.getAvgSpeed());
            latestMinuteNumber.add(minuteNumber);

            // discard all values that are more than 5 minutes older than current minute
            while (latestAvgSpeeds.size() > 1) {
                if (latestMinuteNumber.get(0) < m - 4) {
                    latestAvgSpeeds.remove(0);
                    latestMinuteNumber.remove(0);
                } else {
                    break;
                }
            }
//		assert (latestAvgSpeeds.fieldSize() <= 5);
//		assert (latestMinuteNumber.fieldSize() <= 5);

            cnt1++;
            Integer lav = this.computeLavValue(latestAvgSpeeds);
            this.collector.emit(
                    LRTopologyControl.LAVS_STREAM_ID,
                    bid, new LavTuple((short) (m + 1), this.segmentIdentifier.getXWay(), this.segmentIdentifier
                            .getSegment(), this.segmentIdentifier.getDirection(), lav));
//        double i=cnt1/cnt;
//        if (stat != null) stat.end_measure();
        }
    }


    private Integer computeLavValue(List<Integer> latestAvgSpeeds) {
        int speedSum = 0;
        int valueCount = 0;
        for (Integer speed : latestAvgSpeeds) {
            speedSum += speed;
            ++valueCount;
            if (valueCount > 10) {//workaround to ensure constant workload.
                break;
            }
        }

        return speedSum / valueCount;
    }

    public void display() {

        LOGGER.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.LAVS_STREAM_ID, LavTuple.getSchema());
    }

}
