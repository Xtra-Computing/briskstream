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
 * {@link LatestAverageVelocityBolt_latency} computes the "latest average velocity" (LAV), ie, the average speed over all
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
public class LatestAverageVelocityBolt_latency extends filterBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(LatestAverageVelocityBolt_latency.class);

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

    public LatestAverageVelocityBolt_latency() {
        super(LOGGER, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.LAVS_STREAM_ID, 1.0);
        this.setStateful();
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
//        not in use.
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        cnt += bound;
        for (int i = 0; i < bound; i++) {
//			this.inputTuple.clear();
//			Collections.addAll(this.inputTuple, in.getMsg(i));

//			Long msgId;
//			Long SYSStamp;
//			msgId = in.getLong(1, i);
////			if (msgId != -1) {
//			SYSStamp = in.getLong(2, i);
//			} else {
//				SYSStamp = null;
//			}


            this.inputTuple = (AvgVehicleSpeedTuple) in.getMsg(i).getValue(0);
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

//			cnt1++;
            Integer lav = this.computeLavValue(latestAvgSpeeds);

            long msgID = in.getLong(1, i);
            if (msgID != -1)
                this.collector.emit(
                        LRTopologyControl.LAVS_STREAM_ID,
                        bid, new LavTuple((short) (m + 1), this.segmentIdentifier.getXWay(), this.segmentIdentifier
                                .getSegment(), this.segmentIdentifier.getDirection(), lav), msgID, in.getLong(2, i)
                );
            else
                this.collector.emit(
                        LRTopologyControl.LAVS_STREAM_ID,
                        bid, new LavTuple((short) (m + 1), this.segmentIdentifier.getXWay(), this.segmentIdentifier
                                .getSegment(), this.segmentIdentifier.getDirection(), lav), msgID, 0
                );
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
        declarer.declareStream(LRTopologyControl.LAVS_STREAM_ID, LavTuple.getLatencySchema());
    }

}
