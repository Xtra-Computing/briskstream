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

import applications.bolts.AbstractBolt;
import applications.constants.BaseConstants;
import applications.data.internal.AvgSpeedTuple;
import applications.data.internal.AvgVehicleSpeedTuple;
import applications.data.internal.LavTuple;
import applications.data.util.SegmentIdentifier;
import applications.data.util.TopologyControl;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static applications.constants.BaseConstants.BaseField.MSG_ID;

/**
 * {@link LatestAverageVelocityBolt} computes the "latest average velocity" (LAV), ie, the average speed over all
 * vehicle within an express way-segment (partition direction), over the last five minutes (see Time.getMinute(short)]).
 * The input is expected to be of type {@link AvgSpeedTuple}, to be ordered by timestamp, and must be grouped by
 * {@link SegmentIdentifier}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link AvgSpeedTuple}<br />
 * <strong>Output schema:</strong> {@link LavTuple} ( (stream: {@link TopologyControl#LAVS_STREAM_ID})
 *
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class LatestAverageVelocityBolt extends AbstractBolt {
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOGGER = LoggerFactory.getLogger(LatestAverageVelocityBolt.class);

	/**
	 * Holds the (at max) last five average speed value for each segment.
	 */
	private final Map<SegmentIdentifier, List<Integer>> averageSpeedsPerSegment = new HashMap<>();

	/**
	 * Holds the (at max) last five minute numbers for each segment.
	 */
	private final Map<SegmentIdentifier, List<Short>> minuteNumbersPerSegment = new HashMap<>();
	/**
	 * Internally (re)used object to access individual attributes.
	 */
	private final AvgVehicleSpeedTuple inputTuple = new AvgVehicleSpeedTuple();
	/**
	 * Internally (re)used object.
	 */
	private final SegmentIdentifier segmentIdentifier = new SegmentIdentifier();

	/**
	 * The currently processed 'minute number'.
	 */
	private short currentMinute = 1;


	@Override
	public void execute(Tuple input) {
//        cnt++;
//        if (stat != null) stat.start_measure();
		Long msgId;
		Long SYSStamp;
		msgId = input.getLongByField(MSG_ID);
		SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);


		this.inputTuple.clear();
		this.inputTuple.addAll(input.getValues());
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

//        cnt1++;
		Integer lav = this.computeLavValue(latestAvgSpeeds);
		this.collector.emit(
				TopologyControl.LAVS_STREAM_ID,
				new LavTuple((short) (m + 1), this.segmentIdentifier.getXWay(), this.segmentIdentifier
						.getSegment(), this.segmentIdentifier.getDirection(), lav,msgId,SYSStamp));
//        double i=cnt1/cnt;
//        if (stat != null) stat.end_measure();
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

//        LOGGER.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.LAVS_STREAM_ID, LavTuple.getLatencySchema());
	}

}
