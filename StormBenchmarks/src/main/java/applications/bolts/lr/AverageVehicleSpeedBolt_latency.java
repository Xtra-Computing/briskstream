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

package applications.general.bolts.lr;

import applications.general.bolts.AbstractBolt;
import applications.constants.BaseConstants;
import applications.datatypes.PositionReport;
import applications.datatypes.internal.AvgVehicleSpeedTuple;
import applications.datatypes.util.AvgValue;
import applications.datatypes.util.SegmentIdentifier;
import applications.datatypes.util.TopologyControl;
import applications.util.Time;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static applications.constants.BaseConstants.BaseField.MSG_ID;


/**
 * {@link AverageVehicleSpeedBolt_latency} computes the average speed of a vehicle within an express way-segment (partition
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
public class AverageVehicleSpeedBolt_latency extends AbstractBolt {
	private final static long serialVersionUID = 5537727428628598519L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AverageVehicleSpeedBolt_latency.class);
	/**
	 * Internally (re)used object to access individual attributes.
	 */
	private final PositionReport inputPositionReport = new PositionReport();
	/**
	 * Internally (re)used object.
	 */
	private final SegmentIdentifier segment = new SegmentIdentifier();
	/**
	 * Maps each vehicle to its average speed value that corresponds to the current 'minute number' and specified
	 * segment.
	 */
	private final Map<Integer, Pair<AvgValue, SegmentIdentifier>> avgSpeedsMap = new HashMap<>();

	/**
	 * The currently processed 'minute number'.
	 */
	private short currentMinute = 1;


	@Override
	public void execute(Tuple input) {

		Long msgId;
		Long SYSStamp;
		msgId = input.getLongByField(MSG_ID);
		SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);



		this.inputPositionReport.clear();
		this.inputPositionReport.addAll(input.getValues());
		LOGGER.trace(this.inputPositionReport.toString());

		Integer vid = this.inputPositionReport.getVid();
		short minute = this.inputPositionReport.getMinuteNumber();
		int speed = this.inputPositionReport.getSpeed();
		this.segment.set(this.inputPositionReport);

//		assert (minute >= this.currentMinute);

//		if (minute > this.currentMinute) {

		for (Entry<Integer, Pair<AvgValue, SegmentIdentifier>> entry : this.avgSpeedsMap.entrySet()) {
			Pair<AvgValue, SegmentIdentifier> value = entry.getValue();
			SegmentIdentifier segId = value.getRight();

			this.collector.emit(
					TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
					new AvgVehicleSpeedTuple(entry.getKey(),
							this.currentMinute, segId
							.getXWay(), segId.getSegment(), segId.getDirection(), value.getLeft().getAverage()
							, msgId, SYSStamp

					));
		}

		this.avgSpeedsMap.clear();
		this.currentMinute = minute;
//		}

		Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);
		if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {
			SegmentIdentifier segId = vehicleEntry.getRight();

			this.collector.emit(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
					new AvgVehicleSpeedTuple
							(
									vid,
									this.currentMinute, segId.getXWay(), segId
									.getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage(), msgId, SYSStamp));

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
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, AvgVehicleSpeedTuple.getLatencySchema());
	}

}
