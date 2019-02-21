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
import applications.data.PositionReport;
import applications.data.internal.CountTuple;
import applications.data.util.CarCount;
import applications.data.util.SegmentIdentifier;
import applications.data.util.TopologyControl;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static applications.constants.BaseConstants.BaseField.MSG_ID;


/**
 * {@link CountVehiclesBolt_latency} counts the number of vehicles within an express way segment (partition direction) every
 * minute. The input is expected to be of type {@link PositionReport}, to be ordered by timestamp, and must be grouped
 * by {@link SegmentIdentifier}. A new count value is emitted each 60 seconds (ie, changing 'minute number' [see
 * Time.getMinute(short)]).<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link CountTuple} (stream: {@link TopologyControl#CAR_COUNTS_STREAM_ID})
 *
 * @author mjsax
 */
public class CountVehiclesBolt_latency extends AbstractBolt {
	private static final long serialVersionUID = 6158421247331445466L;
	private static final Logger LOGGER = LoggerFactory.getLogger(CountVehiclesBolt_latency.class);
	/**
	 * Internally (re)used object to access individual attributes.
	 */
	private final PositionReport inputPositionReport = new PositionReport();
	/**
	 * Internally (re)used object.
	 */
	private final SegmentIdentifier segment = new SegmentIdentifier();
	/**
	 * Maps each segment to its count value.
	 */
	private final Map<SegmentIdentifier, CarCount> countsMap = new HashMap<SegmentIdentifier, CarCount>();

	/**
	 * The currently processed 'minute number'.
	 */
	private short currentMinute = -1;

	@Override
	public void execute(Tuple input) {
//        cnt++;

		Long msgId;
		Long SYSStamp;

		msgId = input.getLongByField(MSG_ID);
		SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);

		this.inputPositionReport.clear();
		this.inputPositionReport.addAll(input.getValues());
		LOGGER.trace(this.inputPositionReport.toString());

		short minute = this.inputPositionReport.getMinuteNumber();
		this.segment.set(this.inputPositionReport);

//		assert (minute >= this.currentMinute);
		if (minute < this.currentMinute) {
			//restart..
			currentMinute = minute;
		}

		if (minute > this.currentMinute) {
			boolean emitted = false;

			for (Entry<SegmentIdentifier, CarCount> entry : this.countsMap.entrySet()) {
				SegmentIdentifier segId = entry.getKey();


				int count = entry.getValue().count;
				if (count > 50) {

					emitted = true;
					this.collector.emit(TopologyControl.CAR_COUNTS_STREAM_ID,
							new CountTuple(new Short(
									this.currentMinute), segId.getXWay(), segId.getSegment(), segId.getDirection(), new Integer(
									count), msgId, SYSStamp));
				}
			}
			if (!emitted) {
//                cnt1++;
				this.collector.emit(TopologyControl.CAR_COUNTS_STREAM_ID, new CountTuple(new Short(minute), msgId, SYSStamp));
			}
			this.countsMap.clear();
			this.currentMinute = minute;
		}

		CarCount segCnt = this.countsMap.get(this.segment);
		if (segCnt == null) {
			segCnt = new CarCount();
			this.countsMap.put(this.segment.copy(), segCnt);
		} else {
			++segCnt.count;
		}

	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.CAR_COUNTS_STREAM_ID, CountTuple.getSchema_latency());
	}


}
