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

import applications.datatype.PositionReport;
import applications.datatype.internal.CountTuple;
import applications.datatype.util.CarCount;
import applications.datatype.util.LRTopologyControl;
import applications.datatype.util.SegmentIdentifier;
import brisk.components.operators.base.filterBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.JumboTuple;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * {@link CountVehiclesBolt} counts the number of vehicles within an express way segment (partition direction) every
 * minute. The input is expected to be of type {@link PositionReport}, to be ordered by timestamp, and must be grouped
 * by {@link SegmentIdentifier}. A new count value_list is emitted each 60 seconds (ie, changing 'minute number' [see
 * Time.getMinute(short)]).<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link CountTuple} (stream: {@link LRTopologyControl#CAR_COUNTS_STREAM_ID})
 *
 * @author mjsax
 */
public class CountVehiclesBolt extends filterBolt {
    private static final long serialVersionUID = 6158421247331445466L;
    private static final Logger LOG = LoggerFactory.getLogger(CountVehiclesBolt.class);
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each segment to its count value_list.
     */
    private final Map<SegmentIdentifier, CarCount> countsMap = new HashMap<>();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private PositionReport inputPositionReport = new PositionReport();

    public CountVehiclesBolt() {
        super(LOG, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.CAR_COUNTS_STREAM_ID, 1.0);
        this.setStateful();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        this.inputPositionReport = (PositionReport) in.getValue(0);
        this.segment.set(this.inputPositionReport);

        CarCount segCnt = this.countsMap.get(this.segment);
        if (segCnt == null) {
            segCnt = new CarCount();
            this.countsMap.put(this.segment.copy(), segCnt);
        } else {
            ++segCnt.count;//update count.
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
//		merger = new TimestampMerger(this, PositionReport.TIME_IDX);
//		merger.prepare(config, context, this.collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.CAR_COUNTS_STREAM_ID, CountTuple.getSchema());
    }
}
