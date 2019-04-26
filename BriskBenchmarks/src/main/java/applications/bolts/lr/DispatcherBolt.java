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

import applications.datatype.*;
import applications.datatype.util.LRTopologyControl;
import brisk.components.operators.base.filterBolt;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static applications.CONTROL.enable_shared_state;
import static applications.datatype.util.LRTopologyControl.POSITION_REPORTS_STREAM_ID;


/**
 * {@link DispatcherBolt} retrieves a stream of {@code <ts,string>} tuples, parses the second CSV attribute and emits an
 * appropriate LRB tuple. The LRB input CSV schema is:
 * {@code Type, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos, QID, S_init, S_end, DOW, TOD, Day}<br />
 * <br />
 * <strong>Output schema:</strong>
 * <ul>
 * <li>{@link PositionReport} (stream: {@link LRTopologyControl#POSITION_REPORTS_STREAM_ID})</li>
 * <li>{@link AccountBalanceRequest} (stream: {@link LRTopologyControl#ACCOUNT_BALANCE_REQUESTS_STREAM_ID})</li>
 * <li>{@link DailyExpenditureRequest} (stream: {@link LRTopologyControl#DAILY_EXPEDITURE_REQUESTS_STREAM_ID})</li>
 * <li>{@link TravelTimeRequest} (stream: {@link LRTopologyControl#TRAVEL_TIME_REQUEST_STREAM_ID})</li>
 * </ul>
 *
 * @author mjsax
 **/
public class DispatcherBolt extends filterBolt {
    private static final long serialVersionUID = 6908631355830501961L;
    private static final Logger LOG = LoggerFactory.getLogger(DispatcherBolt.class);

    long cnt = 0, de = 0, pr = 0, ab = 0;
    //     10215332
//    private double pr = 98.85696197046802%, ab = 0.57618584512478315%, de = 0.11689623684645518%;

    /**
     * outputFieldsDeclarer.declareStream(LRTopologyControl.POSITION_REPORTS_STREAM_ID, PositionReport.getSchema());
     * outputFieldsDeclarer.declareStream(LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, AccountBalanceRequest.getSchema());
     * outputFieldsDeclarer.declareStream(LRTopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, DailyExpenditureRequest.getSchema());
     */


    public DispatcherBolt() {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(POSITION_REPORTS_STREAM_ID, 0.9885696197046802);

    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        long bid = in.getBID();

        if (in.isMarker()) {//only one instance of dispatcher can broadcast

            this.collector.broadcast_marker(bid, in.getMarker());

        } else {

            String raw = null;

            try {
                raw = in.getString(0);
            } catch (Exception e) {
                System.nanoTime();
            }
            String[] token = raw.split(" ");

            short type = Short.parseShort(token[0]);
            Short time = Short.parseShort(token[1]);
            Integer vid = Integer.parseInt(token[2]);
            assert (time.shortValue() == Short.parseShort(token[1]));

            if (type == AbstractLRBTuple.position_report) {

                if (enable_shared_state)
                    this.collector.emit_single(POSITION_REPORTS_STREAM_ID,
                            bid,
                            new PositionReport(//
                                    time,//
                                    vid,//
                                    Integer.parseInt(token[3]), // speed
                                    Integer.parseInt(token[4]), // xway
                                    Short.parseShort(token[5]), // lane
                                    Short.parseShort(token[6]), // direction
                                    Short.parseShort(token[7]), // segment
                                    Integer.parseInt(token[8]))); // position
                else
                    this.collector.emit(POSITION_REPORTS_STREAM_ID,
                            bid, new PositionReport(//
                                    time,//
                                    vid,//
                                    Integer.parseInt(token[3]), // speed
                                    Integer.parseInt(token[4]), // xway
                                    Short.parseShort(token[5]), // lane
                                    Short.parseShort(token[6]), // direction
                                    Short.parseShort(token[7]), // segment
                                    Integer.parseInt(token[8]))); // position

            } else {//not in use in this experiment.
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(POSITION_REPORTS_STREAM_ID, PositionReport.getSchema());
    }

}
