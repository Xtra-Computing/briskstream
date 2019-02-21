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
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

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
//		this.output_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, 0.0057618584512478315);
//		this.output_selectivity.put(LRTopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, 0.0011689623684645518);

    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        String raw = null;

        raw = in.getString(0);

        String[] token = raw.split(" ");

        short type = Short.parseShort(token[0]);
        Integer time = Integer.parseInt(token[1]);
        Integer vid = Integer.parseInt(token[2]);
        assert (time.shortValue() == Short.parseShort(token[1]));

        if (type == AbstractLRBTuple.position_report) {
            this.collector.force_emit(POSITION_REPORTS_STREAM_ID,
                    -1, new PositionReport(//
                            time,//
                            vid,//
                            Integer.parseInt(token[3]), // speed
                            Integer.parseInt(token[4]), // xway
                            Short.parseShort(token[5]), // lane
                            Short.parseShort(token[6]), // direction
                            Short.parseShort(token[7]), // segment
                            Integer.parseInt(token[8]))); // position
        } else {
            Integer qid = Integer.parseInt(token[9]);
            switch (type) {
                case AbstractLRBTuple.account_balance_request:

                    break;
                case AbstractLRBTuple.daily_expenditure_request:

                    break;
                case AbstractLRBTuple.travel_time_request://not in use in this experiment.

                    break;
                default:
                    LOG.error("Unkown tuple type: {}", type);

            }
        }

    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
//		long pre_pr = pr;
//		cnt += bound;
        for (int i = 0; i < bound; i++) {
            String raw = null;
//			try {
            raw = in.getString(0, i);
            //raw = raw.substring(3, raw.length() - 2);
            String[] token = raw.split(" ");
            // common attributes of all in tuples
            short type = Short.parseShort(token[0]);
            Integer time = Integer.parseInt(token[1]);
            Integer vid = Integer.parseInt(token[2]);
            assert (time.shortValue() == Short.parseShort(token[1]));

            if (type == AbstractLRBTuple.position_report) {

//					long _bid = BIDGenerator2.getHolder().getAndIncrement();
                this.collector.emit(POSITION_REPORTS_STREAM_ID,
                        bid,
//							gap,
                        new PositionReport(//
                                time,//
                                vid,//
                                Integer.parseInt(token[3]), // speed
                                Integer.parseInt(token[4]), // xway
                                Short.parseShort(token[5]), // lane
                                Short.parseShort(token[6]), // direction
                                Short.parseShort(token[7]), // segment
                                Integer.parseInt(token[8]))); // position

//				pr++;
            } else {
                // common attribute of all requests
                Integer qid = Integer.parseInt(token[9]);
                switch (type) {
                    case AbstractLRBTuple.account_balance_request:


//						this.collector.emit(LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
//								bid, new AccountBalanceRequest(time, vid, qid));
//						ab++;
                        break;
                    case AbstractLRBTuple.daily_expenditure_request:

//						this.collector.emit(LRTopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,
//								bid, new DailyExpenditureRequest(time, vid,//
//										Integer.parseInt(token[4]), // xway
//										qid,//
//										Short.parseShort(token[14]))); // day
//						de++;
                        break;
                    case AbstractLRBTuple.travel_time_request://not in use in this experiment.
//							this.collector.emit(LRTopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, bid,
//									new TravelTimeRequest(time, vid,//
//											new Integer(Integer.parseInt(token[4])), // xway
//											qid,//
//											new Short(Short.parseShort(token[10])), // S_init
//											new Short(Short.parseShort(token[11])), // S_end
//											new Short(Short.parseShort(token[12])), // DOW
//											new Short(Short.parseShort(token[13])))); // TOD
                        break;
                    default:
                        LOG.error("Unkown tuple type: {}", type);

                }
            }
//            double i=cnt1/cnt;
//			} catch (Exception e) {
//				LOG.error("Error in line: {}", raw);
//				e.printStackTrace();
//			}
        }

//		//LOG.DEBUG("Dispatcher (" + this.getContext().getThisTaskId() + ") emit:" + bid + " @ "+ DateTime.now());
//		if( this.collector.getBID(POSITION_REPORTS_STREAM_ID) != (bid + 1)){
//			System.out.println(" ");
//		}
//		clean_gap(gap);
//
//		if ((pr-pre_pr) != bound) {
//			this.collector.emit_inorder_push(POSITION_REPORTS_STREAM_ID, bid, gap); // position
//
//		}
    }


    public void display() {
//		LOG.info("cnt:" + cnt
//				+ "\tde:" + de + "\t DE output selectivity:" + ((de) / (double) cnt)
//						+ "\tpr:" + pr + "\t PR output selectivity:" + ((pr) / (double) cnt)
//				+ "\tab:" + ab + "\t AB output selectivity:" + ((ab) / (double) cnt)
//		);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(POSITION_REPORTS_STREAM_ID, PositionReport.getSchema());
//		outputFieldsDeclarer.declareStream(LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
//				AccountBalanceRequest.getSchema());
//		outputFieldsDeclarer.declareStream(LRTopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,
//				DailyExpenditureRequest.getSchema());
//        outputFieldsDeclarer
//                .declareStream(LRTopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, TravelTimeRequest.getSchema());
    }

//	public LinkedList<Long> gap;

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
//		gap = new LinkedList<>();
    }

}
