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

import applications.datatype.PositionReport;
import applications.datatype.TollNotification;
import applications.datatype.internal.AccidentTuple;
import applications.datatype.internal.CountTuple;
import applications.datatype.internal.LavTuple;
import applications.datatype.util.Constants;
import applications.datatype.util.ISegmentIdentifier;
import applications.datatype.util.LRTopologyControl;
import applications.datatype.util.SegmentIdentifier;
import brisk.components.operators.base.filterBolt;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static applications.datatype.util.LRTopologyControl.CAR_COUNTS_STREAM_ID;
import static applications.datatype.util.LRTopologyControl.TOLL_ASSESSMENTS_STREAM_ID;

/**
 * {@link TollNotificationBolt} calculates the toll for each vehicle and reports it back to the vehicle if a vehicle
 * enters a segment. Furthermore, the toll is assessed to the vehicle if it leaves a segment.<br />
 * <br />
 * The toll depends on the number of cars in the segment (the minute before) the car is driving on and is only charged
 * if the car is not on the exit line, more than 50 cars passed this segment the minute before, the
 * "latest average velocity" is smaller then 40, and no accident occurred in the minute before in the segment and 4
 * downstream segments.<br />
 * <br />
 * {@link TollNotificationBolt} processes four input streams. The first input is expected to be of type
 * {@link PositionReport} and must be grouped by vehicle id. The other inputs are expected to be of type
 * {@link AccidentTuple}, {@link CountTuple}, and {@link LavTuple} and must be broadcasted. All inputs most be ordered
 * by time (ie, timestamp for {@link PositionReport} and minute number for {@link AccidentTuple}, {@link CountTuple},
 * and {@link LavTuple}). It is further assumed, that all {@link AccidentTuple}s and {@link CountTuple}s with a
 * <em>smaller</em> minute number than a {@link PositionReport} tuple as well as all {@link LavTuple}s with the
 * <em>same</em> minute number than a {@link PositionReport} tuple are delivered <em>before</em> those
 * {@link PositionReport}s.<br />
 * <br />
 * This implementation assumes, that {@link PositionReport}s, {@link AccidentTuple}s, {@link CountTuple}s, and
 * {@link LavTuple}s are delivered via streams called {@link LRTopologyControl#POSITION_REPORTS_STREAM_ID},
 * {@link LRTopologyControl#ACCIDENTS_STREAM_ID},LRTopologyControl#ACCIDENTS_STREAM_ID2}, {@link LRTopologyControl#CAR_COUNTS_STREAM_ID}, and
 * {@link LRTopologyControl#LAVS_STREAM_ID}, respectively.<br />
 * <br />
 * <strong>Expected input:</strong> {@link PositionReport}, {@link AccidentTuple}, {@link CountTuple}, {@link LavTuple}<br />
 * <strong>Output schema:</strong>
 * <ul>
 * <li>{@link TollNotification} (stream: {@link LRTopologyControl#TOLL_NOTIFICATIONS_STREAM_ID})</li>
 * <li>{@link TollNotification} (stream: {@link LRTopologyControl#TOLL_ASSESSMENTS_STREAM_ID})</li>
 * </ul>
 *
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class TollNotificationBolt extends filterBolt {
    private final static long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = LoggerFactory.getLogger(TollNotificationBolt.class);
    /**
     * Contains all vehicle IDs and segment of the last {@link PositionReport} to allow skipping already sent
     * notifications (there's only one notification per segment per vehicle).
     */
    private final Map<Integer, Short> allCars = new HashMap<>();
    /**
     * Contains the last toll notification for each vehicle to assess the toll when the vehicle leaves a segment.
     */
    private final Map<Integer, TollNotification> lastTollNotification = new HashMap<>();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segmentToCheck = new SegmentIdentifier();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private PositionReport inputPositionReport = new PositionReport();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private AccidentTuple inputAccidentTuple = new AccidentTuple();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private CountTuple inputCountTuple = new CountTuple();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private LavTuple inputLavTuple = new LavTuple();
    /**
     * Buffer for accidents.
     */
    private Set<ISegmentIdentifier> currentMinuteAccidents = new HashSet<>();
    /**
     * Buffer for accidents.
     */
    private Set<ISegmentIdentifier> previousMinuteAccidents = new HashSet<>();
    /**
     * Buffer for car counts.
     */
    private Map<ISegmentIdentifier, Integer> currentMinuteCounts = new HashMap<>();
    /**
     * Buffer for car counts.
     */
    private Map<ISegmentIdentifier, Integer> previousMinuteCounts = new HashMap<>();
    /**
     * Buffer for LAV values.
     */
    private Map<ISegmentIdentifier, Integer> currentMinuteLavs = new HashMap<>();
    /**
     * Buffer for LAV values.
     */
    private Map<ISegmentIdentifier, Integer> previousMinuteLavs = new HashMap<>();
    /**
     * The currently processed 'minute number'.
     */
    private int currentMinute = -1;

    private double cnt = 0, cnt1 = 0, cnt2 = 0, cnt3 = 0;


    /**
     * declarer.declareStream(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, TollNotification.getSchema());
     * declarer.declareStream(TOLL_ASSESSMENTS_STREAM_ID, TollNotification.getSchema());
     * //output selectivity:
     * //tn: 0.7307448477727714   -> 0.8990713076693898
     * //ta: 0.24149251117472822 -> 0.2375600149674265
     */
    public TollNotificationBolt() {
        super(LOG, new HashMap<>(), new HashMap<>());//let input selectivity to control.
        this.input_selectivity.put(CAR_COUNTS_STREAM_ID, 1.0);//produce no output..
        this.input_selectivity.put(LRTopologyControl.LAVS_STREAM_ID, 1.0); //
        this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID2, 1.0);
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);//-->creates tn and ta.
        this.output_selectivity.put(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, 1.0); //cnt1
//		this.output_selectivity.put(TOLL_ASSESSMENTS_STREAM_ID, 1.0); // cnt2
        this.setStateful();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//       not in use
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        cnt += bound;
        for (int index = 0; index < bound; index++) {
            final String inputStreamId = in.getSourceStreamId(index);
            cnt1++;
            this.collector.emit(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, bid);//as an indication.

            switch (inputStreamId) {
                case LRTopologyControl.POSITION_REPORTS_STREAM_ID:
//					this.inputPositionReport.clear();
//					Collections.addAll(this.inputPositionReport, in.getMsg(index).getValue(0));
                    this.inputPositionReport = (PositionReport) in.getMsg(index).getValue(0);
//					//LOG.DEBUG("this.inputPositionReport:" + this.inputPositionReport.toString());

                    this.checkMinute(this.inputPositionReport.getMinuteNumber());

                    if (this.inputPositionReport.isOnExitLane()) {
                        final TollNotification lastNotification = this.lastTollNotification.remove(this.inputPositionReport
                                .getVid());

                        if (lastNotification != null) {
                            assert (lastNotification.getPos().getXWay() != null);
                            cnt2++;
//							this.collector.emit(TOLL_ASSESSMENTS_STREAM_ID, bid, lastNotification);
                        }
                        continue;
                    }

                    final Short currentSegment = this.inputPositionReport.getSegment();
                    final Integer vid = this.inputPositionReport.getVid();
                    final Short previousSegment = this.allCars.put(vid, currentSegment);
                    if (previousSegment != null && currentSegment.shortValue() == previousSegment.shortValue()) {
                        continue;
                    }

                    int toll = 0;
                    Integer lav = this.previousMinuteLavs.get(new SegmentIdentifier(this.inputPositionReport));

                    final int lavValue;
                    if (lav != null) {
                        lavValue = lav;
                    } else {
                        lav = 0;
                        lavValue = 0;
                    }

                    if (lavValue < 50) {
                        final Integer count = this.previousMinuteCounts.get(new SegmentIdentifier(this.inputPositionReport));
                        int carCount = 0;
                        if (count != null) {
                            carCount = count;
                        }

                        if (carCount > 50) {
                            // downstream is either larger or smaller of current segment
                            final Short direction = this.inputPositionReport.getDirection();
                            final short dir = direction;
                            // EASTBOUND == 0 => diff := 1
                            // WESTBOUNT == 1 => diff := -1
                            final short diff = (short) -(dir - 1 + ((dir + 1) / 2));
//							assert (dir == applications.Constants.EASTBOUND ? diff == 1 : diff == -1);

                            if (diff != 1 && diff != -1) {
                                continue;//position report is errored.
                            }

                            final Integer xway = this.inputPositionReport.getXWay();
                            final short curSeg = currentSegment;

                            this.segmentToCheck.setXWay(xway);
                            this.segmentToCheck.setDirection(direction);

                            int i;
                            for (i = 0; i <= 4; ++i) {
                                final short nextSegment = (short) (curSeg + (diff * i));
                                assert (dir == Constants.EASTBOUND ? nextSegment >= curSeg : nextSegment <= curSeg);

                                this.segmentToCheck.setSegment(nextSegment);

                                if (this.previousMinuteAccidents.contains(this.segmentToCheck)) {
                                    break;
                                }
                            }

                            if (i == 5) { // only true if no accident was found and "break" was not executed
                                final int var = carCount - 50;
                                toll = 2 * var * var;
                            }
                        }
                    }
                    // TODO get accurate emit time...
                    final TollNotification tollNotification = new TollNotification
                            (this.inputPositionReport.getTime(),
                                    this.inputPositionReport.getTime(),
                                    vid,
                                    lav,
                                    toll
                                    ,
                                    this.inputPositionReport.copy()
                            );

                    final TollNotification lastNotification;
                    if (toll != 0) {
                        lastNotification = this.lastTollNotification.put(vid, tollNotification);
                    } else {
                        lastNotification = this.lastTollNotification.remove(vid);
                    }
                    assert lastNotification == null || (lastNotification.getPos().getXWay() != null);
                    assert (tollNotification.getPos().getXWay() != null);
//					cnt1++;
//					this.collector.emit(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, bid, tollNotification);
                    break;

                case LRTopologyControl.ACCIDENTS_STREAM_ID:
//				case LRTopologyControl.ACCIDENTS_STREAM_ID2:
//					cnt3++;
//					this.inputAccidentTuple.clear();
//					Collections.addAll(this.inputAccidentTuple, in.getMsg(index).getValue(0));
//			LOG.trace(this.inputAccidentTuple.show());
                    inputAccidentTuple = (AccidentTuple) in.getMsg(index).getValue(0);
                    this.checkMinute(this.inputAccidentTuple.getMinuteNumber());
//            assert (this.inputAccidentTuple.getMinuteNumber().shortValue() == this.currentMinute);

                    if (this.inputAccidentTuple.isProgressTuple()) {
                        continue;
                    }

                    this.currentMinuteAccidents.add(new SegmentIdentifier(this.inputAccidentTuple));

                    break;
                case CAR_COUNTS_STREAM_ID:
//					this.inputCountTuple.clear();
//					Collections.addAll(this.inputCountTuple, in.getMsg(index).getValue(0));
                    this.inputCountTuple = (CountTuple) in.getMsg(index).getValue(0);
//            LOG.trace(this.inputCountTuple.show());

                    this.checkMinute(this.inputCountTuple.getMinuteNumber());
//            assert (this.inputCountTuple.getMinuteNumber().shortValue() - 1 == this.currentMinute);

                    if (this.inputCountTuple.isProgressTuple()) {
                        continue;
                    }

                    this.currentMinuteCounts.put(new SegmentIdentifier(this.inputCountTuple), this.inputCountTuple.getCount());

                    break;
                case LRTopologyControl.LAVS_STREAM_ID:
//					cnt3++;
//					this.inputLavTuple.clear();
//					Collections.addAll(this.inputLavTuple, in.getMsg(index).getValue(0));
                    inputLavTuple = (LavTuple) in.getMsg(index).getValue(0);
                    LOG.trace("this.inputLavTuple" + this.inputLavTuple.toString());

                    this.checkMinute(this.inputLavTuple.getMinuteNumber());
//            assert (this.inputLavTuple.getMinuteNumber().shortValue() - 1 == this.currentMinute);

                    this.currentMinuteLavs.put(new SegmentIdentifier(this.inputLavTuple), this.inputLavTuple.getLav());
                    break;
                default:
                    LOG.error("Unknown input stream: '" + inputStreamId + "' for tuple " + in);
                    throw new RuntimeException("Unknown input stream: '" + inputStreamId + "' for tuple " + in);
            }
        }
    }


    public void display() {
        LOG.info("cnt:" + cnt + "\tcnt1:" + LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID
                + ":" + cnt1 + "(" + (cnt1 / cnt) + ")"
                + "\tcnt2:" + TOLL_ASSESSMENTS_STREAM_ID + ":" + cnt2 + "(" + (cnt2 / cnt) + ")"
                + "\tcnt3:" + cnt3 + "(" + (cnt3 / cnt) + ")"
        );
    }

    private void checkMinute(short minute) {
        //due to the tuple may be send in reverse-order, it may happen that some tuples are processed too late.
//        assert (minute >= this.currentMinute);
        if (minute < this.currentMinute) {
            //restart..
            currentMinute = minute;
        }

        if (minute > this.currentMinute) {
            LOG.trace("New minute: {}", minute);
            this.currentMinute = minute;
            this.previousMinuteAccidents = this.currentMinuteAccidents;
            this.currentMinuteAccidents = new HashSet<>();
            this.previousMinuteCounts = this.currentMinuteCounts;
            this.currentMinuteCounts = new HashMap<>();
            this.previousMinuteLavs = this.currentMinuteLavs;
            this.currentMinuteLavs = new HashMap<>();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, TollNotification.getSchema());
//		declarer.declareStream(TOLL_ASSESSMENTS_STREAM_ID, TollNotification.getSchema());

    }

}
