package applications.bolts.lr.txn;

import applications.datatype.TollNotification;
import applications.datatype.util.AvgValue;
import applications.datatype.util.SegmentIdentifier;
import applications.param.lr.LREvent;
import brisk.components.operators.api.TransactionalBolt;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.runtime.tuple.impl.msgs.GeneralMsg;
import engine.DatabaseException;
import engine.storage.datatype.DataBox;
import engine.transaction.impl.TxnContext;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static applications.CONTROL.enable_app_combo;
import static applications.Constants.DEFAULT_STREAM_ID;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static engine.profiler.Metrics.MeasureTools.END_POST_TIME_MEASURE;

public abstract class TPBolt extends TransactionalBolt {
    /**
     * Maps each vehicle to its average speed value that corresponds to the current 'minute number' and specified segment.
     */
    private final Map<Integer, Pair<AvgValue, SegmentIdentifier>> avgSpeedsMap = new HashMap<>();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;

    private short time = -1;//not in use.

    public TPBolt(Logger log, int fid) {
        super(log, fid);
    }


    protected void REQUEST_LOCK_AHEAD(LREvent event, TxnContext txnContext) throws DatabaseException {

        transactionManager.lock_ahead(txnContext, "segment_speed", String.valueOf(event.getPOSReport().getSegment()), event.speed_value, READ_WRITE);
        transactionManager.lock_ahead(txnContext, "segment_cnt", String.valueOf(event.getPOSReport().getSegment()), event.count_value, READ_WRITE);

    }

    TollNotification toll_process(Integer vid, Integer count, Double lav, short time) {
        int toll = 0;

        if (lav < 40) {

            if (count > 50) {

                //TODO: check accident. not in use in this experiment.

                { // only true if no accident was found and "break" was not executed
                    final int var = count - 50;
                    toll = 2 * var * var;
                }
            }
        }

        // TODO GetAndUpdate accurate emit time...
        return new TollNotification(
                time, time, vid, lav, toll);
    }

    void REQUEST_POST(LREvent event) throws InterruptedException {

        TollNotification tollNotification = toll_process(event.getPOSReport().getVid(), event.count, event.lav, event.getPOSReport().getTime());

        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, tollNotification, event.getTimestamp())));
        }

    }

    @Override
    protected void POST_PROCESS(long bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            LREvent event = (LREvent) db.eventManager.get((int) i);
            REQUEST_POST(event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected void REQUEST_CORE(LREvent event) {

        if (event.count_value.getRecord().getValue() != null) {// TSTREAM
            event.count = event.count_value.getRecord().getValue().getInt();
            event.lav = event.speed_value.getRecord().getValue().getDouble();
        } else//others
        {
            DataBox dataBox = event.count_value.getRecord().getValues().get(1);
            HashSet cnt_segment = dataBox.getHashSet();
            cnt_segment.add(event.getPOSReport().getVid());//update hashset; updated state also. TODO: be careful of this.
            event.count = cnt_segment.size();
            DataBox dataBox1 = event.speed_value.getRecord().getValues().get(1);
            event.lav = dataBox1.getDouble();
        }


//        int sum = 0;
//        for (int i = 0; i < NUM_ACCESSES; ++i) {
//            SchemaRecordRef ref = event.getRecord_refs()[i];
//            try {
//                DataBox dataBox = ref.getRecord().getValues().GetAndUpdate(1);
//                int read_result = Integer.parseInt(dataBox.getString().trim());
//                sum += read_result;
//            } catch (Exception e) {
//                System.out.println("Null Pointer Exception at: " + event.getBid() + "i:" + i);
//                System.out.println("What causes the exception?" + event.getKeys()[i]);
//            }
//        }
//
//        if (enable_speculative) {
//            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
//            //now we assume it's all correct for testing its upper bond.
//            //so nothing is send out.
//        } else
//            collector.force_emit(event.getBid(), sum, event.getTimestamp());//the tuple is finished finally.
    }


//
//    /**
//     * Merge function of parser.
//     *
//     * @return
//     */
//    String[] parser(Tuple in) {
//        String raw = in.getString(0);
//
//        String[] token = raw.split(" ");
//        return token;
//    }
//
//    /**
//     * Merge function of dispatcher.
//     *
//     * @param token
//     * @return
//     */
//    PositionReport dispatcher(String[] token) {
//        short type = Short.parseShort(token[0]);
//        Short time = Short.parseShort(token[1]);
//        Integer vid = Integer.parseInt(token[2]);
//        assert (time == Short.parseShort(token[1]));
//
//        if (type == AbstractLRBTuple.position_report) {
//            return new PositionReport(//
//                    time,//
//                    vid,//
//                    Integer.parseInt(token[3]), // speed
//                    Integer.parseInt(token[4]), // xway
//                    Short.parseShort(token[5]), // lane
//                    Short.parseShort(token[6]), // direction
//                    Short.parseShort(token[7]), // segment
//                    Integer.parseInt(token[8])); // position
//        }
//        return null;//not in use in this experiment.
//    }

//
//    AvgVehicleSpeedTuple update_avgsv(Integer vid, int speed) throws InterruptedException {
//        Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);
//
//        AvgVehicleSpeedTuple rt = null;
//
//        if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {// vehicle changes segment.
//
//            SegmentIdentifier segId = vehicleEntry.getRight();
//
//            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
//            rt = new AvgVehicleSpeedTuple(vid, this.currentMinute, segId.getXWay(),
//                    segId.getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage(), time);
//
//            // set to null to GetAndUpdate new vehicle entry below
//            vehicleEntry = null;
//        }
//
//        if (vehicleEntry == null) {//no record for this vehicle
//            //write (insert).
//            vehicleEntry = new MutablePair<>(new AvgValue(speed), this.segment.copy());
//            this.avgSpeedsMap.put(vid, vehicleEntry);
//
//            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
//            rt = new AvgVehicleSpeedTuple(vid, this.currentMinute, segment.getXWay(),
//                    segment.getSegment(), segment.getDirection(), vehicleEntry.getLeft().getAverage(), time);
//
//        } else {// vehicle does not change segment but only GetAndUpdate its speed.
//            //write.
//            vehicleEntry.getLeft().updateAverage(speed);
//
//            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
//            rt = new AvgVehicleSpeedTuple(vid, this.currentMinute, segment.getXWay(),
//                    segment.getSegment(), segment.getDirection(), vehicleEntry.getLeft().getAverage(), time);
//        }
//
//        return rt;
//    }
//
//
//
//    @Override
//    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
//
//
//        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
//
//        long bid = in.getBID();
//
//        Long timestamp;//in.getLong(1);
//
//        if (enable_latency_measurement)
//            timestamp = in.getLong(0);
//        else
//            timestamp = 0L;//
//
//        if (in.getValue(0) != null) {
//
//            PositionReport report = (PositionReport) in.getValue(0);
//            int vid = report.getVid();
//            int speed = report.getSpeed().intValue();
//            this.segment.set(report);
//
//            AvgVehicleSpeedTuple vehicleSpeedTuple = update_avgsv(vid, speed);
//
//            LREvent event = new LREvent(report, vehicleSpeedTuple, bid);
//            //txn process.
//            write_handle(event, timestamp);//GetAndUpdate segment statistics. Write and Read Requests.
//        } else {
//            //Simply push forward.
//            write_handle(bid);
//        }
//
//        if (enable_debug)
//            LOG.info("Commit event of bid:" + bid);
//
//    }

//    protected abstract void write_handle(LREvent event, Long timestamp) throws DatabaseException, InterruptedException;
//
//
//    //simply push forward.
//    protected void write_handle(long bid) throws InterruptedException {
//        transactionManager.getOrderLock().blocking_wait(bid);//ensures that locks are added in the event sequence order.
//        //nothing to add..
//        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.
//    }
//
//    protected abstract void REQUEST_CORE(LREvent event) throws InterruptedException;
}

