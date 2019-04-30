package applications.bolts.lr.txn;

import applications.datatype.AbstractLRBTuple;
import applications.datatype.PositionReport;
import applications.datatype.TollNotification;
import applications.datatype.internal.AvgVehicleSpeedTuple;
import applications.datatype.util.AvgValue;
import applications.datatype.util.SegmentIdentifier;
import applications.param.lr.LREvent;
import brisk.components.operators.api.TransactionalBolt;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.enable_debug;
import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.BEGIN_PREPARE_TIME_MEASURE;

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

    void toll_process(long bid, Integer vid, Integer count, Double lav, short time) throws InterruptedException {
        int toll = 0;

        if (lav < 40) {

            int carCount = 0;
            if (count != null) {
                carCount = count.intValue();
            }

            if (carCount > 50) {

                //TODO: check accident. not in use in this experiment.

                { // only true if no accident was found and "break" was not executed
                    final int var = carCount - 50;
                    toll = 2 * var * var;
                }
            }
        }

        // TODO GetAndUpdate accurate emit time...
        final TollNotification tollNotification
                = new TollNotification(
                time, time, vid, lav, toll);

        this.collector.emit(bid, tollNotification);
    }




    /**
     * Merge function of parser.
     *
     * @return
     */
    String[] parser(Tuple in) {
        String raw = in.getString(0);

        String[] token = raw.split(" ");
        return token;
    }

    /**
     * Merge function of dispatcher.
     *
     * @param token
     * @return
     */
    PositionReport dispatcher(String[] token) {
        short type = Short.parseShort(token[0]);
        Short time = Short.parseShort(token[1]);
        Integer vid = Integer.parseInt(token[2]);
        assert (time == Short.parseShort(token[1]));

        if (type == AbstractLRBTuple.position_report) {
            return new PositionReport(//
                    time,//
                    vid,//
                    Integer.parseInt(token[3]), // speed
                    Integer.parseInt(token[4]), // xway
                    Short.parseShort(token[5]), // lane
                    Short.parseShort(token[6]), // direction
                    Short.parseShort(token[7]), // segment
                    Integer.parseInt(token[8])); // position
        }
        return null;//not in use in this experiment.
    }

    final SegmentIdentifier segment = new SegmentIdentifier();


    AvgVehicleSpeedTuple update_avgsv(Integer vid, int speed) throws InterruptedException {
        Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);

        AvgVehicleSpeedTuple rt = null;

        if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {// vehicle changes segment.

            SegmentIdentifier segId = vehicleEntry.getRight();

            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
            rt = new AvgVehicleSpeedTuple(vid, this.currentMinute, segId.getXWay(),
                    segId.getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage(), time);

            // set to null to GetAndUpdate new vehicle entry below
            vehicleEntry = null;
        }

        if (vehicleEntry == null) {//no record for this vehicle
            //write (insert).
            vehicleEntry = new MutablePair<>(new AvgValue(speed), this.segment.copy());
            this.avgSpeedsMap.put(vid, vehicleEntry);

            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
            rt = new AvgVehicleSpeedTuple(vid, this.currentMinute, segment.getXWay(),
                    segment.getSegment(), segment.getDirection(), vehicleEntry.getLeft().getAverage(), time);

        } else {// vehicle does not change segment but only GetAndUpdate its speed.
            //write.
            vehicleEntry.getLeft().updateAverage(speed);

            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
            rt = new AvgVehicleSpeedTuple(vid, this.currentMinute, segment.getXWay(),
                    segment.getSegment(), segment.getDirection(), vehicleEntry.getLeft().getAverage(), time);
        }


        return rt;
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {


        BEGIN_PREPARE_TIME_MEASURE(thread_Id);

        long bid = in.getBID();

        Long timestamp;//in.getLong(1);

        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        if (in.getValue(0) != null) {

            PositionReport report = (PositionReport) in.getValue(0);
            int vid = report.getVid();
            int speed = report.getSpeed().intValue();
            this.segment.set(report);

            AvgVehicleSpeedTuple vehicleSpeedTuple = update_avgsv(vid, speed);

            LREvent event = new LREvent(report, vehicleSpeedTuple, bid);
            //txn process.
            write_handle(event, timestamp);//GetAndUpdate segment statistics. Write and Read Requests.
        } else {
            //Simply push forward.
            write_handle(bid);
        }

        if (enable_debug)
            LOG.info("Commit event of bid:" + bid);

    }

    protected abstract void write_handle(LREvent event, Long timestamp) throws DatabaseException, InterruptedException;


    //simply push forward.
    protected void write_handle(long bid) throws InterruptedException {
        transactionManager.getOrderLock().blocking_wait(bid);//ensures that locks are added in the event sequence order.
        //nothing to add..
        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.
    }

    protected abstract void read_core(LREvent event) throws InterruptedException;
}

