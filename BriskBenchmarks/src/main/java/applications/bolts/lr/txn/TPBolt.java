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

    private void toll_process(long bid, Integer vid, Integer count, Double lav, short time) throws InterruptedException {
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

        // TODO get accurate emit time...
        final TollNotification tollNotification
                = new TollNotification(
                time, time, vid, lav, toll);

        this.collector.emit_single(bid, tollNotification);
    }

    protected void post_process(LREvent event) throws InterruptedException {
        Integer vid = event.getVSreport().getVid();
        int count = event.count_value.getRecord().getValue().getHashSet().size();
        double lav = event.speed_value.getRecord().getValue().getDouble();


        toll_process(event.getBid(), vid, count, lav, event.getPOSReport().getTime());
//        int sum = 0;
//        for (int i = 0; i < NUM_ACCESSES; ++i) {
//            SchemaRecordRef ref = event.getRecord_refs()[i];
//            try {
//                DataBox dataBox = ref.getRecord().getValues().get(1);
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


    protected void write_core(LREvent event) throws InterruptedException {
//        for (int i = 0; i < NUM_ACCESSES; ++i) {
//            List<DataBox> values = event.getValues()[i];
//            SchemaRecordRef recordRef = event.getRecord_refs()[i];
//            SchemaRecord record = recordRef.getRecord();
//            List<DataBox> recordValues = record.getValues();
//            recordValues.get(1).setString(values.get(1).getString(), VALUE_LEN);
//        }
//        collector.force_emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
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

            // set to null to get new vehicle entry below
            vehicleEntry = null;
        }

        if (vehicleEntry == null) {//no record for this vehicle
            //write (insert).
            vehicleEntry = new MutablePair<>(new AvgValue(speed), this.segment.copy());
            this.avgSpeedsMap.put(vid, vehicleEntry);

            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
            rt = new AvgVehicleSpeedTuple(vid, this.currentMinute, segment.getXWay(),
                    segment.getSegment(), segment.getDirection(), vehicleEntry.getLeft().getAverage(), time);

        } else {// vehicle does not change segment but only update its speed.
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

        //not supported.

    }

    protected abstract void write_handle(LREvent event, Long timestamp) throws DatabaseException, InterruptedException;

    protected abstract void read_handle(LREvent event, Long timestamp) throws InterruptedException, DatabaseException;


}

