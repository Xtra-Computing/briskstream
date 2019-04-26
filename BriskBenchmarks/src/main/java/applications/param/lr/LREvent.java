package applications.param.lr;

import applications.datatype.PositionReport;
import applications.datatype.internal.AvgVehicleSpeedTuple;
import engine.storage.SchemaRecordRef;

/**
 * Currently only consider position events.
 */
public class LREvent {

    private PositionReport posreport;//event associated meta data.
    private final AvgVehicleSpeedTuple vsreport;//intermediate input.


    public SchemaRecordRef speed_value = new SchemaRecordRef();
    public SchemaRecordRef count_value = new SchemaRecordRef();

    private final long bid;
    public double[] enqueue_time = new double[1];

    /**
     * creating a new LREvent.
     *
     * @param posreport
     * @param vehicleSpeedTuple
     * @param bid
     */
    public LREvent(PositionReport posreport, AvgVehicleSpeedTuple vehicleSpeedTuple, long bid) {
        this.posreport = posreport;
        vsreport = vehicleSpeedTuple;

        this.bid = bid;
    }

    public long getBid() {
        return this.bid;
    }

    public PositionReport getPOSReport() {
        return posreport;
    }

    public AvgVehicleSpeedTuple getVSreport() {
        return vsreport;
    }
}