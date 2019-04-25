package applications.param.lr;

import applications.datatype.internal.AvgVehicleSpeedTuple;
import engine.storage.SchemaRecordRef;

/**
 * Currently only consider position events.
 */
public class LREvent {
	private AvgVehicleSpeedTuple report;//event associated meta data.

	public SchemaRecordRef speed_value = new SchemaRecordRef();
	public SchemaRecordRef count_value = new SchemaRecordRef();

	/**
	 * creating a new LREvent.
	 * @param report
	 */
	public LREvent(AvgVehicleSpeedTuple report) {
		this.report = report;

	}
}