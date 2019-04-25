package applications.bolts.lr.txn;

import applications.datatype.AbstractLRBTuple;
import applications.datatype.PositionReport;
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

	public TPBolt(Logger log, int fid) {
		super(log, fid);
	}

	protected void read_core(LREvent event) throws InterruptedException {
//
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
	 * @param event
	 * @param bid
	 * @throws DatabaseException
	 */
	private void txn_request(LREvent event, long bid) throws DatabaseException {
//		txn_context = new TxnContext(thread_Id, this.fid, bid, event.index_time);//create a new txn_context for this new transaction.
//		it simply construct the operations and return.
//		transactionManager.Asy_ModifyRecord(txn_context, "accounts", event.getAccountId(), new INC(event.getAccountTransfer()));// read and modify the account itself.
//		transactionManager.Asy_ModifyRecord(txn_context, "bookEntries", event.getBookEntryId(), new INC(event.getBookEntryTransfer()));// read and modify the asset itself.
	}

	/**
	 * Merge function of parser.
	 *
	 * @return
	 */
	private String[] parser(Tuple in) {
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
	private PositionReport dispatcher(String[] token) {
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

	private final SegmentIdentifier segment = new SegmentIdentifier();


	private AvgVehicleSpeedTuple update_avgsv(Integer vid, int speed) throws InterruptedException {
		Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);

		AvgVehicleSpeedTuple tuple = null;
		if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {// vehicle changes segment.

			SegmentIdentifier segId = vehicleEntry.getRight();

			//read and emit.

			// VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
			tuple = new AvgVehicleSpeedTuple(vid,
					this.currentMinute, segId.getXWay(), segId.getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage());
			// set to null to get new vehicle entry below
			vehicleEntry = null;
		}

		if (vehicleEntry == null) {//no record for this vehicle
			//write (insert).
			vehicleEntry = new MutablePair<>(new AvgValue(speed), this.segment.copy());
			this.avgSpeedsMap.put(vid, vehicleEntry);
		} else {// vehicle does not change segment but only update its speed.
			//write.
			vehicleEntry.getLeft().updateAverage(speed);
		}
		return tuple;
	}

	@Override
	public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

		BEGIN_PREPARE_TIME_MEASURE(thread_Id);

		long bid = in.getBID();


		//pre process.
		String[] token = parser(in);
		PositionReport report = dispatcher(token);
		int vid = report.getVid();
		int speed = report.getSpeed().intValue();
		this.segment.set(report);
		AvgVehicleSpeedTuple vehicleSpeedTuple = update_avgsv(vid, speed);

		LREvent event = new LREvent(vehicleSpeedTuple);
		//txn process.
		if (vehicleSpeedTuple != null) {
			//update segment statistics. Write and Read Requests.


		} else {
			//Simply Read statistics.

		}


		//post process.


	}

	protected abstract void write_handle(LREvent event, Long timestamp) throws DatabaseException, InterruptedException;

	protected abstract void read_handle(LREvent event, Long timestamp) throws InterruptedException, DatabaseException;
}
