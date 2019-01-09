package applications.bolts.mb;

import applications.param.MicroEvent;
import brisk.components.operators.api.TransactionalBolt;
import engine.storage.SchemaRecord;
import engine.storage.SchemaRecordRef;
import engine.storage.datatype.DataBox;
import org.slf4j.Logger;

import java.util.List;

import static applications.CONTROL.SIZE_EVENTS;
import static applications.CONTROL.enable_speculative;
import static applications.constants.MicroBenchmarkConstants.Constant.VALUE_LEN;

public abstract class MBBolt extends TransactionalBolt {
    public MBBolt(Logger log, int fid) {
        super(log, fid);
    }

    protected MicroEvent[] input_events = new MicroEvent[SIZE_EVENTS];

    protected void read_core(MicroEvent event) throws InterruptedException {

        int sum = 0;
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];

            DataBox dataBox = ref.record.getValues().get(1);

            int read_result = Integer.parseInt(dataBox.getString().trim());
            sum += read_result;

        }

        if (enable_speculative) {
            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
            //now we assume it's all correct for testing its upper bond.
            //so nothing is send out.

        } else
            collector.force_emit(event.getBid(), sum, event.getEmit_timestamp());//the tuple is finished.
    }


    protected void write_core(MicroEvent event) throws InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            List<DataBox> values = event.getValues()[i];
            SchemaRecordRef recordRef = event.getRecord_refs()[i];
            SchemaRecord record = recordRef.record;
            List<DataBox> recordValues = record.getValues();
            recordValues.get(1).setString(values.get(1).getString(), VALUE_LEN);

        }
        collector.force_emit(event.getBid(), true, event.getEmit_timestamp());//the tuple is finished.

    }

    private int count = 0;

    protected MicroEvent next_event(long bid, Long timestamp) {
        MicroEvent event = input_events[count++];
        event.setBid(bid);
        event.setEmit_timestamp(timestamp);
        if (count == SIZE_EVENTS)
            count = 0;
        return event;
    }

    public void prepareEvents() {
        for (int i = 0; i < SIZE_EVENTS; i++) {
            input_events[i] = generateEvent();
        }
    }
}
