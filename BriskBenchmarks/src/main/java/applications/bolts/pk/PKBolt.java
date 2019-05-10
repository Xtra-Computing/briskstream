package applications.bolts.pk;

import applications.param.PKEvent;
import brisk.components.operators.api.TransactionalBolt;
import engine.storage.datatype.DataBox;
import engine.storage.datatype.ListDoubleDataBox;
import org.slf4j.Logger;

import java.util.List;

import static applications.constants.PositionKeepingConstants.Constant.SIZE_EVENT;
import static applications.constants.PositionKeepingConstants.Constant.SIZE_VALUE;

public abstract class PKBolt extends TransactionalBolt {
    public PKBolt(Logger log, int fid) {
        super(log, fid);
    }

    /**
     * @param event
     */
    protected void PK_core(PKEvent event) throws InterruptedException {


        for (int i = 0; i < SIZE_EVENT; i++) {
            List<DataBox> srcRecord = event.getList_value_ref(i).getRecord().getValues();
            //compute.
            ListDoubleDataBox valueList = (ListDoubleDataBox) srcRecord.get(1);
            double sum = srcRecord.get(2).getDouble();

            double[] nextDouble = event.getValue(i);
            int j;
            for (j = 0; j < SIZE_VALUE; j++) {
                sum -= valueList.addItem(nextDouble[j]);
                sum += nextDouble[j];
            }
            //GetAndUpdate content.
            srcRecord.get(2).setDouble(sum);
            collector.force_emit(event.getBid(), true);

//            double movingAverageInstant;
//            //
//            if (valueList.size() < MOVING_AVERAGE_WINDOW) {//just added
//                movingAverageInstant = nextDouble[SIZE_VALUE - 1];
//            } else {
//                movingAverageInstant = sum / MOVING_AVERAGE_WINDOW;
//            }
//
//            boolean spike = Math.abs(nextDouble[SIZE_VALUE - 1] - movingAverageInstant) > SpikeThreshold * movingAverageInstant;
//            // measure_end the preconditions
//            collector.force_emit(input_event.getBid(), spike);
        }
    }



}
