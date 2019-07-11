package applications.ml.bolts;

import applications.constants.BaseConstants;
import applications.constants.FraudDetectionConstants;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public abstract class LearnerBolt extends MapBolt {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerBolt.class);
    private static final long serialVersionUID = 6445550040247603261L;
    double sel = 0;
    double nsel = 0;
    int cnt = 0;
    int loop = 1;

    public LearnerBolt() {
        super(LOG, new HashMap<>());
        this.read_selectivity = 1;
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 1.0);//workaround to ensure same output selectivity
        this.setStateful();
    }

    /**
     * Initilize this Learner.
     *
     * @param thread_Id
     * @param thisTaskId
     * @param graph
     */
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {














        LOG.info("Successfully instantiating Learner");
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        LOG.info("Learner has received a tuple: " + in.getString(0));
    }


    @Override
    public Fields getDefaultFields() {
        return new Fields(
                FraudDetectionConstants.Field.ENTITY_ID,
                FraudDetectionConstants.Field.SCORE,
                FraudDetectionConstants.Field.STATES);
    }
}