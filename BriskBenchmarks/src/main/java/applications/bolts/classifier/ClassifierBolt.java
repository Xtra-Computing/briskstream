package applications.bolts.classifier;

import applications.constants.BaseConstants;
import applications.constants.FraudDetectionConstants;
import applications.model.predictor.ModelBasedPredictor;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class ClassifierBolt extends MapBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ClassifierBolt.class);
    private static final long serialVersionUID = 6445550040247603261L;
    double sel = 0;
    double nsel = 0;
    int cnt = 0;
    int loop = 1;
    private ModelBasedPredictor predictor;


    public ClassifierBolt() {
        super(LOG, new HashMap<>());
        this.read_selectivity = 1;
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 1.0);//workaround to ensure same output selectivity
        this.setStateful();
    }

    /**
     * Initilize this classifer.
     *
     * @param thread_Id
     * @param thisTaskId
     * @param graph
     */
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        LOG.info("Classifer has received a tuple: " + in.getString(0));
    }


    public void display() {
//		LOG.info("cnt:" + cnt + "\tcnt1:" + sel + "(" + (sel / cnt) + ")");
    }


    @Override
    public Fields getDefaultFields() {
        return new Fields(
                FraudDetectionConstants.Field.ENTITY_ID,
                FraudDetectionConstants.Field.SCORE,
                FraudDetectionConstants.Field.STATES);
    }
}