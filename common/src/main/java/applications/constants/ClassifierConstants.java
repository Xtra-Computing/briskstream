package applications.constants;

public interface ClassifierConstants extends BaseConstants {
    String PREFIX = "classifier";
    String DEFAULT_MODEL = "";//to be defined.
    int max_hz = 450000;

    interface Conf extends BaseConf {
        String LEARNER_THREADS = "learner.threads";
        String EVALUTOR_THREADS = "evalutor.threads";
    }

    interface Component extends BaseComponent {
        String LEARNER = "learnerBolt";
        String EVALUATOR = "evaluatorBolt";
    }

    interface Field extends BaseField {
        String RECORD_KEY = "RECORD_KEY";
        String ENTITY_ID = "entityID";
        String RECORD_DATA = "recordData";
        String SCORE = "score";
        String STATES = "states";
    }

}
