package spark.applications.constants;

public interface microbenchmarkConstants extends BaseConstants {
    String PREFIX = "mb";

    interface Field {
        String TIME = "time";
        String TEXT = "text";
        String STATE = "state";
    }

    interface Conf extends BaseConf {
        String EXECUTOR_THREADS = "mb.threads";
    }

    interface Component extends BaseComponent {
        String EXECUTOR = "executor";
    }
}
