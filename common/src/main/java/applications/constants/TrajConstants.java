package applications.constants;

public interface TrajConstants extends BaseConstants {
    String PREFIX = "traj";
    int max_hz = 450000;

    interface Field extends BaseField {
        String POINT = "point";
    }

    interface Conf extends BaseConf {
        String COMPRESSOR_THREADS = "traj.splitter.threads";
    }

    interface Component extends BaseComponent {
        String COMPRESSOR = "compressor";
    }
}
