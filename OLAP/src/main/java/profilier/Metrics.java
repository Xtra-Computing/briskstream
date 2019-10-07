package profilier;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import static profilier.CONTROL.kMaxThreadNum;

public class Metrics {

    private static Metrics ourInstance = new Metrics();


    public DescriptiveStatistics[] total = new DescriptiveStatistics[kMaxThreadNum];//Total execution time.

    /**
     * Specially for T-Stream..
     */

    public DescriptiveStatistics[] enqueue_time = new DescriptiveStatistics[kMaxThreadNum];//event enqueue


    private Metrics() {
    }

    public static Metrics getInstance() {
        return ourInstance;
    }
}