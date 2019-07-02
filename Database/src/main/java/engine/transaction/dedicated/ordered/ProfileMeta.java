package engine.transaction.dedicated.ordered;

import static applications.CONTROL.kMaxThreadNum;

public class ProfileMeta {

    static int[] size = new int[kMaxThreadNum];//submitted task size.
    static long[] submit_time = new long[kMaxThreadNum];//time takes in job submission.
}
