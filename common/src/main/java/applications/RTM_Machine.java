package applications;

public class RTM_Machine extends Platform {


    public RTM_Machine() {
        this.latency_L2 = 11.2;//measured latency in ns for each cache line sized tuple access.
        this.latency_LLC = 50;//measured latency in ns for each cache line sized tuple access.
        this.latency_LOCAL_MEM = 110;//measured latency in ns for each cache line access.
        this.cache_line = 64.0;//bytes
        this.CLOCK_RATE = 1.9;//GHz
        this.num_socket = 4;
        this.CoresPerSocket = 10;//cores per socket
        this.num_cores = 40;
        bandwidth_map = new double[][]{  //in MB/s
                {20564.8, 9944.0, 9284.1, 9773.4},
                {9892.5, 20705.0, 9310.0, 9822.4},
                {9892.5, 9310.0, 20705.0, 9822.4},
                {9892.5, 9310.0, 9822.4, 20705.0},
        };

        latency_map = new double[][]{// in ns
                {142.6, 327.5, 313.4, 316.0},
                {328.7, 145.6, 322.2, 324.1},
                {327.5, 328.6, 148.0, 325.5},
                {327.7, 327.0, 322.2, 145.1},
        };
    }
}
