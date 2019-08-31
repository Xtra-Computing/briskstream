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

        };

        latency_map = new double[][]{// in ns

        };
    }
}
