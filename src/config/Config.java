package config;

/**
 * Created by CGJ on 14-11-13.
 */
public class Config {
    public static final String MASTER_NODE = "localhost";
    public static final String[] SLAVE_NODES = {
//           "unix2.andrew.cmu.edu",
//	       "unix3.andrew.cmu.edu",
//	       "unix4.andrew.cmu.edu"
            "localhost"
    };
    // port for transferring data in mapreduce phase
    public static final int DATA_PORT = 19782;

    // port for sending task message
    public static final int TASK_PORT = 18432;

    // Number of replica per data block
    public static final int NUM_WHFS_REPLICA = 2;

    // Number of lines per block
    public static final int BLOCK_SIZE = 400;

    // WHFS namenode port
    public static final int NAMENODE_PORT = 15826;

    // Heartbeat timeout in ms
    public static final int HEARTBEAT_TIMEOUT = 10000;

    // DataNode file server port
    public static final int DATANODE_FILE_PORT = 15827;

    // Local base path
    public static final String LOCAL_BASE_PATH = "/tmp/mlocal/";

    // WHFS base path
    public static final String WHFS_BASE_PATH = "/tmp/wwhfs/";

    // map and reduce result file folder
    public static final String MAP_RESULTS_FOLDER = "/tmp/final/";

    // WHFS temp path
    public static final String WHFS_TEMP_PATH = "/tmp/fstemp/";
}
