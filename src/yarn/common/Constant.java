package yarn.common;

public class Constant {
    public static String CONFIG_FILE_NAME = "hadoop.xml";

    public static String AM_CONFIG_PREFIX = "yarn.am";

    public static String CONTAINER_CONFIG_PREFIX = "yarn.container";

    public static long JOB_STATUS_INTERVAL_MSECS = 4000;

    public static String LOCAL_RESOURCE = "local-resource";

    public static String MIN_HEAP_SIZE = "min-heap-size-mb";

    public static String MAX_HEAP_SIZE = "max-heap-size-mb";

    public static String LAUNCH_CLASS_NAME = "launch-class-name";

    public static String OUT_LOG_FILE = "out-log-file";

    public static String ERROR_LOG_FILE = "error-log-file";

    public static String AM_REQUIRED_MEM = "yarn.am.required-memory-mb";

    public static String AM_REQUIRED_VIRTUAL_CORES = "yarn.am.required-virtual-cores";

    public static String QUEUE = "yarn.am.queue";

    public static String AM_HOST_NAME = "yarn.am.host-name";

    public static String AM_RPC_PORT = "yarn.am.rpc-port";

    public static String AM_TRACKING_URL = "yarn.am.tracking-url";

    public static String AM_REQUIRED_CONTAINER_COUNT = "yarn.am.required-containers-count";

    public static String AM_INTERVAL = "yarn.am.intervalMs";

    public static int DEFAULT_AM_REQUIRED_CONTAINER_COUNT = 10;

    public static String CONTAINER_REQUIRED_MEM = "yarn.container.required-memory-mb";

    public static String CONTAINER_REQUIRED_VIRTUAL_CORES = "yarn.container.required-virtual-cores";

    public static String CONTAINER_REQUIRED_PRIORITY = "yarn.container.required-priority";

    public static String MASTER_HANDLER = "yarn.am.handler";

    public static String TASK_HANDLER = "yarn.container.handler";

    public static String HDFS_LOCAL_RESOURCE_DIR = "yarn.hdfs.tmp.local-resource-dir";

    public static String HDFS_TMP_DIR = "yarn.hdfs.tmp.dir";

}
