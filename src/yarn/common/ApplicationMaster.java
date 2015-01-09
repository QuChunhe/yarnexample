package yarn.common;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {
    private static final Log log = LogFactory.getLog(ApplicationMaster.class);

    private RMCallbackHandler rmCallbackHandler;
    private AMRMClientAsync<ContainerRequest> amRMClient;
    private YarnConfiguration yarnConf;
    private int requiredContainerCount;
    private MasterHandler masterHandler;
    private String subproblemInput[];

    protected void checkEnvironment() {
        Map<String, String> envs = System.getenv();
        ApplicationAttemptId appAttemptID;

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            throw new IllegalArgumentException("Application Attempt Id not set in the environment");

        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
        }

        log.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId()
                + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());
    }

    private void registerRMCallBackHandler() {
        rmCallbackHandler = new RMCallbackHandler(yarnConf, subproblemInput);
        int intervalMs = yarnConf.getInt(Constant.AM_INTERVAL, 2000);
        amRMClient = AMRMClientAsync.createAMRMClientAsync(intervalMs, rmCallbackHandler);
        amRMClient.init(yarnConf);
        amRMClient.start();
        log.info("Have started AMRMClientAsync");
    }

    private RegisterApplicationMasterResponse registerAMToRM() {
        RegisterApplicationMasterResponse response = null;
        try {
            String appMasterHostname = "";
            int appMasterRpcPort = 0;
            String appMasterTrackingUrl = "";
            response = amRMClient.registerApplicationMaster(appMasterHostname,
                                                            appMasterRpcPort,
                                                            appMasterTrackingUrl);
        } catch (IOException ioe) {
            log.error("Cannot register ApplicationMaster because of IOException", ioe);
            System.exit(1);
        } catch (YarnException e) {
            log.error("Cannot register ApplicationMaster because of YarnException", e);
            System.exit(1);
        }
        return response;
    }

    private ContainerRequest buildContainerRequest(int requiredMemory,
                                                   int requiredVirtualCores,
                                                   int requiredPriority) {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(requiredPriority);

        // Set up resource type requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(requiredMemory);
        capability.setVirtualCores(requiredVirtualCores);

        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        log.info("Requested container ask: " + request.toString());
        return request;
    }

    private void madeAllContainerRequestToRM() {
        // Memory to request for the container
        int requiredMemory = yarnConf.getInt(Constant.CONTAINER_REQUIRED_MEM, 128);
        // VirtualCores to request for the container
        int requiredVirtualCores = yarnConf.getInt(Constant.CONTAINER_REQUIRED_VIRTUAL_CORES, 1);
        // Priority of the request
        int requiredPriority = yarnConf.getInt(Constant.CONTAINER_REQUIRED_PRIORITY, 10);

        RegisterApplicationMasterResponse response = registerAMToRM();
        if (null != response) {
            int maxMem = response.getMaximumResourceCapability().getMemory();
            log.info("Max mem capabililty of resources in this cluster " + maxMem);

            int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
            log.info("Max vcores capabililty of resources in this cluster " + maxVCores);

            if (requiredMemory > maxMem) {
                log.info("Container memory specified above max threshold of cluster." + " Using max value."
                        + ", specified=" + requiredMemory + ", max=" + maxMem);
                requiredMemory = maxMem;
            }

            if (requiredVirtualCores > maxVCores) {
                log.info("Container virtual cores specified above max threshold of cluster."
                        + " Using max value." + ", specified=" + requiredVirtualCores + ", max=" + maxVCores);
                requiredVirtualCores = maxVCores;
            }
        }

        for (int i = 0; i < requiredContainerCount; ++i) {
            ContainerRequest containerAsk = buildContainerRequest(requiredMemory,
                                                                  requiredVirtualCores,
                                                                  requiredPriority);
            amRMClient.addContainerRequest(containerAsk);
        }
    }

    private void awaitTaskCompletion() {
        log.info("Wait to finish ...");
        while (rmCallbackHandler.doesFinish() == false) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                log.info("Thread was interrupted!", e);
            }
        }
        log.info("Have finished!");
    }

    private void cleanup() {
        FinalApplicationStatus appStatus;
        String appMessage = null;
        if (rmCallbackHandler.isSuccessfull()) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics.";
        }
        rmCallbackHandler.stop();
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            log.error("Failed to unregister application because of YarnException", ex);
        } catch (IOException e) {
            log.error("Failed to unregister application because of IOException", e);
        }
        amRMClient.stop();
    }

    private MasterHandler loadMasterHandler() {
        String className = yarnConf.get(Constant.MASTER_HANDLER);
        if (null == className) {
            log.error(Constant.MASTER_HANDLER + " must set in configuration file");
            System.exit(1);
        }

        MasterHandler masterHandler = null;
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> masterHandlerClass = classLoader.loadClass(className);
            masterHandler = (MasterHandler) masterHandlerClass.newInstance();
        } catch (ClassNotFoundException e) {
            log.error(className + " can not been loded", e);
            System.exit(1);
        } catch (InstantiationException e) {
            log.error(className + " can not been instantiated", e);
            System.exit(1);
        } catch (IllegalAccessException e) {
            log.error(className + " can not been accessed", e);
            System.exit(1);
        }

        return masterHandler;
    }

    private void init() {
        log.info("Begin to initiate ApplicationMaster");
        masterHandler = loadMasterHandler();
        masterHandler.init(yarnConf);
    }

    public ApplicationMaster(String[] args) {
        if (args.length < 1) {
            log.error("ApplicationMaster need at least one args for LocalResouce Directory!");
            System.exit(1);
        }
        String tmpLocalResourceDir = args[0];
        log.info("input temporary LocalResource Directory : " + tmpLocalResourceDir);

        yarnConf = new YarnConfiguration(new Configuration());
        yarnConf.addResource(Constant.CONFIG_FILE_NAME);
        yarnConf.set(Constant.HDFS_LOCAL_RESOURCE_DIR, tmpLocalResourceDir);

        requiredContainerCount = yarnConf.getInt(Constant.AM_REQUIRED_CONTAINER_COUNT,
                                                 Constant.DEFAULT_AM_REQUIRED_CONTAINER_COUNT);
        log.info("requiredContainerCount : " + requiredContainerCount);
    }

    public void run() {
        log.info("ApplicationMaster begin to run");
        init();
        subproblemInput = masterHandler.divide(requiredContainerCount);

        checkEnvironment();
        registerRMCallBackHandler();
        madeAllContainerRequestToRM();
        awaitTaskCompletion();

        masterHandler.combine();
        cleanup();
    }
}
