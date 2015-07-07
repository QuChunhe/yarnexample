package yarn.common;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

public class SimpleClient {
    private static final Log log = LogFactory.getLog(SimpleClient.class);

    private String jobName;
    private YarnConfiguration yarnConf;
    private YarnClient client;
    private ApplicationId appId;
    private String tmpHdfsDir;

    private Resource checkResourceAvailable(YarnClientApplication app) {
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        log.info("Max mem capabililty of resources in this cluster " + maxMem);
        int requiredMem = yarnConf.getInt(Constant.AM_REQUIRED_MEM, -1);
        if ((requiredMem < 0) || (requiredMem > maxMem)) {
            log.warn(Constant.AM_REQUIRED_MEM + " is not setted or is greated than the max mem!");
            requiredMem = maxMem;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        log.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);
        int requireVCores = yarnConf.getInt(Constant.AM_REQUIRED_VIRTUAL_CORES, -1);
        if ((requireVCores < 0) || (requireVCores > maxVCores)) {
            log.warn(Constant.AM_REQUIRED_VIRTUAL_CORES
                    + " is not setted or is greated than the max virtual cores!");
            requireVCores = maxVCores;
        }

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(requiredMem);
        capability.setVirtualCores(requireVCores);
        return capability;
    }

    private String normalizeName() {
        String lowerCaseName = jobName.toLowerCase();
        boolean isNewWord = true;
        StringBuilder nameBuilder = new StringBuilder();
        for (int i = 0; i < lowerCaseName.length(); i++) {
            char c = lowerCaseName.charAt(i);
            if ((c >= 'a') && (c <= 'z')) {
                if (isNewWord) {
                    isNewWord = false;
                    nameBuilder.append((char) (c - 32));
                } else {
                    nameBuilder.append(c);
                }
            } else {
                isNewWord = true;
            }
        }
        return nameBuilder.toString();
    }

    private void createYarnClient() {
        client = YarnClient.createYarnClient();
        client.init(yarnConf);
        client.start();
    }

    private void copyFromLocal(FileSystem fs, String namePrefix, String tmpDir) {
        String name = namePrefix + "." + Constant.LOCAL_RESOURCE;
        String localJarFiles[] = yarnConf.getStrings(name);
        if (null == localJarFiles) {
            log.error(name + " must have values!");
            System.exit(1);
        }
        for (String localFile : localJarFiles) {
            log.info(localFile + " will be copied to HDFS " + tmpHdfsDir);
            String[] segment = localFile.split(File.separator);
            String fileName = segment[segment.length - 1].trim();
            Path src = new Path(localFile);
            Path dst = new Path(tmpHdfsDir + tmpDir + "/" + fileName);
            try {
                fs.copyFromLocalFile(false, true, src, dst);
            } catch (IOException e) {
                log.error(localFile + " cannot be copied to HDFS", e);
            }
        }
    }

    private void createLocalResource() {
        String tmpParentDir = yarnConf.get(Constant.HDFS_TMP_DIR, "/user/hadoop/tmp/");
        tmpHdfsDir = tmpParentDir + normalizeName() + System.currentTimeMillis() + "/";
        FileSystem fs = null;
        try {
            fs = FileSystem.get(yarnConf);
        } catch (IOException e) {
            log.error("Cannot get FileSystem from configuration!", e);
            System.exit(1);
        }
        copyFromLocal(fs, Constant.AM_CONFIG_PREFIX, "am");
        copyFromLocal(fs, Constant.CONTAINER_CONFIG_PREFIX, "container");
    }

    private void init() {
        createYarnClient();
        createLocalResource();
    }

    private ApplicationSubmissionContext buildApplicationSubmissionContext() {
        YarnClientApplication app = null;
        try {
            app = client.createApplication();
        } catch (YarnException e) {
            log.error("Cannot create YarnClientApplication because of YarnException", e);
            System.exit(1);
        } catch (IOException e) {
            log.error("Cannot create YarnClientApplication because of IOException", e);
            System.exit(1);
        }

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        log.info("Obtained new Application ID: " + appId);
        appContext.setApplicationName(jobName);
        appContext.setResource(checkResourceAvailable(app));
        appContext.setQueue(yarnConf.get(Constant.QUEUE, "default"));
        ContainerBuilder containerBuilder = new MasterContainerBuilder(yarnConf, tmpHdfsDir);
        appContext.setAMContainerSpec(containerBuilder.createLaunchContext());

        return appContext;
    }

    private void printFinalJobReport() {
        ApplicationReport report = null;
        try {
            report = client.getApplicationReport(appId);
        } catch (YarnException | IOException e) {
            log.error("Exception encountered while attempting to request " + "a final job report for "
                      + jobName, e);
            return;
        }
        FinalApplicationStatus finalAppStatus = report.getFinalApplicationStatus();
        long secs = (report.getFinishTime() - report.getStartTime()) / 1000L;
        String time = String.format("%d minutes, %d seconds.", secs / 60L, secs % 60L);
        log.info("Completed " + jobName + " : " + finalAppStatus.name() + ", total running time: " + time);
    }

    private void awaitJobCompletion() {
        do {
            try {
                Thread.sleep(Constant.JOB_STATUS_INTERVAL_MSECS);
            } catch (InterruptedException ir) {
                log.warn(jobName + " client is interrupted", ir);
            }
        } while (doesFinish() == false);

        printFinalJobReport();
    }

    public void cleanup() {
        try {
            FileSystem fs = FileSystem.get(yarnConf);
            fs.delete(new Path(tmpHdfsDir), true);
            log.info("Has deleted HDFS directory" + tmpHdfsDir);
        } catch (IOException e) {
            log.error("Cannot delete HDFS directory " + tmpHdfsDir, e);
        }
    }

    public SimpleClient(String jobName, String appConfFile) {
        this.jobName = jobName;
        this.yarnConf = new YarnConfiguration(new Configuration());
        yarnConf.addResource(Constant.CONFIG_FILE_NAME);
        yarnConf.addResource(appConfFile);
        this.appId = null;
    }

    public float getProgress() {
        float progress = 0;
        if (null == appId) {
            log.warn(jobName + " has not start!");
        } else {
            try {
                ApplicationReport report = client.getApplicationReport(appId);
                progress = report.getProgress();
            } catch (YarnException e) {
                log.error("Cannot get progress of " + jobName + " because of YarnException", e);
            } catch (IOException e) {
                log.error("Cannot get progress of " + jobName + " because of IOException", e);
            }
        }
        return progress;
    }

    public boolean doesFinish() {
        boolean done = false;
        ApplicationReport report = null;
        if (null == appId) {
            log.warn(jobName + " has not start!");
        } else {
            try {
                report = client.getApplicationReport(appId);
                YarnApplicationState state = report.getYarnApplicationState();
                log.info(jobName + " state : " + state.name());
                switch (state) {
                case FAILED:
                    log.warn(jobName
                            + " reports FAILED state, diagnostics show: "
                            + report.getDiagnostics());
                case KILLED:

                case FINISHED:
                    done = true;
                    break;
                default:
                    log.info(jobName + " state : "
                            + "attemptId " + report.getCurrentApplicationAttemptId() + " , "
                            + "Containers used: "
                            + report.getApplicationResourceUsageReport().getNumUsedContainers());
                    break;
                }
            } catch (YarnException | IOException e) {
                String diagnostics = (null == report) ? "" : "Diagnostics: " + report.getDiagnostics();
                log.error(jobName + " meet fatal error : " + diagnostics, e);
                try {
                    log.info("FORCIBLY KILLING Application : " + jobName);
                    client.killApplication(appId);
                } catch (YarnException | IOException ex) {
                    log.error("Exception raised in attempt to kill " + jobName, ex);
                }
            }
        }
        return done;
    }

    public void run(final boolean doesBlock) {
        init();
        ApplicationSubmissionContext appContext = buildApplicationSubmissionContext();

        try {
            log.info("Submitting " + jobName + " to ResourceManager");
            // obtain an "updated copy" of the appId for status checks/job kill later
            appId = client.submitApplication(appContext);
            log.info("Get appId " + appId + " after submitting " + jobName);
        } catch (YarnException e) {
            log.error("Can not sumbit " + jobName + " because of YarnException", e);
            System.exit(1);
        } catch (IOException e) {
            log.error("Can not submit " + jobName + " because of IOException", e);
            System.exit(1);
        }
        log.info(jobName + " has been submitted to ResourceManager");

        if (doesBlock) {
            awaitJobCompletion();
            cleanup();
        }        
    }
}
