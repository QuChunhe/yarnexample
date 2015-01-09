package yarn.common;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private static final Log log = LogFactory.getLog(RMCallbackHandler.class);

    private YarnConfiguration yarnConf;

    /** Completed Containers Counter */
    private AtomicInteger completedCount;
    /** Failed Containers Counter */
    private AtomicInteger failedCount;
    /** Number of containers requested */
    private AtomicInteger allocatedCount;
    /** Number of successfully completed containers in this job run. */
    private AtomicInteger successfulCount;
    /** number of containers required */
    private final int requiredCount;

    private NMClientAsync nmClientAsync = null;

    private List<Container> launchedContainers = new LinkedList<Container>();

    private String taskInput[];

    private void launchContainers(final List<Container> allocatedContainers) {
        if (null == nmClientAsync) {
            NMClientAsync.CallbackHandler containerListener = new NMCallbackHandler();
            nmClientAsync = NMClientAsync.createNMClientAsync(containerListener);
            nmClientAsync.init(yarnConf);
            nmClientAsync.start();
        }

        int pos = allocatedCount.get() - allocatedContainers.size();
        for (Container container : allocatedContainers) {
            log.info("Launching command on a new container." + ", containerId=" + container.getId()
                    + ", containerNode=" + container.getNodeId().getHost() + ":"
                    + container.getNodeId().getPort() + ", containerNodeURI="
                    + container.getNodeHttpAddress() + ", containerResourceMemory="
                    + container.getResource().getMemory());
            String args = taskInput[pos++];
            ContainerBuilder containerBuilder = new TaskContainerBuilder(yarnConf, args);
            ContainerLaunchContext ctx = containerBuilder.createLaunchContext();
            nmClientAsync.startContainerAsync(container, ctx);
            launchedContainers.add(container);
        }
    }

    public RMCallbackHandler(YarnConfiguration yarnConf, String taskInput[]) {
        completedCount = new AtomicInteger(0);
        failedCount = new AtomicInteger(0);
        allocatedCount = new AtomicInteger(0);
        successfulCount = new AtomicInteger(0);
        this.yarnConf = yarnConf;
        requiredCount = yarnConf.getInt(Constant.AM_REQUIRED_CONTAINER_COUNT,
                Constant.DEFAULT_AM_REQUIRED_CONTAINER_COUNT);
        this.taskInput = taskInput;
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainerStatus) {
        log.info("Got response from RM for container ask, completedCnt=" + completedContainerStatus.size());
        for (ContainerStatus containerStatus : completedContainerStatus) {
            log.info("Got container status for containerID=" + containerStatus.getContainerId() + ", "
                    + "state=" + containerStatus.getState() + ", " + "exitStatus="
                    + containerStatus.getExitStatus() + ", " + "diagnostics="
                    + containerStatus.getDiagnostics());
            int exitStatus = containerStatus.getExitStatus();
            if (ContainerExitStatus.SUCCESS != exitStatus) {
                if (ContainerExitStatus.ABORTED != exitStatus) {
                    failedCount.incrementAndGet();
                }
            } else {
                successfulCount.incrementAndGet();
            }
            completedCount.incrementAndGet();
        }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
        log.info("Got response from RM for container ask, allocated containers count "
                + allocatedContainers.size());
        allocatedCount.addAndGet(allocatedContainers.size());
        log.info("Total allocated container count : " + allocatedCount.get());
        launchContainers(allocatedContainers);
    }

    @Override
    public void onShutdownRequest() {
        log.info("onShutdownRequest");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        log.info("onNodesUpdated : " + updatedNodes.toString());
    }

    @Override
    public float getProgress() {
        float progress = (float) completedCount.get() / requiredCount;
        return progress;
    }

    @Override
    public void onError(Throwable e) {
        log.error("onError : ", e);
    }

    public boolean doesFinish() {
        return successfulCount.get() == requiredCount;
    }

    public void stop() {
        if (launchedContainers.isEmpty()) {
            return;
        }
        for (Container container : launchedContainers) {
            nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
        }
        launchedContainers.clear();
        nmClientAsync.stop();
    }

    public boolean isSuccessfull() {
        boolean success = false;
        if ((failedCount.get() == 0) && (completedCount.get() == requiredCount)) {
            success = true;
        } else {
            log.info("total=" + requiredCount + ", completed=" + completedCount.get() + ", allocated="
                    + allocatedCount.get() + ", failed=" + failedCount.get());
        }
        return success;
    }
}
