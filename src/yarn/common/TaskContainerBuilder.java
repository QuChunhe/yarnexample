package yarn.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

public class TaskContainerBuilder extends ContainerBuilder {
    private static final Log log = LogFactory.getLog(TaskContainerBuilder.class);

    private String subproblemInput;
    private ByteBuffer allTokens = null;

    public TaskContainerBuilder(Configuration conf, String subproblemInput) {
        super(conf, "yarn.container");
        this.subproblemInput = subproblemInput;
    }

    @Override
    protected String getLocalResourceDir() {
        String tmpDir = conf.get(Constant.HDFS_LOCAL_RESOURCE_DIR) + "container/";
        return tmpDir;
    }

    @Override
    protected String getInputArguments() {
        return subproblemInput;
    }

    @Override
    protected ByteBuffer getAllTokens() {
        if (null !=allTokens) {
            return allTokens;
        }
        
        DataOutputBuffer dob = new DataOutputBuffer();
        Credentials credentials = null;
        try {
            credentials = UserGroupInformation.getCurrentUser().getCredentials();
            credentials.writeTokenStorageToStream(dob);
        } catch (IOException e) {
            log.error("Cannot get Credentials", e);
        }

        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        log.info("Executing with tokens:");
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            log.info(token);
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        // Create appSubmitterUgi and add original tokens to it
        String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
        UserGroupInformation appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);
        return allTokens;
    }

}
