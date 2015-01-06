package yarn.common;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MasterContainerBuilder extends ContainerBuilder {
	private static final Log log = LogFactory.getLog(MasterContainerBuilder.class);
	
	private String tmpHdfsDir;

	public MasterContainerBuilder(Configuration conf, String tmpHdfsDir) {
		super(conf, Constant.AM_CONFIG_PREFIX);
		this.tmpHdfsDir = tmpHdfsDir;
	}

	@Override
	protected String getLocalResourceDir() {
		String tmpDir = tmpHdfsDir + "am/";
		return tmpDir;
	}

	@Override
	protected String getInputArguments() {
		return tmpHdfsDir;
	}

	@Override
	protected ByteBuffer getAllTokens() {
		ByteBuffer fsTokens = null;
		if (UserGroupInformation.isSecurityEnabled()) {
			String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
			if (tokenRenewer == null || tokenRenewer.length() == 0) {
				log.error("Can't get Master Kerberos principal for the RM to use as renewer");
			}
			Credentials credentials = new Credentials();
			FileSystem fs;
			try {
				fs = FileSystem.get(conf);
				// For now, only getting tokens for the default file-system.
				final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
				if (tokens != null) {
					for (Token<?> token : tokens) {
						log.info("Get delegation tokens for " + fs.getUri() + " : "	+ token);
					}
				}
				DataOutputBuffer dob = new DataOutputBuffer();
				credentials.writeTokenStorageToStream(dob);
				fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
				log.info("The file system tokens : " +  fsTokens);
			} catch (IOException e) {
				log.error("Can not get the file system tokens", e);
			}
		} else {
			log.info("Be not SecurityEnabled!");
		}
		return  fsTokens;
	}

}
