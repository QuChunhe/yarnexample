package yarn.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public abstract class ContainerBuilder {
	private static final Log log = LogFactory.getLog(ContainerBuilder.class);
	
	private String namePrefix;
	protected Configuration conf;	
	
	private LocalResource buildLocalResource(FileSystem fs,
			                                 Path jarPath) throws IOException {
		log.info(jarPath.toString() +" will be added to LocalResource");		

		FileStatus dstStatus = fs.getFileStatus(jarPath);
		LocalResource jarResource = Records.newRecord(LocalResource.class);
		jarResource.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
		jarResource.setSize(dstStatus.getLen());
		jarResource.setTimestamp(dstStatus.getModificationTime());
		jarResource.setType(LocalResourceType.FILE); 
		jarResource.setVisibility(LocalResourceVisibility.APPLICATION);
		
		return jarResource;		
	}
	
	protected Map<String, String> buildEnvironment() {
		Map<String, String> env = Maps.<String, String> newHashMap();

		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$());
		classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String cpEntry : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
		                                      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR)
			            .append(cpEntry.trim()); 
		}
		classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
		env.put("CLASSPATH", classPathEnv.toString());
		log.info("Environment for Container :" + env);
		return env;
	}
	
	private String getFileName(Path jarPath) {
		String[] segement = jarPath.toString().split(Path.SEPARATOR);
		return segement[segement.length-1];
	}
	
	protected Map<String, LocalResource> buildLocalResourceMap() {
		String tmpDir = getLocalResourceDir();				
		Map<String, LocalResource> localResourceMap =Maps.<String, LocalResource>newHashMap();	
		try {	
			FileSystem fs = FileSystem.get(conf);
			RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(tmpDir),false);
			while (files.hasNext()) {
				Path jarPath = files.next().getPath();
				LocalResource localResource = null;
				try {
					localResource = buildLocalResource(fs, jarPath);
				} catch (IOException e) {
					log.error("Cannot add " + jarPath.toString()
							+ " to LocalResource map!", e);
				}
				if (null != localResource) {
					localResourceMap.put(getFileName(jarPath), localResource);
				}
			}
		} catch (IOException e) {
			log.error("Cannot get the LocalResource files in " + tmpDir, e);
			System.exit(1);
		}
	    return localResourceMap;
	}
	
	private List<String> buildExecCommand() {
		String className = conf.get(namePrefix + "." + Constant.LAUNCH_CLASS_NAME);
		if (null == className) {
			log.error("The name of the class to be launched must "
					  + "be specified in the configuration file!");
			System.exit(1);
		}
		
		int minHeapSize = conf.getInt(namePrefix + "." + Constant.MIN_HEAP_SIZE, 128);
		int maxHeapSize = conf.getInt(namePrefix + "." + Constant.MAX_HEAP_SIZE, 128);
		String outlog = conf.get(namePrefix + "." + Constant.OUT_LOG_FILE, "out.log");
		String errlog = conf.get(namePrefix + "." + Constant.ERROR_LOG_FILE, "err.log");
		String cmd = "${JAVA_HOME}/bin/java " + "-cp .:${CLASSPATH} "+
	      "-Xmx" + maxHeapSize + "M " + "-Xms" + minHeapSize + "M " + 
	       className + " "+ getInputArguments() + " " +
	      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + outlog + " " +
	      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + errlog + " ";
		log.info("ExecCommand : " + cmd);
	    return ImmutableList.of(cmd);
	}
	
	abstract protected String getLocalResourceDir();
	
	abstract protected String getInputArguments();
	
	abstract protected ByteBuffer getAllTokens();
	
	public ContainerBuilder(Configuration conf,String namePrefix) {
		this.conf = conf;
		this.namePrefix = namePrefix;
	} 
	
	public ContainerLaunchContext createLaunchContext() {
		ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
	
		ByteBuffer allTokens = getAllTokens();
		if (null != allTokens) {
			context.setTokens(allTokens);
		}
		
		context.setEnvironment(buildEnvironment());
		context.setLocalResources(buildLocalResourceMap());
		context.setCommands(buildExecCommand());
		// context.setResource(buildContainerMemory());
		// context.setUser(ApplicationConstants.Environment.USER.name());
		return context;
	}

}
