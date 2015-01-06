package yarn.common;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

public interface MasterHandler {
	
	void init(YarnConfiguration yarnConf);
	
	String[] divide(int allocatedContainerCount);
	
	void combine();

}
