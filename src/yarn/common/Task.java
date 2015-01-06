package yarn.common;

import org.apache.hadoop.yarn.conf.YarnConfiguration;


public interface Task {
	
	void init(YarnConfiguration yarnConf);
	
	void run(String[] args);

}
