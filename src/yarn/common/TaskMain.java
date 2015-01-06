package yarn.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class TaskMain {
	private static final Log log = LogFactory.getLog(TaskMain.class);
	
	public static void main(String[] args) {
		YarnConfiguration yarnConf = new YarnConfiguration(new Configuration());
		yarnConf.addResource(Constant.CONFIG_FILE_NAME);
		
		String className = yarnConf.get(Constant.TASK_HANDLER);
		if (null == className) {
			log.error(Constant.TASK_HANDLER + " must be set");
			System.exit(1);
		}
		Task task = null;
		try {
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			Class<?> taskHandlerClass =  classLoader.loadClass(className);
			task = (Task) taskHandlerClass.newInstance();
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
		
		task.init(yarnConf);
		task.run(args);
	}

}
