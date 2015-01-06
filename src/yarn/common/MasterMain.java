package yarn.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MasterMain {
	private static final Log log = LogFactory.getLog(MasterMain.class);
	
	public static void main(String[] args) {
		log.info("MasterMain has " + args.length +" args");
		ApplicationMaster master = new ApplicationMaster(args);
		master.run();		
	}

}
