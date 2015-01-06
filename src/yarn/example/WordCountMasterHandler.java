package yarn.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import yarn.common.MasterHandler;


public class WordCountMasterHandler implements MasterHandler{
	private static final Log log = LogFactory.getLog(WordCountMasterHandler.class);
	
	private  FileSplitter fileSplitter;

	@Override
	public void init(YarnConfiguration yarnConf) {
		String file = yarnConf.get("yarn.application.word-count.input-file");
		fileSplitter = new FileSplitter(yarnConf,file);
	}

	@Override
	public String[] divide(int allocatedContainerCount) {
		FileSplitter.FileSegement[] segement = fileSplitter.splitWithWord(allocatedContainerCount);		
		String[] args = new String[allocatedContainerCount];
		for(int i = 0; i < allocatedContainerCount; i++) {
			args[i] = segement[i].start + " " + segement[i].length;
		}
		return args;
	}

	@Override
	public void combine() {
		log.info("hava combined!");
		
	}

}
