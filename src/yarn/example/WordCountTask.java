package yarn.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import redis.clients.jedis.Jedis;
import yarn.common.Task;
import yarn.example.FileSplitter.FileSegement;

public class WordCountTask implements Task {
	private static final Log log = LogFactory.getLog(WordCountTask.class);
	
	private YarnConfiguration yarnConf;
	private String file;
	Jedis jedis;

	@Override
	public void run(String[] args) {
		long start = Long.parseLong(args[0]);
		long length = Long.parseLong(args[1]);
		FileSegement segement = new FileSegement(start,length);
		WordReader wordReader = new WordReader(yarnConf,file, segement);
		while(wordReader.hasMore()) {
			String word = wordReader.nextWord();
			if (word.length() < 2) {
				continue;
			}
			word = word.toLowerCase();
			long num = jedis.zincrby("wordcount", 1, word).longValue();
			log.info(word + " : " + num);			
		}
		jedis.close();
	}

	@Override
	public void init(YarnConfiguration yarnConf) {
		file = yarnConf.get("yarn.application.word-count.input-file");
		this.yarnConf = yarnConf;
		String redisAddress = yarnConf.get("yarn.application.redis.server-address",
                                           "localhost");
		int redisPort = yarnConf.getInt("yarn.application.redis.server-port",
		                                6379);
		jedis = new Jedis(redisAddress,redisPort);
	}

}
