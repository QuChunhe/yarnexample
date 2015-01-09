package yarn.example;

import java.io.EOFException;
import java.io.IOException;

import yarn.example.FileSplitter.FileSegement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WordReader {
    private static final Log log = LogFactory.getLog(WordReader.class);

    private FileSegement segement;
    private String word = null;
    private long len = 0;
    private FSDataInputStream in;

    public WordReader(Configuration conf, String file, FileSegement segement) {
        this.segement = segement;

        in = null;
        try {
            FileSystem fs = FileSystem.newInstance(conf);
            in = fs.open(new Path(file), 1024);
            in.seek(segement.start);
        } catch (IOException e) {
            log.error("Cannot open " + file, e);
        }
    }

    public boolean hasMore() {
        if (null == in) {
            log.error("Cannot open File");
            return false;
        }
        if (len > segement.length) {
            return false;
        }

        char ch = 0;
        Boolean hasException = false;
        do {
            try {
                len++;
                ch = (char) in.readByte();
                // log.info(len + " --- " + ch);
            } catch (EOFException e) {
                log.warn("Have read EOF", e);
                hasException = true;
            } catch (IOException e) {
                log.error("Cannot read", e);
                hasException = true;
            }
        } while ((!hasException) && (!Util.isLetter(ch)) && (len < segement.length));

        if (hasException || (len >= segement.length)) {
            return false;
        }
        StringBuilder builder = new StringBuilder(32);
        do {
            builder.append(ch);
            try {
                len++;
                ch = (char) in.readByte();
                // log.info(len + " +++ " + ch);
            } catch (EOFException e) {
                log.warn("Have read EOF", e);
                hasException = true;
            } catch (IOException e) {
                log.error("Cannot read", e);
                hasException = true;
            }
        } while ((!hasException) && (Util.isLetter(ch)) && (len < segement.length));

        if ((len >= segement.length) && (!hasException) && (Character.isLetter(ch))) {
            builder.append(ch);
        }
        word = builder.toString();
        return true;
    }

    public String nextWord() {
        return word;
    }
}
