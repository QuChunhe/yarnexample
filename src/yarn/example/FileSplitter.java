package yarn.example;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSplitter {
    private static final Log log = LogFactory.getLog(FileSplitter.class);

    private Configuration conf;
    private String file;

    public FileSplitter(Configuration conf, String file) {
        this.conf = conf;
        this.file = file;
    }

    public static class FileSegement {
        public long start;
        public long length;

        public FileSegement() {
            this(0, 0);
        }

        public FileSegement(long start, long length) {
            this.start = start;
            this.length = length;
        }

    }

    public FileSegement[] splitWithWord(int groupNum) {
        if (groupNum < 2) {
            log.error("Group number " + groupNum + " is less than two");
            System.exit(1);
        }
        FileSystem fs = null;
        try {
            fs = FileSystem.newInstance(conf);
        } catch (IOException e) {
            log.error("Can not get the FileSystem", e);
            System.exit(1);
        }
        long fileLen = 0;
        try {
            FileStatus status = fs.getFileStatus(new Path(file));
            fileLen = status.getLen();
            log.info(file + " length : " + fileLen);
        } catch (IOException e) {
            log.error("Cannot get " + file + " status", e);
            System.exit(1);
        }

        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(new Path(file), 10240);
        } catch (IllegalArgumentException e) {
            log.error("Cann not open " + file + "because of IllegalArgumentException", e);
            System.exit(1);
        } catch (IOException e) {
            log.error("Cann not open " + file + "because of IOException", e);
            System.exit(1);
        }

        long segLen = fileLen / groupNum;
        if (segLen < 1) {
            log.warn("The size of " + file + " is too small");
            segLen = 1;
        }
        FileSegement[] segement = new FileSegement[groupNum];
        segement[0] = new FileSegement(0, segLen);
        for (int i = 1; i < groupNum; i++) {
            long pos = segement[i - 1].start + segement[i - 1].length - 1;
            try {
                inputStream.seek(pos);
                char ch;
                do {
                    pos++;
                    ch = (char) inputStream.readByte();
                } while (Util.isLetter(ch) && (pos < fileLen));
                segement[i - 1].length = pos - segement[i - 1].start;
                segement[i] = new FileSegement(pos, (i + 1) * segLen - pos);

                if (segement[i].length < 0) {
                    segement[i].length = 0;
                }
            } catch (EOFException e) {
                segement[i] = new FileSegement(pos, 0);
            } catch (IOException e) {
                log.error("Can not read " + file, e);
                System.exit(1);
            }
        }
        segement[groupNum - 1].length = fileLen - segement[groupNum - 1].start;
        return segement;
    }

}
