package yarn.example;

import yarn.common.SimpleClient;

public class WordCount {

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient("word count", "WordCount.xml");
        client.run(true);
    }
}
