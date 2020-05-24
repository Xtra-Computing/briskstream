package applications.spout.generator;

import java.util.Random;

public class WCGenerator extends Generator<char[]> {

    final Random rand = new Random();
    private final String delimiter;
    private final int taskId;
    private final int number_of_words;

    public WCGenerator(int taskId, int number_of_words, String delimiter) {
        this.taskId = taskId;
        this.number_of_words = number_of_words;
        this.delimiter = delimiter;
    }

    @Override
    public char[] next() {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < number_of_words; i++) {
            sb.append(String.valueOf(rand.nextDouble()));
            if (i != number_of_words - 1) {
                sb.append(delimiter);
            }
        }

        return sb.toString().toCharArray();
    }

}
