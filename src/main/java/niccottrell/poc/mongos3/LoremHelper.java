package niccottrell.poc.mongos3;

import de.svenjacobs.loremipsum.LoremIpsum;

import java.util.Random;

public class LoremHelper {

    /**
     * Each thread has its own helper
     */
    private static ThreadLocal<LoremHelper> helpers = new ThreadLocal<>();

    /**
     * Each thread has its own generator
     */
    private final LoremIpsum loremIpsum = new LoremIpsum();

    /**
     * Each thread has its own buffer of words
     */
    private String[] loremText = null;

    private Random rng = new Random();

    public static String getWords(int length) {
        LoremHelper loremHelper = helpers.get();
        if (loremHelper == null) {
            loremHelper = new LoremHelper();
            helpers.set(loremHelper);
        }
        return loremHelper.moreWords(length);
    }

    /**
     * @param length Number of words to return
     */
    private String moreWords(int length) {

        if (loremText == null) {
            loremText = loremIpsum.getWords(1000).split("\\s+"); // split on spaces
        }

        Double d = rng.nextDouble();

        // start somewhere inside the buffer
        int start = (int) Math.abs(Math.floor(d * (loremText.length - length)));

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(loremText[start + i]);
            if (i < length) sb.append(" ");
        }

        return sb.toString();
    }


}
