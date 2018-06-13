package niccottrell.poc.mongos3;

import de.svenjacobs.loremipsum.LoremIpsum;

import java.util.ArrayList;
import java.util.List;
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
    private String loremText;

    private List<Integer> breakPoints = new ArrayList<>();

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
            // generate new text
            loremText = loremIpsum.getWords(1000).toLowerCase();
            for (int i = 0; i < loremText.length(); i++) {
                char ch = loremText.charAt(i);
                if (ch == ' ') {
                    breakPoints.add(i);
                }
            }
        }

        // start somewhere inside the buffer
        int startIdx = rng.nextInt(breakPoints.size() - length);
        int endIdx = startIdx + length;

        // return the substring without allocating a new char[]
        return loremText.substring(breakPoints.get(startIdx) + 1, breakPoints.get(endIdx));
    }

}
