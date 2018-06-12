package niccottrell.poc.mongos3;

import org.junit.Test;

public class LoremTest {

    @Test
    public void test1() {

        String l16 = LoremHelper.getWords(16);
        System.out.println(l16);
        String l32 = LoremHelper.getWords(32);
        System.out.println(l32);
        String l64 = LoremHelper.getWords(64);
        System.out.println(l64);

    }
}
