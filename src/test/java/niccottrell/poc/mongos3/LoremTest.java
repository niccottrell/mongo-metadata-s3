package niccottrell.poc.mongos3;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class LoremTest {

    @Test
    public void test1() {
        String l1 = doTest(16);
        String l2 = doTest(16);
        Assert.assertNotEquals(l1, l2);
        doTest(32);
        doTest(64);
    }

    private String doTest(int length) {
        String l16 = LoremHelper.getWords(length);
        System.out.println(l16);
        Assert.assertEquals(length, StringUtils.countMatches(l16, " ") + 1);
        Assert.assertEquals(StringUtils.trim(l16), l16);
        return l16;
    }

}
