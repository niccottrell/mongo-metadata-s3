package niccottrell.poc.mongos3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class Reporter implements Runnable {

    private static final DateFormat DF_FULL = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateFormat DF_TIME = new SimpleDateFormat("HH:mm:ss");

    private final Results results;
    private final Demo demo;

    public Reporter(Results results, Demo demo) {
        this.results = results;
        this.demo = demo;
    }

    @Override
    public void run() {

        Long insertsDone = results.getOps().get(Results.Op.INSERTS);

        if (results.getSecondsElapsed() < demo.getSettings().reportTime)
            return;

        Date now = new Date();

        System.out.format("After %d seconds (%s), %,d new records inserted - collection has %,d in total \n",
                results.getSecondsElapsed(), DF_TIME.format(now), insertsDone, results.initialCount + insertsDone);

        HashMap<Results.Op, Long> opsInterval = results.getOpsPerSecondLastInterval();
        System.out.format("%,d inserts,\n\t%,d S3 puts,\n\t%,d archived per second since last report\n",
                opsInterval.get(Results.Op.INSERTS), opsInterval.get(Results.Op.PUTS), opsInterval.get(Results.Op.ARCHIVES));
    }

    public void finalReport() {

        Long opsDone = results.getOps().get(Results.Op.INSERTS);

        Long secondsElapsed = results.getSecondsElapsed();

        System.out.println("------------------------");
        System.out.format("After %d seconds, %d new records inserted - collection has %d in total \n",
                secondsElapsed, opsDone, results.initialCount + opsDone);

        System.out.format("%d operations per second on average", (int) (1f * opsDone / secondsElapsed));
        System.out.println();

        System.out.println();

    }
}
