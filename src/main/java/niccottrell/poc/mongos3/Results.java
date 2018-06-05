package niccottrell.poc.mongos3;

import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Results {

    /**
     * The time this LoadRunner started
     */
    private Date startTime;

    private Date lastIntervalTime;

    public enum Op {
        INSERTS,
        PUTS,
        ARCHIVES
    }

    private ConcurrentHashMap<Op, OpStats> opStats = new ConcurrentHashMap<>();

    long initialCount;

    Results() {
        startTime = new Date();
        lastIntervalTime = new Date();
        opStats.put(Op.INSERTS, new OpStats());
        opStats.put(Op.PUTS, new OpStats());
        opStats.put(Op.ARCHIVES, new OpStats());
    }

    /**
     * This returns inserts per second since we last called it
     * Rather than us keeping an overall figure
     */
    public HashMap<Op, Long> getOpsPerSecondLastInterval() {

        HashMap<Op, Long> result = new HashMap<>();

        Date now = new Date();
        long secondsSinceLastCheck = (now.getTime() - lastIntervalTime.getTime()) / 1000;

        for (Op op : Op.values()) {

            OpStats opStats = this.opStats.get(op);
            long total = opStats.totalOpsDone.get();
            long last = opStats.intervalCount.get();

            long opsPerInterval = (total - last) / secondsSinceLastCheck;
            result.put(op, opsPerInterval);

            opStats.intervalCount.set(total);
        }

        lastIntervalTime = now;

        return result;
    }

    /**
     * @return Total counts since the test launched
     */
    public HashMap<Op, Long> getOps() {
        HashMap<Op, Long> result = new HashMap<>();
        for (Op op : Op.values()) {
            OpStats opStats = this.opStats.get(op);
            result.put(op, opStats.totalOpsDone.get());
        }
        return result;
    }

    public long getSecondsElapsed() {
        return (new Date().getTime() - startTime.getTime()) / 1000;
    }

    public void incOps(Op op, int count) {
        opStats.get(op).totalOpsDone.addAndGet(count);
    }

    public class OpStats {
        AtomicLong intervalCount;
        AtomicLong totalOpsDone;
        AtomicLong slowOps;

        OpStats() {
            intervalCount = new AtomicLong(0);
            totalOpsDone = new AtomicLong(0);
            slowOps = new AtomicLong(0);
        }
    }
}
