package niccottrell.poc.mongos3.workers;

import niccottrell.poc.mongos3.Demo;
import niccottrell.poc.mongos3.Results;

public abstract class Worker implements Runnable {

    Demo demo;

    private long sleepMs;

    public Worker(Demo demo, long sleepMs) {
        this.demo = demo;
        this.sleepMs = sleepMs;
    }

    protected boolean keepGoing() {
        return demo.getResults().getSecondsElapsed() < demo.getSettings().duration;
    }

    protected void incOps(Results.Op op, int count) {
        demo.getResults().incOps(op, count);
    }

    protected void sleep() {
        sleep(this.sleepMs);
    }

    protected void sleep(long sleepMs) {
        if (sleepMs > 0) {
            try {
                // System.out.format("%s sleeping for %sms\n", this.getClass().getSimpleName(), sleepMs);
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
