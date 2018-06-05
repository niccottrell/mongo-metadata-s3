package niccottrell.poc.mongos3.workers;

import niccottrell.poc.mongos3.Demo;
import niccottrell.poc.mongos3.Results;

public abstract class Worker implements Runnable {

    Demo demo;

    public Worker(Demo demo) {
        this.demo = demo;
    }

    protected boolean keepGoing() {
        return demo.getResults().getSecondsElapsed() < demo.getSettings().duration;
    }

    protected void incOps(Results.Op op, int count) {
        demo.getResults().incOps(op,count);
    }
}
