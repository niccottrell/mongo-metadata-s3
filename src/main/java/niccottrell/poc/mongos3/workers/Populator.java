package niccottrell.poc.mongos3.workers;

import niccottrell.poc.mongos3.Demo;
import niccottrell.poc.mongos3.Results;

@SuppressWarnings("FieldCanBeLocal")
public class Populator extends Worker {

    int threadIdx;

    private int batchSize = 50;

    public Populator(Demo demo, int threadIdx) {
        super(demo, 0);
        this.threadIdx = threadIdx;
    }

    @Override
    public void run() {
        System.out.format("Populator thread %s started.\n", threadIdx);
        while (keepGoing()) {
            demo.populateMongo(batchSize, false);
            incOps(Results.Op.INSERTS, batchSize);
        }
    }

}
