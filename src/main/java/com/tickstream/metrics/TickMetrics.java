package com.tickstream.metrics;

import java.util.concurrent.atomic.LongAdder;

public class TickMetrics {
    // this LongAdder prevents cpu cache-line contention (False Sharing)
    //by stripping counters across physical cores.
    private final LongAdder processedCount = new LongAdder();
    private final LongAdder droppedCount = new LongAdder();

    public void recordProcessed(){
        processedCount.increment();
    }
    public void recordDropped(){
        droppedCount.increment();
    }
    public long getProcessedAndReset(){
        return processedCount.sumThenReset();
    }
    public long getDroppedAndReset(){
        return droppedCount.sumThenReset();
    }
}
