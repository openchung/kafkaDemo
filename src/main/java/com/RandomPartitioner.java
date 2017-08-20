package com;
import java.util.concurrent.atomic.AtomicLong;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by guohongkuan on 2017/7/31.
 */
public class RandomPartitioner  implements Partitioner {

    private static AtomicLong next = new AtomicLong();

    public RandomPartitioner(VerifiableProperties verifiableProperties) {}

    @Override
    public int partition(Object key, int numPartitions) {
        long nextIndex = next.incrementAndGet();
        return (int)Math.random() * numPartitions;
    }
}


