package com;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by guohongkuan on 2017/8/21.
 */
public class LeaderLatchDemo {
    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        CuratorFramework curator = CuratorFrameworkFactory.newClient("192.168.99.100:2181", retryPolicy);

        LeaderLatch leaderLatch = new LeaderLatch(curator, "/leaderlatch", "participant1");
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                System.out.println("I'm the leader now");
            }

            @Override
            public void notLeader() {
                System.out.println("I relinquish the leadership");
            }
        });
        curator.start();
        leaderLatch.start();
        leaderLatch.await();

        System.out.println("Is leader " + leaderLatch.hasLeadership());
        System.in.read();
        System.out.println("After delete node : Is leader " + leaderLatch.hasLeadership());
        System.in.read();
        System.out.println("After delete node : Is leader " + leaderLatch.hasLeadership());
        System.in.read();
        System.out.println("After reelect node : Is leader " + leaderLatch.hasLeadership());
        leaderLatch.close();
        System.in.read();
        System.out.println("After close : Is leader " + leaderLatch.hasLeadership());
        curator.close();
        Thread.sleep(5000000);
    }
}
