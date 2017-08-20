package com;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by guohongkuan on 2017/8/21.
 */
public class LeaderSelectorDemo {
    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        CuratorFramework curator = CuratorFrameworkFactory.newClient("192.168.99.100:2181", retryPolicy);

        LeaderSelector leaderSelector = new LeaderSelector(curator, "/leaderselector", new CustomizedAdapter());
        leaderSelector.autoRequeue();
        curator.start();
        leaderSelector.start();
        Thread.sleep(1000000);
        leaderSelector.close();
        curator.close();
    }

    private static class CustomizedAdapter extends LeaderSelectorListenerAdapter {
        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            System.out.println("Take the leadership");
            Thread.sleep(3000);
        }
    }
}
