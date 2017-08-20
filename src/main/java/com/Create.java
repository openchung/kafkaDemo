package com;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created by guohongkuan on 2017/8/21.
 */



public class Create {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        final ZooKeeper zk = new ZooKeeper("192.168.99.100:2181", 6000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("State : " + watchedEvent.getState());
            }
        });

        if (!zk.getChildren("/", false).contains("chroot")){
            zk.create("/chroot", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String nodeName = zk.create("/chroot/leader", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        nodeName = nodeName.replace("/chroot/", "");

        leader(zk, nodeName);

        Thread.sleep(100000);
        zk.close();

    }

    private static void leader(ZooKeeper zk, String nodeName) throws KeeperException, InterruptedException {
        List<String> leaders = zk.getChildren("/chroot", false);
        Collections.sort(leaders);
        System.out.println(leaders);
        Boolean isLeader = false;

        if (leaders.get(0).equals(nodeName)){
            isLeader = true;
        }
        System.out.println(nodeName + "竞选" + (isLeader ? "成功" : "失败"));

        if (! isLeader && leaders.size() > 1){
            String lastNode = "/chroot";
            for (int i = 1; i < leaders.size(); i++) {
                if (nodeName.equals(leaders.get(i))){
                    lastNode = "/chroot/" + leaders.get(i - 1);
                    break;
                }
            }
            System.out.println("监控 " + lastNode);
            zk.exists(lastNode, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println(watchedEvent.getPath() + " | " + watchedEvent.getType().name());
                    if (watchedEvent.getType().name().equals("NodeDeleted")){
                        try {
                            leader(zk, nodeName);
                        } catch (KeeperException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    try {
                        zk.exists("/chroot", this);
                    } catch (InterruptedException | KeeperException e) {
//                    e.printStackTrace();
                    }
                }
            });
        }
    }
}
