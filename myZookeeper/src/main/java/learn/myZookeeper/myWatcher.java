package learn.myZookeeper;


import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class myWatcher implements Watcher {

    private int eventCount = 0;
    private ZooKeeper zooKeeper;

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        eventCount++;
        System.out.println("�յ��¼���" + event);
        try {
            zooKeeper.exists("/", this);
        } catch (KeeperException | InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        // try {
        // zooKeeper.getChildren("/", true);
        // } catch (KeeperException e1) {
        // // TODO Auto-generated catch block
        // System.out.println(e1.getMessage());
        // e1.printStackTrace();
        // } catch (InterruptedException e1) {
        // // TODO Auto-generated catch block
        // System.out.println(e1.getMessage());
        // e1.printStackTrace();
        // }
        // TODO Auto-generated method stub
        System.out.println(event.getState() + ":" + event.getType() + ":" + event.getPath());
        if (KeeperState.SyncConnected == event.getState()) {
            System.out.println(event.getState() + ":" + event.getType() + ":" + event.getPath());
            if (EventType.None == event.getType() && null == event.getPath()) {
                // connectedSemaphore.countDown();
                System.out.println("connect success");
                try {
                    List<String> children = zooKeeper.getChildren("/", true);
                } catch (KeeperException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else if (event.getType() == EventType.NodeChildrenChanged) {
                try {
                    List<String> children = zooKeeper.getChildren("/", true);
                    for (String str : children) {
                        System.out.println(str);
                    }
                    for (int i = 0; i < 10; i++) {
                        System.out.println("event_" + eventCount + "sleep_" + i);
                        Thread.sleep(1000);
                    }
                    if (!children.isEmpty()) {
                        String path = event.getPath() + children.get(children.size() - 1);
                        System.out.println("delete " + path);
                        zooKeeper.delete(path, -1);
                    }
                } catch (KeeperException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

}
