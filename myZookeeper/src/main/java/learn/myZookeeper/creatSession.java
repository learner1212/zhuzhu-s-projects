package learn.myZookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class creatSession implements Watcher {
    private static int eventCount = 0;
    private static boolean isdelete = true;
    private static ZooKeeper zooKeeper;
    private static Logger logger = LoggerFactory.getLogger(creatSession.class);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // TODO Auto-generated method stub
        myWatcher my_watcher = new myWatcher();
        zooKeeper = new ZooKeeper("192.168.159.128:2181", 5000, new creatSession());
        for (int i = 1; i <= 10; i++) {
            String path = "/note-" + i;
            if (zooKeeper.exists(path, false) != null) {
                isdelete = true;
                zooKeeper.delete(path, -1);
                System.out.println("delete  " + path);
            }
        }
        for (int i = 1; i <= 10; i++) {
            String path = "/note-" + i;
            isdelete = false;
            zooKeeper.create(path, "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("create  " + path);
        }

        Thread.sleep(Integer.MAX_VALUE);
        // zooKeeper.getChildren("/", true);
    }

    @Override
    public void process(WatchedEvent event) {
        // if (isdelete) {
        // try {
        // zooKeeper.exists("/", true);
        // } catch (KeeperException | InterruptedException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // return;
        // }
        eventCount++;
        System.out.println("�յ��¼���" + event);
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
        // System.out.println(event.getState() + ":" + event.getType() + ":" +
        // event.getPath());
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
                        System.out.println("event_" + eventCount + "_sleep_" + i);
                        Thread.sleep(1000);
                    }
                    // if (!children.isEmpty()) {
                    // String path = event.getPath() +
                    // children.get(children.size() - 1);
                    // System.out.println("delete " + path);
                    // zooKeeper.delete(path, -1);
                    // }
                } catch (KeeperException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

}
