package learn.myZookeeper;

import java.io.IOException;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.KeeperException;

public class creatSessionByZkClient {
    private static int eventCount = 0;
    private static boolean isdelete = true;
    private static ZkClient zkClient;
    // private static Logger logger =
    // LoggerFactory.getLogger(creatSessionByZkClient.class);

    private static class zkNoteListener implements IZkDataListener {

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            // TODO Auto-generated method stub
            System.out.println("DataChange");
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            // TODO Auto-generated method stub
            System.out.println("DataDeleted");
        }
    }

    private static class ZkChildListener implements IZkChildListener {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            // TODO Auto-generated method stub
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
            System.out.println("�յ��¼���" + eventCount);
            System.out.println(currentChilds.toString());
            for (int i = 0; i < 10; i++) {
                System.out.println("event_" + eventCount + "_sleep_" + i);
                Thread.sleep(1000);
            }
            for (int i = 10; i > 0; i--) {
                String path = "/note-" + i;

            }
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // TODO Auto-generated method stub
        // myWatcher my_watcher = new myWatcher();
        zkClient = new ZkClient("192.168.159.128:2181", 5000, 5000, new SerializableSerializer());
        zkClient.subscribeDataChanges("/fhjahf", new zkNoteListener());
        // gsfgsfd
        // zooKeeper = new ZooKeeper("192.168.159.128:2181", 5000, new
        // creatSessionByZkClient());
        // for (int i = 1; i <= 10; i++) {
        // String path = "/note-" + i;
        // isdelete = true;
        // zkClient.delete(path);
        // System.out.println("delete " + path);
        // }
        // zkClient.subscribeChildChanges("/", new ZkChildListener());
        // for (int i = 1; i <= 10; i++) {
        // String path = "/note-" + i;
        // isdelete = false;
        // zkClient.create(path, "123".getBytes(), CreateMode.PERSISTENT);
        // Thread.sleep(1000);
        // System.out.println("create " + path);
        // }
        // List<String> list = new ArrayList<>();
        // list.add("test7");
        // list.add("test8");
        // zkClient.writeData("/note-7", list);
        // List<String> list1 = zkClient.readData("/note-7");
        // System.out.println(list1.toString());
        // // zooKeeper = new ZooKeeper("132.122.1.168:2181", 5000, new
        // // creatSession());
        // // my_watcher.setZooKeeper(zooKeeper);
        Thread.sleep(Integer.MAX_VALUE);
        // zooKeeper.getChildren("/", true);
    }

}