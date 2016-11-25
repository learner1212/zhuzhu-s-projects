package master;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;

public class WorkServer {

    private volatile boolean running;

    private ZkClient zkClient;

    private static final String MASTER_PATH = "/master";

    private IZkDataListener dataListener;

    private RunningData serverData;

    private RunningData masterData;

    private ScheduledExecutorService delayExecutor = Executors.newScheduledThreadPool(1);

    private int delayTime = 5;

    public WorkServer(RunningData rd) {
        this.serverData = rd;
        this.dataListener = new IZkDataListener() {

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                // TODO Auto-generated method stub
                if (masterData != null && masterData.getName().equals(serverData.getName())) {
                    takeMaster();
                } else {
                    delayExecutor.schedule(new Runnable() {
                        @Override
                        public void run() {
                            takeMaster();
                        }
                    }, delayTime, TimeUnit.SECONDS);
                }
                takeMaster();
            }

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                // TODO Auto-generated method stub

            }
        };
    }

    public void start() throws Exception {
        if (running) {
            throw new Exception("server has startup...");
        }
        running = true;
        zkClient.subscribeDataChanges(MASTER_PATH, dataListener);
        takeMaster();
    }

    public void stop() throws Exception {
        if (!running) {
            throw new Exception("server has stoped...");
        }
        running = false;
        zkClient.unsubscribeDataChanges(MASTER_PATH, dataListener);
        releaseMaster();
    }

    private void takeMaster() {
        if (!running) {
            return;
        }
        try {
            zkClient.create(MASTER_PATH, serverData, CreateMode.EPHEMERAL);
            masterData = serverData;
            delayExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    if (checkMaster()) {
                        releaseMaster();
                    }
                }
            }, 5, TimeUnit.SECONDS);

        } catch (ZkNodeExistsException e) {
            RunningData runningData = zkClient.readData(MASTER_PATH, true);
            if (runningData == null) {
                takeMaster();
            } else {
                masterData = runningData;
            }
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    private void releaseMaster() {
        if (checkMaster()) {
            zkClient.delete(MASTER_PATH);
        }
    }

    private boolean checkMaster() {
        try {
            RunningData runningData = zkClient.readData(MASTER_PATH, true);
            masterData = runningData;
            if (masterData.getName().equals(serverData.getName())) {
                return true;
            }
        } catch (ZkNoNodeException e) {
            return false;
        } catch (ZkInterruptedException e) {
            return checkMaster();
        } catch (ZkException e) {
            return false;
        }
        return false;
    }
}
