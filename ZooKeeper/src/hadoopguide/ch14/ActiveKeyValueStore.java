package hadoopguide.ch14;

import java.nio.charset.Charset;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ActiveKeyValueStore extends ConnectionWatcher {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final int MAX_RETRY = 5;
    private static final int RETRY_INTERVAL = 5000;

    public void write(String path, String value) throws KeeperException, InterruptedException {

        for (int i = 1; i <= MAX_RETRY; i++) {
            try {
                Stat stat = zk.exists(path, false);
                if (stat == null) {
                    zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }

                zk.setData(path, value.getBytes(CHARSET), -1);

                break;
            } catch (KeeperException.SessionExpiredException e) {
                throw e;
            } catch (KeeperException e) {
                if (i == MAX_RETRY) {
                    throw e;
                }
                Thread.sleep(RETRY_INTERVAL);
            }
        }

    }

    public String read(String path, Watcher watcher) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            return null;
        }

        byte[] data = zk.getData(path, watcher, null);
        return new String(data, CHARSET);
    }
}
