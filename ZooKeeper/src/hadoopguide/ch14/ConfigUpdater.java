package hadoopguide.ch14;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

public class ConfigUpdater {
    public static final String PATH = "/config";
    
    private ActiveKeyValueStore store;
    private Random random = new Random();
    
    public ConfigUpdater(String hosts) throws InterruptedException, IOException {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }
    
    public void run() throws KeeperException, InterruptedException {
        while(true) {
            String value = random.nextInt(100)+"";
            store.write(PATH, value);
            System.out.println("Set "+ PATH  + " to "+value);
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }
    
    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        ConfigUpdater updater = new ConfigUpdater(args[0]);
        updater.run();
    }
}
