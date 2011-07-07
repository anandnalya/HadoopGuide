package hadoopguide.ch14;

import java.util.List;

import org.apache.zookeeper.KeeperException;

public class DeleteGroup extends ConnectionWatcher {

    public void deleteGroup(String groupName) throws InterruptedException, KeeperException {
        String path = "/" + groupName;
        
        try {
            List<String> children = zk.getChildren(path, false);
            if(!children.isEmpty()) {
                for (String child : children) {
                    deleteGroup(groupName + "/" + child);
                }
            }
            zk.delete(path, -1);
            System.out.println("deleted " + path);
        }catch (KeeperException.NoNodeException e) {
            System.out.println("Group does not exist: " + path);
            System.exit(1);
        }
    }
    
    public static void main(String[] args) throws Exception{
        DeleteGroup deleteGroup = new DeleteGroup();
        deleteGroup.connect(args[0]);
        deleteGroup.deleteGroup(args[1]);
        deleteGroup.close();

    }

}
