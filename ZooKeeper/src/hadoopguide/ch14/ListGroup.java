package hadoopguide.ch14;

import java.util.List;

import org.apache.zookeeper.KeeperException;

public class ListGroup extends ConnectionWatcher{

    public void list(String groupName) throws KeeperException, InterruptedException {
        String path = "/"+ groupName;
        
        try {
            List<String> children = zk.getChildren(path, false);
            
            if(children.isEmpty()) {
                System.out.println("No members in group " + groupName);
                System.exit(1);
            }else {
                for (String child : children) {
                    System.out.println(child);
                }
            }
        }catch(KeeperException.NoNodeException e) {
            System.out.println("Group " + groupName + " does not exist");
            System.exit(1);
        }
    }
    
    public static void main(String[] args) throws Exception{
        ListGroup listGroup = new ListGroup();
        listGroup.connect(args[0]);
        listGroup.list(args[1]);
        listGroup.close();

    }

}
