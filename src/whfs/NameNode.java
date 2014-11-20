package whfs;

import Tracker.JobTracker;
import config.Config;
import file.FileManager;
import msg.Message;
import net.NetObject;
import net.Server;

import java.io.*;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;


/**
 * NameNode of our distributed file system
 * Created by wenhanl on 14-11-3.
 */
public class NameNode extends Thread {

    // Map of registered (alive) data nodes
    private ConcurrentHashMap<String, SocketChannel> dataNodes = null;

    // List of registered data nodes
    private static List<String> dataNodeList = null;

    //IP to hostname
    private static ConcurrentHashMap<String,String> IpToHostname;

    // Map of data node socket to last heartbeat time (milisecond);
    private ConcurrentHashMap<String, Integer> nodeLastHeartbeat = null;

    // Map of DataNode to data blocks
    private ConcurrentHashMap<String, ArrayList<String>> nodeBlocks = null;

    // List of files in WHFS
    private List<String> whfsFiles = null;

    // Map of block to DataNode
    private ConcurrentHashMap<String, ArrayList<String>> blockToNode = null;

    // Blocking queue for inter-thread communication
    private BlockingDeque<String> blockingDeque = null;

    // none-function node
    private static List<String> brokenNode = null;

    public NameNode(BlockingDeque<String> q){
        blockingDeque = q;
        dataNodeList = Collections.synchronizedList(new ArrayList<String>());
        dataNodes = new ConcurrentHashMap<>();
        nodeLastHeartbeat = new ConcurrentHashMap<>();
        nodeBlocks = new ConcurrentHashMap<>();
        blockToNode = new ConcurrentHashMap<>();
        whfsFiles = Collections.synchronizedList(new ArrayList<String>());
        brokenNode = new ArrayList<>();
        IpToHostname = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        Server server = new Server(Config.NAMENODE_PORT);

        // Start a heartbeat daemon
        startHeartbeatDaemon();

        // User command reactor
        userActionDaemon();
        int NodeIndex = 0;
        while (true) {
            NetObject obj = server.listen();

            try {
                switch (obj.type) {
                    case DATA:
                        Message msg = (Message) Message.deserialize(obj.data);
                        handleMsg(msg);
                        break;
                    case CONNECTION:
                        String addr = getHostname(obj.sock.getRemoteAddress().toString());
                        System.out.println("Connection estanblished from " + addr);
                        // Register new DataNode
                        IpToHostname.put(addr,Config.SLAVE_NODES[NodeIndex++]);
                        addDataNode(addr, obj.sock);
                        break;
                    case EXCEPTION:
                        System.out.println("Some slave disconnected");
                        break;
                    default:
                        System.out.println("Type Error");
                }
            } catch (IOException | ClassNotFoundException e){
                System.err.println(e.getMessage());
            }
        }
    }

    /**
     * User action daemon react to user command
     */
    private void userActionDaemon(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    String msg;
                    while ((msg = blockingDeque.poll()) != null) {
                        // process msg
                        userCommandHandler(msg);

                    }
                    // do other stuff
                }
            }
        }).start();
    }

    /**
     * Start a heartbeat daemon as a background daemon
     * Check every 2 seconds, delete data nodes not heartbeat for a configurable time.
     */
    private void startHeartbeatDaemon(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if(dataNodes.size() == 0){
                        continue;
                    }
                    Iterator<String> iterator = dataNodes.keySet().iterator();
                    while (iterator.hasNext()) {
                        String addr = iterator.next();
                        int time = nodeLastHeartbeat.get(addr);
                        time += 2000;
                        if (time >= Config.HEARTBEAT_TIMEOUT){
                            System.out.println("DataNode " + addr + " timeout");
                            brokenNode.add(IpToHostname.get(addr));
                            heartBeatTimeoutAction(addr);
                            JobTracker.rerunMap(brokenNode);
                        }
                        nodeLastHeartbeat.put(addr, time);
                    }
                }
            }
        }).start();
    }

    /**
     * Handle user command
     * @param input
     */
    private void userCommandHandler(String input){
        if(input.isEmpty())
            return;
        String args[] = input.split(" ");
        if(args[0].equals("import")){
            if(args.length != 3){
                return;
            }
            String localPath = args[1];
            String whfsPath = args[2];

            importHandler(localPath, whfsPath);
        } else if (args[0].equals("listfile")){
            for(String file : whfsFiles){
                System.out.println(file);
            }
        } else if (args[0].equals("listnode")){
            for(String node : dataNodeList){
                System.out.println(node);
            }
        } else if (args[0].equals("replica")){
            System.out.println(blockToNode.toString());
        } else if (args[0].equals("nodes")){
            System.out.println(nodeBlocks.toString());
        }
    }

    /**
     * Handler of import command
     * @param localPath localPath to import from
     * @param whfsPath WHFS path to import to
     */
    private void importHandler(String localPath, String whfsPath) {
        // Partition local file into blocks
        String inputPath = Config.LOCAL_BASE_PATH + localPath;
        File local = new File(inputPath);
        if(!local.exists()){
            System.out.println("Local file not exist");
            return;
        }
        String outputPath = Config.LOCAL_BASE_PATH + localPath + "-split-";
        ArrayList<File> splitFiles = new ArrayList<>();

        // Split file from local path into blocks before transfer to DataNodes
        Util.splitFile(inputPath, outputPath);

        // Register whfs file
        whfsFiles.add(whfsPath);

        String blockName;
        String blockPrefix = whfsPath + "_block_";

        // Get splited files
        File baseDir = new File(Config.LOCAL_BASE_PATH);
        File[] fileList = baseDir.listFiles();
        for (File file : fileList) {
            String path = file.getAbsolutePath();
            if (path.contains(outputPath)) {
                splitFiles.add(file);
            }
        }

        // Warm file split has reached maximum split limit (26 * 26 = 676)
        // So result will be inaccurate
        if(splitFiles.size() == 676){
            System.out.println("Warming: Your input file is too large and causing too many splits\n" +
                    " Please increase BLOCK_SIZE in Config to make sure split amount is less than 676 and import again");
            Util.clearFiles(splitFiles);
            return;
        }

        Collections.sort(splitFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        // Divide blocks by nodes
        int numNodes = dataNodeList.size();
        if (numNodes == 0) {
            System.out.println("No available dataNodes");
            return;
        }
        int blockPerNode = splitFiles.size() / numNodes;
        if(blockPerNode == 0) blockPerNode = 1;

        // Send each part of blocks to specific DataNode as averagely as possible
        int offset = 0;
        int nodeIndex = 0;
        int blockIndex = 0;
        ArrayList<String> currBlocks;
        ArrayList<String> currNodes;

        int replica = Math.min(Config.NUM_WHFS_REPLICA, numNodes);  // Replica number can't be greater than number of nodes

        for (File file : splitFiles) {
            String hostname = Config.SLAVE_NODES[nodeIndex];


            // Get block name
            String indexStr = blockIndex < 10 ? "0" + String.valueOf(blockIndex) : String.valueOf(blockIndex);
            blockName = blockPrefix + indexStr;


            // Add header to file (hostname and block name)
            String header = hostname + "\t" + blockName + "\n";
            FileManager.addHeader(file, header);


            if (blockToNode.containsKey(blockName)) {
                currNodes = blockToNode.get(blockName);
            } else {
                currNodes = new ArrayList<>();
            }

            // Transfer replicas to remote hosts
            for(int i = 0;i < replica; i++) {
                String replicaHost = dataNodeList.get((nodeIndex + i) % numNodes);
                FileManager.transferFile(file, replicaHost, Config.DATANODE_FILE_PORT);

                // Register node to block
                if (!nodeBlocks.containsKey(replicaHost)) {
                    currBlocks = new ArrayList<>();
                } else {
                    currBlocks = nodeBlocks.get(replicaHost);
                }
                currBlocks.add(blockName);
                nodeBlocks.put(replicaHost, currBlocks);

                // Register block to node map
                currNodes.add(replicaHost);
                blockToNode.put(blockName, currNodes);
            }

            offset++;
            blockIndex++;

            // move to next node
            if (offset == blockPerNode && blockIndex <= (numNodes - 1) * blockPerNode) {
                offset = 0;
                nodeIndex++;
            }

        }
        // Clean up
        Util.clearFiles(splitFiles);

        // Print finish message
        System.out.println("Import finished.");
    }

    /**
     * Add dataNode to both maps to keep consistency
     * @param key
     * @param sock
     */
    private void addDataNode(String key, SocketChannel sock){
        dataNodeList.add(key);
        dataNodes.put(key, sock);
        nodeLastHeartbeat.put(key, 0);
    }

    /**
     * Delete dataNode from both maps to keep consistency
     * @param addr
     */
    private void deleteDataNode(String addr){
        dataNodeList.remove(addr);
        nodeLastHeartbeat.remove(addr);
        dataNodes.remove(addr);
    }

    private void handleMsg(Message msg){
        switch(msg.getType()){
            case HEARTBEAT:
                // Reset wait time to zero
                nodeLastHeartbeat.put(getHostname(msg.getAddr().toString()), 0);
                break;
        }
    }

    /**
     * Action taken when heartbeat timeout from a host address
     * @param addr
     */
    private synchronized void heartBeatTimeoutAction(String addr) {
        // Remove from node list and heartbeat list
        deleteDataNode(addr);
        ArrayList<String> blockList = nodeBlocks.get(addr);

        for(String block : blockList){
            ArrayList<String> nodelist = blockToNode.get(block);

            // Remove addr from this block's blockToNode map
            nodelist.remove(addr);
            blockToNode.put(block, nodelist);
        }

        // Remove from node to block list
        nodeBlocks.remove(addr);
    }

    public static List<String> getBrokenNode() {
        return brokenNode;
    }

    private String getHostname(String addr){
        return addr.split("/")[1].split(":")[0];
    }
}
