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
    // Map from fileName to block list
    private HashMap<String, ArrayList<Integer>> fileBlock = null;

    //file in which nodes --  CGJ
    private static HashMap<String, ArrayList<Integer>> fileNodes = null;

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
        fileBlock = new HashMap<>();
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
                        String addr = obj.sock.getRemoteAddress().toString();
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
        // Register whfs file
        whfsFiles.add(whfsPath);

        // Partition local file into blocks
        String inputPath = Config.LOCAL_BASE_PATH + localPath;
        String outputPath = Config.LOCAL_BASE_PATH + localPath + "-split-";
        ArrayList<File> splitFiles = new ArrayList<>();

        // Split file from local path into blocks before transfer to DataNodes
        Util.splitFile(inputPath, outputPath);

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
        ArrayList<Integer> fileBlockNode = new ArrayList<>();
        fileNodes = new HashMap<>();
        ArrayList<String> currBlocks;
        ArrayList<String> currNodes;

        int replica = Math.min(Config.NUM_WHFS_REPLICA, numNodes);  // Replica number can't be greater than number of nodes
        for(int rep = 0; rep < 1; rep++) {
            offset = 0;
            nodeIndex = rep;
            blockIndex = 0;
            for (File file : splitFiles) {
                int ni = nodeIndex % numNodes;
                if(rep == 0)
                    fileBlockNode.add(nodeIndex);
                String hostname = Config.SLAVE_NODES[ni];
                String ipaddr = dataNodeList.get(ni);
//                System.out.println(ipaddr + "ip");


                // Register block to node
                String indexStr = blockIndex < 10 ? "0" + String.valueOf(blockIndex) : String.valueOf(blockIndex);
                blockName = blockPrefix + indexStr;

                if (blockToNode.containsKey(blockName)) {
                    currNodes = blockToNode.get(blockName);
                } else {
                    currNodes = new ArrayList<>();
                }
                currNodes.add(ipaddr);
                blockToNode.put(blockName, currNodes);

                // Add header to file (hostname and block name)
                if (rep == 0) {
                    String header = hostname + "\t" + blockName + "\n";
                    FileManager.addHeader(file, header);
                }

                // Transfer file to remote DataNode
                FileManager.transferFile(file, hostname, Config.DATANODE_FILE_PORT);

                //replica
                for(int i=1;i<Config.NUM_WHFS_REPLICA;i++)
                    FileManager.transferFile(file, addHostname(hostname), Config.DATANODE_FILE_PORT);

                // Register node to block
                if (!nodeBlocks.containsKey(ipaddr)) {
                    currBlocks = new ArrayList<>();
                } else {
                    currBlocks = nodeBlocks.get(ipaddr);
                }

                currBlocks.add(blockName);
                nodeBlocks.put(ipaddr, currBlocks);

                offset++;
                blockIndex++;

                // move to next node
                if (offset == blockPerNode && blockIndex <= (numNodes - 1) * blockPerNode) {
                    offset = 0;
                    nodeIndex++;
                    if(rep == 0)
                        fileBlockNode.add(nodeIndex);
                }
            }
        }
        // Clean up
        Util.clearFiles(splitFiles);
        fileNodes.put(whfsPath,fileBlockNode);
    }

    public String addHostname(String hostname){
        int res = 0;
        for(int i = 0;i<Config.SLAVE_NODES.length;i++)
            if(Config.SLAVE_NODES[i] == hostname)
                res = i;
        if(res == Config.SLAVE_NODES.length-1)
            return Config.SLAVE_NODES[0];
        else
            return Config.SLAVE_NODES[res];
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
//        System.out.println(dataNodeList.get(0));
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
                nodeLastHeartbeat.put(msg.getAddr().toString(), 0);
                break;
        }
    }

    //<<<<<<< HEAD
    public static HashMap<String, ArrayList<Integer>> getfileNodes(){
        return fileNodes;
    }

    public static List<String> getdataNodeList() {
        return dataNodeList;
    }
    //=======
    private synchronized void heartBeatTimeoutAction(String addr) {
        // Remove from node list and heartbeat list
//        System.out.println(addr + "timetime");
        deleteDataNode(addr);
        String hostname = addr;
        ArrayList<String> blockList = nodeBlocks.get(hostname);

        for(String block : blockList){
            ArrayList<String> nodelist = blockToNode.get(block);

            // Remove addr from this block's blockToNode map
            nodelist.remove(hostname);
            blockToNode.put(block, nodelist);

            // When there is more than 1 node have this block, move it to node without this block
            if(nodelist.size() > 0 && dataNodeList.size() > 1) {
                String from = nodelist.get(0);
                String to = null;
                for(String node : dataNodeList){
                    String host = getHostname(node);
                    if(!nodelist.contains(host)){
                        to = host;
                    }
                }
            }
        }

        // Remove from node to block list
        nodeBlocks.remove(hostname);
    }

    private void blockFromTo(String from, String to){

    }

    public static List<String> getBrokenNode() {
        return brokenNode;
    }

    private String getHostname(String addr){
        return addr.split("/")[1].split(":")[0];
//>>>>>>> 812321829daa50393288db0dd84ad75964ba09d6
    }
}
