package Tracker;

import config.Config;
import file.FileManager;
import mapr.*;
import msg.MPMessage;
import msg.MPMessageManager;
import msg.MPPartitionMessage;
import msg.MPTaskMessage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static msg.MPMessage.MessageType.PARTITION;
import static msg.MPMessage.MessageType.TASK;

/**
 * Task tracker in slave node, in response to scheduling from JobTracker
 * Created by CGJ on 14-11-12.
 */
public class TaskTracker extends Thread{

    private ServerSocket slaveServer;

    public TaskTracker(int port) throws IOException {
        slaveServer = new ServerSocket(port);
	    FileManager.createDir(Config.MAP_RESULTS_FOLDER);
    }

    /**
     * Handle message from JT and other TT
     * @param MsgManager
     * @throws IOException
     */
    private void handleConnection(MPMessageManager MsgManager) throws IOException {
        MPMessage msgIn = MsgManager.receiveMessage();
        if (msgIn.getType() == TASK) {
            runMapReduceTask((MPTaskMessage) msgIn, MsgManager);
        }
        else if(msgIn.getType() == PARTITION) {
            MPPartitionMessage partMsg = (MPPartitionMessage) msgIn;
            List<Record<String, String>> partRecords = getPartitionedRecords(partMsg.getReducerNum(), partMsg.getReducertask());
            MsgManager.sendMessage(new MPPartitionMessage(partMsg.getReducerNum(),partRecords, null));
        }
    }


    /**
     * Run mapreduce task
     * @param taskMsg
     * @param comm
     */
    private void runMapReduceTask(MPTaskMessage taskMsg, MPMessageManager comm) {
        Task task = taskMsg.getTask();
        if (task instanceof MapperTask) {
            MapperTask mapperTask = (MapperTask) task;
            new MapTaskProcessor(mapperTask, comm).start();
        }
        else if (task instanceof ReducerTask) {
            ReducerTask reducerTask = (ReducerTask) task;
            new ReduceTaskProcessor(reducerTask, comm).start();
        }
    }


    /**
     *
     * @param partitionNum
     * @param task
     * @return
     * @throws IOException
     */
    public List<Record<String, String>> getPartitionedRecords(int partitionNum, ReducerTask task) throws IOException{
        List<Record<String, String>> partitionedRecords = new ArrayList<>();
        String resultFileName = Config.MAP_RESULTS_FOLDER + "MapTempFile_" + task.getInput();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(resultFileName));
        } catch (FileNotFoundException e) {
            return null;
        }
        String line;
        while ((line = br.readLine()) != null) {
            int lastTab = line.lastIndexOf("\t");
            String key = line.substring(0, lastTab);
            String value = line.substring(lastTab + 1);
            int partition = Math.abs(key.hashCode() % Config.NUM_REDUCERS);
            if (partition == partitionNum) {
                partitionedRecords.add(new Record<>(key, value));
            }
        }
        br.close();
        return partitionedRecords;
    }


    public void run() {
        while (true) {
            try {
                final Socket sock = slaveServer.accept();
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            handleConnection(new MPMessageManager(sock));
                        } catch (IOException e) {
                            System.err.println("Error: Did not handle request from incoming msg properly (" +
                                    e.getMessage() + ").");
                            e.printStackTrace();
                        }
                    }
                }).start();
            } catch (IOException e) {
                System.err.println("Error: oops, an error in the SlaveNode thread! (" + e.getMessage() + ").");
            }
        }
    }
}
