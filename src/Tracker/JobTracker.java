package Tracker;

import config.Config;
import file.FileManager;
import mapr.*;
import msg.MPMessage;
import msg.MPMessageManager;
import msg.TaskUpdateMessage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JobTracker for mapreduce framework
 *
 * Created by CGJ on 14-11-12.
 */
public class JobTracker extends Thread {
    private ServerSocket masterServer;
    private BlockingDeque<String> blockingDeque = null;
    private static int curTaskId;
    private static Map<String,List<Integer>> relatedReducers;
    private static MPCoordinator coordinator;
    private static List<MapReduceJob> jobDump;

    public JobTracker(BlockingDeque<String> q) throws IOException {
        blockingDeque = q;
        curTaskId = 0;
        relatedReducers = new ConcurrentHashMap<>();
        masterServer = new ServerSocket(Config.DATA_PORT);
        coordinator = new MPCoordinator();
        jobDump = new ArrayList<>();
        // Create result store folder in case it's not created
        FileManager.createDir(Config.MAP_RESULTS_FOLDER);
    }

    /**
     * Receive user command
     */
    private void userActionDaemon() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    String msg;
                    while ((msg = blockingDeque.poll()) != null) {
                        // process msg
                        userCommandHandler(msg);
                    }
                }
            }
        }).start();
    }

    /**
     * Take action on user command
     * @param input
     */
    private void userCommandHandler(String input) {
        if (input.isEmpty())
            return;
        String args[] = input.split(" ");
        if (args[0].equals("mprun")) {
            if(args.length != 3){
                System.out.println("Wrong command format.");
                return;
            }

            // Run a user specified mapreduce job
            MapReduceJob newJob = null;
            try {
                String className = "example." + args[1];
                newJob = (MapReduceJob) Class.forName(className).newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            newJob.setInputFile(args[2]);
            jobDump.add(newJob);
            newMPJob(newJob);
        }

    }

    /**
     * Rerun mapreduce job after node failure during running
     * @param brokenNode
     */
    public static void rerunMap(List<String> brokenNode) {
        while(true){
            if(MPCoordinator.getTaskMap().isEmpty())
                break;
        }
        curTaskId = 0;
        for(MapReduceJob newJob : jobDump){
            List<Task> allTasks = new ArrayList<>();
            List<Integer> mapperTaskId = new ArrayList<>();
            List<Integer> reducerTaskId = new ArrayList<>();

            for (int slaveid=0;slaveid < Config.SLAVE_NODES.length;slaveid++) {
                if( brokenNode.contains(Config.SLAVE_NODES[slaveid]))
                    continue;
                String hostname = Config.SLAVE_NODES[slaveid];
                allTasks.add(new MapperTask(newJob.getMapper(), hostname, curTaskId, newJob.getInputFile()));
                mapperTaskId.add(curTaskId++);
            }

            for (int i = 0; i < Config.SLAVE_NODES.length - jobDump.size(); i++) {
                ReducerTask reducetask = new ReducerTask(newJob.getReducer(), i, mapperTaskId, curTaskId, newJob.getInputFile());
                allTasks.add(reducetask);
                reducerTaskId.add(curTaskId++);
            }
            relatedReducers.put(newJob.getInputFile(), reducerTaskId);
            coordinator.scheduleMapperTasks(allTasks);
        }

    }

    private void handleConnection(MPMessageManager MsgManager) throws IOException {
        MPMessage msgIn = MsgManager.receiveMessage();

        if (msgIn.getType() == MPMessage.MessageType.UPDATE) {
            coordinator.processTaskUpdateMessage((TaskUpdateMessage) msgIn);
        }
    }

    public void newMPJob(MapReduceJob newJob) {
        List<Task> allTasks = new ArrayList<>();
        List<Integer> mapperTaskId = new ArrayList<>();
        List<Integer> reducerTaskId = new ArrayList<>();

        for (int slaveid=0;slaveid < Config.SLAVE_NODES.length;slaveid++) {
            String hostname = Config.SLAVE_NODES[slaveid];
            allTasks.add(new MapperTask(newJob.getMapper(), hostname, curTaskId, newJob.getInputFile()));
            mapperTaskId.add(curTaskId++);
        }

        for (int i = 0; i < Config.SLAVE_NODES.length; i++) {
            ReducerTask reducetask = new ReducerTask(newJob.getReducer(), i, mapperTaskId, curTaskId, newJob.getInputFile());
            allTasks.add(reducetask);
            reducerTaskId.add(curTaskId++);
        }
        relatedReducers.put(newJob.getInputFile(), reducerTaskId);
        coordinator.scheduleMapperTasks(allTasks);
    }

    /**
     * Verify all reduce tasks are done from one reducer task
     * @param task
     */
    public static void verifyAllReducerTaskDone(ReducerTask task) {
        List<Integer> relatedReducerList = relatedReducers.get(task.getInput());
        for (int j = 0; j < relatedReducerList.size(); j++) {
            int reducerJobId = relatedReducerList.get(j);
            if (reducerJobId == task.getTaskID()) {
                relatedReducerList.remove(j);
            }
        }
        if (relatedReducerList.size() == 0) {
            relatedReducers.remove(task.getInput());
        }

        System.out.println("Task " + task.getInput() + " is done! Please check the output file!");
    }

    public void run() {
        userActionDaemon();
        while (true) {
            try {
                final Socket sock = masterServer.accept();
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            handleConnection(new MPMessageManager(sock));
                        } catch (IOException e) {
                            System.out.println("JobTracker cannot process incoming message!");
                            e.printStackTrace();
                        }
                    }
                }).start();
            } catch (IOException e) {
                System.out.println("JobTracker cannot process incoming socket!");
            }
        }
    }
}