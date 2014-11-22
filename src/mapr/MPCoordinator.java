package mapr;

import Tracker.JobTracker;
import config.Config;
import msg.MPMessageManager;
import msg.MPTaskMessage;
import msg.TaskUpdateMessage;
import whfs.NameNode;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
/**
 * Created by CGJ on 14-11-13.
 */
public class MPCoordinator {

    private static ConcurrentHashMap<Integer, Task> taskMap;
    private Integer reducerCounter;

    public MPCoordinator() throws IOException {
        this.taskMap = new ConcurrentHashMap<>();
        reducerCounter = 0;

        // Create a empty log file
        PrintWriter writer = new PrintWriter("../Log/log");
        writer.print("");
        writer.close();


    }

    /**
     * Schedule mapper tasks
     * @param tasks
     */
    public void scheduleMapperTasks(List<Task> tasks) {
        for (Task task : tasks) {
            //System.out.println("Task received for scheduling:\n  " + task);
            taskMap.put(task.getTaskID(), task);
            if (task instanceof MapperTask) {
                try {
                    MPMessageManager slaveComm = new MPMessageManager(((MapperTask) task).getHostname(), Config.TASK_PORT);
                    slaveComm.sendMessage(new MPTaskMessage(task));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                task.running = true;
            }
        }
        writeToLog();
    }


    /**
     * Schedule reducer tasks
     * @param reducerTask
     * @param brokenNodes
     */
    public void scheduleReducerTasks(ReducerTask reducerTask, List<String> brokenNodes) {
        int partitionNum = reducerCounter % (Config.SLAVE_NODES.length- brokenNodes.size());

        reducerCounter++;
        reducerTask.setPartitionNum(partitionNum);
        // send tasks to slave to begin processing
        try {
            String hostname = Config.SLAVE_NODES[partitionNum];
            while(NameNode.getBrokenNode().contains(Config.SLAVE_NODES[partitionNum]))
                hostname = Config.SLAVE_NODES[(++partitionNum)%Config.SLAVE_NODES.length];
            MPMessageManager slaveComm = new MPMessageManager(hostname, Config.TASK_PORT);
            slaveComm.sendMessage(new MPTaskMessage(reducerTask));
            // set running in each task processed to one
            reducerTask.running = true;
            //System.out.println("Reducer task" + partitionNum + " launch!");
            writeToLog();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Process task update message
     * @param msg
     */
    public void processTaskUpdateMessage(TaskUpdateMessage msg) {
	if(taskMap.isEmpty())return;	
        final Task targetTask = taskMap.get(msg.getTaskID());
	if(targetTask == null)return;
        targetTask.running = msg.isRunning();
        targetTask.done = msg.isDone();
        taskMap.put(targetTask.getTaskID(),targetTask);

        // Write to log for monitor
        writeToLog();

        if (targetTask instanceof MapperTask) {
            // changes waiting status of reduce jobs that have the map job as it's dependant
            for (Task task : taskMap.values()) {
                if (task instanceof ReducerTask) {
                    ReducerTask reducerTask = (ReducerTask) task;
                    reducerTask.setMapperTaskStatus(msg.getTaskID(), msg.isDone());
                    if (reducerTask.allMappersAreReady()) {
                        //schedule reducer task on a slave
                        System.out.println("All Mappers detected as ready for reducer task with TaskID " + reducerTask.getTaskID() + ", initiating Reducer");
/*                        try {
                            Thread.sleep(15000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }*/
		        scheduleReducerTasks(reducerTask, NameNode.getBrokenNode());
                    }
                }
            }
        } else if (targetTask instanceof ReducerTask) {

            if (msg.isDone()) {
                // Remove dependant maps
                for (int taskID : ((ReducerTask) targetTask).getDependentMapperJobIds()) {
                    taskMap.remove(taskID);
                }
                // Remove reduce job
                taskMap.remove(msg.getTaskID());
                // Store results in user-defined output file
                if (msg.getPayload() instanceof List) {
                    writeOutputReduceRecords((ReducerTask) targetTask, (List<Record>) msg.getPayload());
                }

                JobTracker.verifyAllReducerTaskDone((ReducerTask) targetTask);
            }
        }
    }


    /**
     * Write final result to Master local from all reducers.
     * @param task
     * @param finalReducerResults
     */
    public void writeOutputReduceRecords(ReducerTask task, List<Record> finalReducerResults) {
        // Append to output file
        String MPFinalOutputFile = Config.MAP_RESULTS_FOLDER + "MP_Result_" + task.getInput();
        try {
            FileWriter fw = new FileWriter(MPFinalOutputFile, true);
            for (Record record : finalReducerResults) {
                fw.append(record.getKey() + "\t" + record.getValue() + "\n");
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Wrote " + finalReducerResults.size() + " result records to " + MPFinalOutputFile + "!");
    }

    public static Map<Integer, Task> getTaskMap(){
        return taskMap;
    }

    private void writeToLog() {
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter("../Log/log", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String status;
        String tasknode;
        String tasktype;
        if(taskMap.size() == 0){
            try {
                fileWriter.append("All done!");
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }
        for (int i : taskMap.keySet()) {
            Task cur = taskMap.get(i);
            if(cur == null) continue;
            if (cur.running) {
                status = "running";
            }
            else if (cur.done){
                status = "done";
            }
            else{
                status = "not begin";
            }
            if (cur instanceof MapperTask) {
                tasktype = "map";
                tasknode = ((MapperTask) cur).getHostname();
            } else {
                tasktype = "reduce";
                tasknode = Config.SLAVE_NODES[((ReducerTask) cur).getPartitionNum()];
            }
            String s = tasktype + "  " + "TaskId: " + i + " is " + status + " in Node: " + tasknode;
            try {
                fileWriter.append(s + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fileWriter.append("----------------------------------------------\n");
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
