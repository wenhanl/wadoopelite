package mapr;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by CGJ on 14-11-13.
 */


public class Task implements Serializable {
    protected int taskID;
    protected String input;
    public boolean running;
    public boolean done;

    public Task(int taskID, String input) {
        this.taskID = taskID;
        this.input = input;
        running = false;
        done = false;
    }
    public int getTaskID(){return taskID;}
    public String getInput(){return input;}
    public void setTaskID(int taskID){this.taskID = taskID;}
    public void setInput(String input){this.input = input;}

}
