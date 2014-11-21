package mapr;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by CGJ on 14-11-13.
 */

@Getter
@Setter
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

}
