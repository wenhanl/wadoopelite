package mapr;

import lombok.Data;

/**
 * Created by CGJ on 14-11-13.
 */

@Data
public class MapperTask extends Task {
    private Mapper mapper;
    private String hostname;

    public MapperTask(Mapper mapper, String hostname, int taskID, String input) {
        super(taskID, input);
        this.mapper = mapper;
        this.hostname = hostname;
    }

}
