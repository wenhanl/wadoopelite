package mapr;

/**
 * Created by CGJ on 14-11-13.
 */

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

@Data
public class ReducerTask extends Task {
    private int partitionNum;
    private Reducer reducer;
    //the mapper task which the reducer task is dependent
    private HashMap<Integer, Boolean> mapperTaskStatus;

    public ReducerTask(Reducer reducer, int partitionNum, List<Integer> mapperTaskIds, int taskid, String input) {
        super(taskid, input);
        this.reducer = reducer;
        this.partitionNum = partitionNum;
        mapperTaskStatus = new HashMap<>();
        for (int mapperTaskId : mapperTaskIds) {
            mapperTaskStatus.put(mapperTaskId, false);
        }
    }

    public Set<Integer> getDependentMapperJobIds() {
        return mapperTaskStatus.keySet();
    }
    /**
     * when a mapper task complete, this method will update the dependent mapper task of the reducer task
     *
     * @param taskId  Id of the mapper job whose status you wish to change
     * @param isDone current running status of that mapper, true: that mapper done, false: mapper is still running
     */
    public void setMapperTaskStatus(int taskId, boolean isDone) {
        if (mapperTaskStatus.containsKey(taskId)) {
            mapperTaskStatus.put(taskId, isDone);
        }
    }
    /**
     * When a mapper task complete, check if all the dependent mapper task is complete which means this reducer task is ready
     *
     * @return boolean indicating whether all the mappers that the reducer depends on are completed
     */
    public boolean allMappersAreReady() {
        boolean mappersReady = true;
        for (int jobID : getDependentMapperJobIds()) {
            mappersReady &= mapperTaskStatus.get(jobID);
        }
        return mappersReady;
    }

}
