package mapr;

/**
 * Created by CGJ on 14-11-12.
 */
import lombok.Data;

import java.io.Serializable;

@Data
public abstract class MapReduceJob<Kin, Vin, Kout, Vout> implements Serializable {

    private String inputFile;

    private String id;

    abstract public Mapper<Kin, Vin, Kout, Vout> getMapper();

    abstract public Reducer<Kout, Vout, Kout, Vout> getReducer();
}