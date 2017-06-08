package TIPPERSUtils;

/**
 * Created by zhongzhuojian on 22/05/2017.
 */
public class Aggregator {


    public enum AggregateOp{
        NOT_AGGREGATE, MIN, MAX, COUNT, SUM, AVG;
    }

    String tableName;
    AggregateOp aggregateOp;
}
