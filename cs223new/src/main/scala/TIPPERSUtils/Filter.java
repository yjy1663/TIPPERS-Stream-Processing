package TIPPERSUtils;

/**
 * Created by zhongzhuojian on 22/05/2017.
 */
public class Filter {

    public enum Comp {
        EQUAL, LESS, GREATER, LESS_EQUAL, GREATER_EQUAL, NOT_EQUAL;
    }

    public class Condition{
        String leftAttr;
        Comp op;
        boolean isRightAttr;
        String rightAttr;
        Object value;


    }

    String tableName;
    Condition condition;
    public Filter(){

        condition = new Condition();
    }
}
