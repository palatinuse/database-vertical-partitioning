package db.schema.entity;

import java.util.Map;

/**
 * A query projecting some columns, having a single range filter condition, and optionally selections as well.
 * It contains information only relevant to scanning and filtering a single table.
 * Database queries touching multiple tables have to be represented by a separate Query object for each table being touched.
 *
 * @author Endre Palatinus
 *
 */
public class RangeQuery extends Query {

    protected Range rangeCondition;

    public RangeQuery(String name, int weight, Range rangeCondition) {
        super(name, weight);
        this.rangeCondition = rangeCondition;
    }

    public RangeQuery(Query query, Range rangeCondition) {
        super(query);
        this.rangeCondition = rangeCondition;
    }

    public Range getRangeCondition(){
        return this.rangeCondition;
    }

    public void setRangeCondition(Range range){
        this.rangeCondition = range;
    }
}
