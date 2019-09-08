package simpledb;

public class QueryPlans {

	public QueryPlans(){
	}

	//SELECT * FROM T1, T2 WHERE T1.column0 = T2.column0;
	public Operator queryOne(DbIterator t1, DbIterator t2) {
        JoinPredicate predicate = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joined = new Join(predicate, t1, t2);
        return joined;
	}

	//SELECT * FROM T1, T2 WHERE T1. column0 > 1 AND T1.column1 = T2.column1;
	public Operator queryTwo(DbIterator t1, DbIterator t2) {
        IntField int1 = new IntField(1);
        Predicate pred1 = new Predicate(0, Predicate.Op.GREATER_THAN, int1);
        JoinPredicate pred2 = new JoinPredicate(1, Predicate.Op.EQUALS, 1);

        Filter filtered_t1 = new Filter(pred1, t1);
        Join joined = new Join(pred2, filtered_t1, t2);

		return joined;
	}

	//SELECT column0, MAX(column1) FROM T1 WHERE column2 > 1 GROUP BY column0;
	public Operator queryThree(DbIterator t1) {
        IntField int1 = new IntField(1);
        Predicate pred1 = new Predicate(2, Predicate.Op.GREATER_THAN, int1);
        Filter filtered_t1 = new Filter(pred1, t1);
        Aggregate agg = new Aggregate(filtered_t1, 1, 0, Aggregator.Op.MAX);

        return agg;
	}

	// SELECT ​​* FROM T1, T2
	// WHERE T1.column0 < (SELECT COUNT(*​​) FROM T3)
	// AND T2.column0 = (SELECT AVG(column0) FROM T3)
	// AND T1.column1 >= T2. column1
	// ORDER BY T1.column0 DESC;
	public Operator queryFour(DbIterator t1, DbIterator t2, DbIterator t3) throws TransactionAbortedException, DbException {
        t1 = new OrderBy(0, false, t1);
        Aggregate agg_1 = new Aggregate(t3, 0, -1, Aggregator.Op.COUNT);
        IntField t3_count = (IntField) agg_1.fetchNext().getField(0);

        t3.rewind();
        agg_1.rewind();
        Aggregate agg_2 = new Aggregate(t3, 0, -1, Aggregator.Op.AVG);

        IntField t3_c0_average = (IntField) agg_2.fetchNext().getField(0);

        Predicate pred1 = new Predicate(0, Predicate.Op.LESS_THAN, t3_count);
        Predicate pred2 = new Predicate(0, Predicate.Op.EQUALS, t3_c0_average);
        JoinPredicate pred3 = new JoinPredicate(1, Predicate.Op.GREATER_THAN_OR_EQ, 1);

        Filter filtered_t1 = new Filter(pred1, t1);
        Filter filtered_t2 = new Filter(pred2, t2);
        Join joined  = new Join(pred3, filtered_t1, filtered_t2);

        return joined;
	}


}