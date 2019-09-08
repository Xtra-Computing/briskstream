package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.Before;
import org.junit.Test;
import simpledb.systemtest.SimpleDbTestBase;

public class ChunkNestedLoopJoinTest extends SimpleDbTestBase {

  int width1 = 2;
  int width2 = 3;
  DbIterator scan1;
  DbIterator scan2;
  DbIterator eqJoin;
  DbIterator gtJoin;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleLists() throws Exception {
    this.scan1 = TestUtil.createTupleList(width1,
        new int[] { 1, 2,
                    3, 4,
                    5, 6,
                    7, 8 });
    this.scan2 = TestUtil.createTupleList(width2,
        new int[] { 1, 2, 3,
                    2, 3, 4,
                    3, 4, 5,
                    4, 5, 6,
                    5, 6, 7 });
    this.eqJoin = TestUtil.createTupleList(width1 + width2,
        new int[] { 1, 2, 1, 2, 3,
                    3, 4, 3, 4, 5,
                    5, 6, 5, 6, 7 });
    this.gtJoin = TestUtil.createTupleList(width1 + width2,
        new int[] {
                    3, 4, 1, 2, 3, // 1, 2 < 3
                    3, 4, 2, 3, 4,
                    5, 6, 1, 2, 3, // 1, 2, 3, 4 < 5
                    5, 6, 2, 3, 4,
                    5, 6, 3, 4, 5,
                    5, 6, 4, 5, 6,
                    7, 8, 1, 2, 3, // 1, 2, 3, 4, 5 < 7
                    7, 8, 2, 3, 4,
                    7, 8, 3, 4, 5,
                    7, 8, 4, 5, 6,
                    7, 8, 5, 6, 7 });
  }

  /**
   * Unit test for ChunkNestedLoopJoin.fetchNext() using a &gt; predicate
   */
  @Test public void gtJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.GREATER_THAN, 0);
    ChunkNestedLoopJoin op = new ChunkNestedLoopJoin(pred, scan1, scan2, 2);
    op.open();
    gtJoin.open();
    TestUtil.matchAllTuples(gtJoin, op);
  }

  /**
   * Unit test for ChunkNestedLoopJoin.fetchNext() using an = predicate
   */
  @Test public void eqCNLJ() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    ChunkNestedLoopJoin op = new ChunkNestedLoopJoin(pred, scan1, scan2, 1);
    op.open();
    eqJoin.open();
    TestUtil.matchAllTuples(eqJoin, op);
  }


    /**
     * Unit test for ChunkNestedLoopJoin.fetchNext() using an = predicate
     */
    @Test public void biggerChunk() throws Exception {
        JoinPredicate pred = new JoinPredicate(0, Predicate.Op.GREATER_THAN, 0);
        ChunkNestedLoopJoin op = new ChunkNestedLoopJoin(pred, scan1, scan2, 4);
        op.open();
        gtJoin.open();
        TestUtil.matchAllTuples(gtJoin, op);
    }
  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ChunkNestedLoopJoinTest.class);
  }
}

