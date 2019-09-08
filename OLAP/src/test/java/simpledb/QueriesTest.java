package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.Before;
import org.junit.Test;
import simpledb.systemtest.SimpleDbTestBase;

public class QueriesTest extends SimpleDbTestBase {

  int width1 = 2;
  int width2 = 3;
  int width3 = 3;
  DbIterator scan1;
  DbIterator scan2;
  DbIterator scan3;
  DbIterator scan4;
  DbIterator q1;
  DbIterator q2;
  DbIterator q3;
  DbIterator q4;
  QueryPlans queryPlans;

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
    this.scan3 = TestUtil.createTupleList(width3,
        new int[] { 1, 2, 1,
                    1, 4, 2,
                    1, 6, 3,
                    3, 2, 4,
                    3, 4, 2,
                    3, 6, 0,
                    5, 7, 1 });
    this.scan4 = TestUtil.createTupleList(1,
        new int[] { 1,
                    2,
                    3,
                    1,
                    2,
                    3});
    this.q1 = TestUtil.createTupleList(width1 + width2,
        new int[] { 1, 2, 1, 2, 3,
                    3, 4, 3, 4, 5,
                    5, 6, 5, 6, 7 });
    this.q2 = TestUtil.createTupleList(width1 + width2,
        new int[] {
                    3, 4, 3, 4, 5,
                    5, 6, 5, 6, 7 });
    this.q3 = TestUtil.createTupleList(2,
        new int[] { 1, 6,
                    3, 4 });
    this.q4 = TestUtil.createTupleList(width1 + width2,
        new int[] { 5, 6, 2, 3, 4,
                    3, 4, 2, 3, 4});
    this.queryPlans = new QueryPlans();
  }

  /**
   * Unit test for query 1.
   */
  @Test public void queryOne() throws Exception {
    Operator op = queryPlans.queryOne(scan1, scan2);
    op.open();
    q1.open();
    TestUtil.matchAllTuples(q1, op);
  }

  /**
   * Unit test for query 2.
   */
  @Test public void queryTwo() throws Exception {
    Operator op = queryPlans.queryTwo(scan1, scan2);
    op.open();
    q2.open();
    TestUtil.matchAllTuples(q2, op);
  }

  /**
   * Unit test for query 3.
   */
  @Test public void queryThree() throws Exception {
    Operator op = queryPlans.queryThree(scan3);
    op.open();
    q3.open();
    TestUtil.matchAllTuples(q3, op);
  }

  /**
   * Unit test for query 4.
   */
  @Test public void queryFour() throws Exception {
    Operator op = queryPlans.queryFour(scan1, scan2, scan4);
    op.open();
    q4.open();
    TestUtil.matchAllTuples(q4, op);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(QueriesTest.class);
  }
}

