package applications.general.bolts.gs;


import applications.general.param.mb.MicroEvent;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.enable_app_combo;
import static engine.profiler.MeasureTools.*;

public class GSBolt_ts_nopush extends GSBolt_ts {


    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_ts_nopush.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private Collection<MicroEvent> WriteEventsHolder;

    public GSBolt_ts_nopush(int fid) {
        super(fid);
    }

    @Override
    protected void write_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            //it simply construct the operations and return.
            transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);//asynchronously return.
        }
        WriteEventsHolder.add(event);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        WriteEventsHolder = new ArrayDeque<>();

    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {

            int readSize = EventsHolder.size();

            int writeEvents = WriteEventsHolder.size();

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);

            transactionManager.start_evaluate(thread_Id, in.getBID());//start lazy evaluation in transaction manager.

            END_TP_TIME_MEASURE(thread_Id);// overhead_total TP time.

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            READ_REQUEST_CORE();

            WRITE_REQUEST_CORE();

            END_COMPUTE_TIME_MEASURE_TS(thread_Id, 0, readSize + writeEvents, 0);//overhead_total compute time.

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);//overhead_total txn time.

            READ_POST();

            WRITE_POST();

            if (!enable_app_combo) {
                final Marker marker = in.getMarker();
                this.collector.ack(in, marker);//tell spout it has finished transaction processing.
            } else {

            }

            //post_process for events left-over.

            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize + writeEvents);

            EventsHolder.clear();//all tuples in the EventsHolder are finished.
            WriteEventsHolder.clear();

        } else {
            execute_ts_normal(in);
        }
    }

    private void WRITE_REQUEST_CORE() {
        for (MicroEvent event : WriteEventsHolder) {
            WRITE_CORE(event);
        }
    }

    private void WRITE_POST() throws InterruptedException {
        for (MicroEvent event : WriteEventsHolder)
            WRITE_POST(event);
    }

}