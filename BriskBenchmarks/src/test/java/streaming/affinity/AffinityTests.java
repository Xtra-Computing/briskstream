package streaming.affinity;


import applications.util.Configuration;
import brisk.controller.affinity.AffinityController;
import brisk.execution.runtime.executorThread;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.msgs.GeneralMsg;
import brisk.queue.impl.P1C1Queue;
import ch.usi.overseer.OverHpc;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import static applications.Constants.EVENTS.PERF_COUNT_HW_CPU_CYCLES;
import static applications.util.OsUtils.isUnix;

/**
 * Created by tony on 5/14/2017.
 */
public class AffinityTests {
    private final static Logger LOG = LoggerFactory.getLogger(AffinityTests.class);
    private final int which_queue;
    private final AffinityController AC;
    private final double size = 20 * 1E3;
    private int array_size;
    //	protected String[] array;
    private OverHpc HPCMonotor;
    private Producer p;
    private Consumer c1;
    private Consumer c2;
    private Consumer c3;
    private Queue<TransferTuple> q;
    private Queue<TransferTuple> q2;
    private volatile boolean profile_producer = false;

    public AffinityTests(Configuration conf) {
        which_queue = conf.getInt("which_queue", 1);
//		OsUtils.configLOG(LOG);
        if (isUnix()) {
            HPCMonotor = OverHpc.getInstance();
            if (HPCMonotor == null) {
                System.out.println("ERROR: unable to init OverHpc");
            }

            // Init event: cycles, TODO: LLC miss for memory fetch.
            if (!HPCMonotor.initEvents(PERF_COUNT_HW_CPU_CYCLES)) {
                LOG.error("ERROR: invalid event");
            }
        }
        AC = new AffinityController(conf, new applications.HUAWEI_Machine());
    }

    public void warmup() {
//		System.out.println("====Start case 1 test=====");
        CountDownLatch latch = new CountDownLatch(2);

        long[] cpu_producer = AC.requirePerCore(0);
        long[] cpu_consumer1 = AC.requirePerCore(0);
        long[] cpu_consumer2 = AC.requirePerCore(0);
        long[] cpu_consumer3 = AC.requirePerCore(0);

        p = new Producer(cpu_producer, 0, latch, which_queue);
        p.start();
        try {
//			//LOG.DEBUG("profile producer first..");
//			profile_producer = true;
            p.join();

            c1 = new Consumer(cpu_consumer1, 0, latch, 1);
            c1.start();
//			//LOG.DEBUG("profile consumer1 now..");
//			profile_producer = false;
            c1.join();

            c2 = new Consumer(cpu_consumer2, 0, latch, 2);
            c2.start();
//			//LOG.DEBUG("profile consumer2 now..");
//			profile_producer = false;
            c2.join();

            c3 = new Consumer(cpu_consumer3, 0, latch, 2);
            c3.start();
//			//LOG.DEBUG("profile consumer3 now..");
//			profile_producer = false;
            c3.join();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//		System.out.println("queue:" + q.getClass().getName() + " Exe_time of Producer (ms):\t" + p.execution_stats.getPercentile(50));
//		System.out.println("queue:" + q.getClass().getName() + " Exe_time of Consumer 1 (ms):\t" + c1.execution_stats.getPercentile(50));
//		System.out.println("queue:" + q.getClass().getName() + " Exe_time of Consumer 2 (ms):\t" + c2.execution_stats.getPercentile(50));
//		System.out.println("queue:" + q.getClass().getName() + " Exe_time of Consumer 3 (ms):\t" + c3.execution_stats.getPercentile(50));
//		System.out.println("Case 1 test finished");
    }


    public void case1() {
        System.out.println("====Start case 1 test=====");
        CountDownLatch latch = new CountDownLatch(2);

        long[] cpu_producer = AC.requirePerCore(0);
        long[] cpu_consumer1 = AC.requirePerCore(0);
        long[] cpu_consumer2 = AC.requirePerCore(0);
        long[] cpu_consumer3 = AC.requirePerCore(0);

        p = new Producer(cpu_producer, 0, latch, which_queue);
        System.out.println("Both producer and consumer are scheduled into the same socket 0");
        p.start();
        try {
            //LOG.DEBUG("profile producer first..");
//			profile_producer = true;
            p.join();

            c1 = new Consumer(cpu_consumer1, 0, latch, 1);
            c1.start();
            //LOG.DEBUG("profile consumer1 now..");
//			profile_producer = false;
            c1.join();

            c2 = new Consumer(cpu_consumer2, 0, latch, 2);
            c2.start();
            //LOG.DEBUG("profile consumer2 now..");
//			profile_producer = false;
            c2.join();

            c3 = new Consumer(cpu_consumer3, 0, latch, 2);
            c3.start();
            //LOG.DEBUG("profile consumer3 now..");
//			profile_producer = false;
            c3.join();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("queue:" + q.getClass().getName() + " Exe_time of Producer (ms):\t" + p.execution_stats.getPercentile(50));
        System.out.println("queue:" + q.getClass().getName() + " Exe_time of Consumer 1 (ms):\t" + c1.execution_stats.getPercentile(50));
        System.out.println("queue:" + q.getClass().getName() + " Exe_time of Consumer 2 (ms):\t" + c2.execution_stats.getPercentile(50));
        System.out.println("queue:" + q.getClass().getName() + " Exe_time of Consumer 3 (ms):\t" + c3.execution_stats.getPercentile(50));
        System.out.println("Case 1 test finished");
    }

    public void case2() {
        System.out.println("=======Start case 2 test===========");
        CountDownLatch latch = new CountDownLatch(2);

        long[] cpu_producer = AC.requirePerCore(0);
        long[] cpu_consumer1 = AC.requirePerCore(0);
        long[] cpu_consumer2 = AC.requirePerCore(7);
        long[] cpu_consumer3 = AC.requirePerCore(7);

        p = new Producer(cpu_producer, 0, latch, which_queue);
        System.out.println("Both procuder and consuemr are scheduled into the different sockets");

        p.start();
        try {
            //LOG.DEBUG("profile producer first..");
//			profile_producer = true; //			profile_producer = false;
            p.join();
            c1 = new Consumer(cpu_consumer1, 0, latch, 1);
            c1.start();
            //LOG.DEBUG("profile consumer 1 now..");
            c1.join();

            c2 = new Consumer(cpu_consumer2, 7, latch, 2);
            c2.start();
            //LOG.DEBUG("profile consumer 2 now..");
            c2.join();

            c3 = new Consumer(cpu_consumer3, 7, latch, 2);
            c3.start();
            //LOG.DEBUG("profile consumer 3 now..");
            c3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("which queue:" + q.getClass().getName() + " Exe_time of Producer (ms):\t" + p.execution_stats.getPercentile(50));
        System.out.println("which queue:" + q.getClass().getName() + " Exe_time of Consumer 1 (ms):\t" + c1.execution_stats.getPercentile(50));
        System.out.println("which queue:" + q.getClass().getName() + " Exe_time of Consumer 2 (ms):\t" + c2.execution_stats.getPercentile(50));
        System.out.println("which queue:" + q.getClass().getName() + " Exe_time of Consumer 3 (ms):\t" + c3.execution_stats.getPercentile(50));
        System.out.println("=====Case 2 test finished=======");
    }


    protected class Producer extends executorThread {
        final DescriptiveStatistics execution_stats = new DescriptiveStatistics();
        private final int which_queue;
        //		protected int index_e = 0;
//		private String[] array;
        long previousValue;
        long currentvalue;
        private long Exe_time;
        private long start;
        private long end;

        Producer(long[] cpu, int node, CountDownLatch latch, int which_queue) {
            super(null, null, null, cpu, node, latch, HPCMonotor, null);
            this.which_queue = which_queue;
        }

        private void ini(List<String> str_l) {
//			int count = array_size;//limit test size
//			String fileName;
//			if (OsUtils.isMac()) {
//				fileName = System.getProperty("user.home").concat("/Documents/data/app/").concat("wc/test.csv");
//			} else {
//				fileName = System.getProperty("user.home").concat("/Documents/data/app/").concat("wc/Skew01.dat");
//			}
//			try {
//				Scanner scanner = new Scanner(new File(fileName), "UTF-8");
//				while (scanner.hasNextLine()) {
////					count--;
////					if (count < 0) {
////						break;
////					}
//					str_l.add(scanner.nextLine()); //normal..
//				}
//				array = str_l.toArray(new String[str_l.size()]);
//				array_size = array.length;
//
//
//			} catch (FileNotFoundException e) {
//				e.printStackTrace();
//			}

//			array = new String[1];

            switch (which_queue) {
                case 1:
                    q = new P1C1Queue<>();
                    q2 = new P1C1Queue<>();
                    break;
                case 2:
                    q = new SpscArrayQueue<>(1073740);
                    q2 = new SpscArrayQueue<>(1073740);
                    break;
                case 3:
                    q = new MpscArrayQueue<>(1073740);
                    q2 = new MpscArrayQueue<>(1073740);
                    break;
            }
            //// new P1C1Queue<>(null);
            //LOG.DEBUG("Producer prepared data :" + array_size);
        }

//		volatile String emit;

        @Override
        public void run() {

            try {
                Thread.currentThread().setName("Producer");
                final int pid = HPCMonotor != null ? HPCMonotor.getThreadId() : (int) Thread.currentThread().getId();
                long[] binding = binding();
//			//LOG.DEBUG("Producer: " + pid + " binding:" + Long.toBinaryString(0x100000000L | binding[0]).substring(1));
                List<String> str_l = new LinkedList<>();
                ini(str_l);


                TransferTuple t = new TransferTuple(0, 0, 10, null);

                int p = 0;

                for (int i = 1; i <= size; i++) {
                    char[] array = String.valueOf(i).toCharArray();
                    t.add(p++, new GeneralMsg(null, array));
                    if (i % 10 == 0) {

                        if (HPCMonotor != null) {
                            HPCMonotor.bindEventsToThread(pid);
                            previousValue = HPCMonotor.getEventFromThread(pid, 0);
                        } else {
                            start = System.nanoTime();
                        }

                        q.offer(t);
                        t = new TransferTuple(0, 0, 10, null);
                        p = 0;
//						//LOG.DEBUG(String.valueOf(OsUtils.Addresser.addressOf(q)));

                        if (HPCMonotor != null) {
                            currentvalue = HPCMonotor.getEventFromThread(pid, 0);
                            Exe_time = ((currentvalue - previousValue));
                            execution_stats.addValue(Exe_time * 2.5 / 1E6);
                        } else {
                            end = System.nanoTime();
                            execution_stats.addValue((end - start) / 1E6);
                        }

                    }
                }


                //LOG.DEBUG("Producer finished test, ready for consumer to test");

//				latch.countDown();          //tells others I'm ready.

            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        }

        @Override
        protected void _execute_noControl() {
        }

        @Override
        protected void _execute() {

        }

        @Override
        protected void _profile() {

        }
    }

    protected class Consumer extends executorThread {
        final DescriptiveStatistics execution_stats = new DescriptiveStatistics();
        private final int which_consumer;
        protected int index_e = 0;
        long previousValue;
        int cnt = 0;
        long currentvalue;
        String read;
        private String[] array;
        private long Exe_time;
        private long start;
        private long end;
        private Queue<TransferTuple> privateq;

        Consumer(long[] cpu, int node, CountDownLatch latch, int which_consumer) {
            super(null, null, null, cpu, node, latch, HPCMonotor, null);
            this.which_consumer = which_consumer;


            if (which_consumer == 1) {
                privateq = q;
            } else {
                privateq = q2;
            }
        }

        @Override
        public void run() {

            try {
                Thread.currentThread().setName("Consumer");
                final int pid = HPCMonotor != null ? HPCMonotor.getThreadId() : (int) Thread.currentThread().getId();

//				latch.countDown();          //tells others I'm ready.
//				try {
//					latch.await();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}

                long[] binding = binding();
//			//LOG.DEBUG("Consumer:" + pid + " binding:" + Long.toBinaryString(0x100000000L | binding[0]).substring(1));
//				LinkedList<String> obtained = new LinkedList<>();

                array = new String[(int) size];
//				LinkedList<TransferTuple> obtained = new LinkedList<>();

                int count = 0;

                //System.out.println("Queue size: " + q.size());
                for (int i = 0; i < size / 10; i++) {


                    if (HPCMonotor != null) {
                        HPCMonotor.bindEventsToThread(pid);
                        previousValue = HPCMonotor.getEventFromThread(pid, 0);
                    } else {
                        start = System.nanoTime();
                    }
                    TransferTuple tuple = privateq.poll();
                    for (int j = 0; j < 10; j++) {

//						try {
                        array[count++] = tuple.getString(0, j);
//						} catch (Exception e) {
//							System.nanoTime();
//						}
                    }
                    if (HPCMonotor != null) {
                        currentvalue = HPCMonotor.getEventFromThread(pid, 0);
                        Exe_time = ((currentvalue - previousValue));
                        execution_stats.addValue(Exe_time * 2.5 / 1E6);
                    } else {
                        end = System.nanoTime();
                        execution_stats.addValue((end - start) / 1E6);
                    }
                }

//				for (TransferTuple tuple : obtained) {
//					for (int i = 0; i < 10; i++) {
//						System.out.println("I got the array with element:" + tuple.getString(0, i));
//					}
//				}

                TransferTuple t = new TransferTuple(0, 0, 10, null);
                int p = 0;
                for (int i = 0; i < size; i++) {
                    if (i != 0 && i % 10 == 0) {
                        q2.offer(t);
                        t = new TransferTuple(0, 0, 10, null);
                        p = 0;
                    }


                    char[] result = array[i].toCharArray();
//					System.arraycopy(array[i], 0, result, 0, array[i].length);
                    t.add(p++, new GeneralMsg(null, result));
                }
                q2.offer(t);
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        }

        @Override
        protected void _execute_noControl() {


        }

        @Override
        protected void _execute() {

        }

        @Override
        protected void _profile() {

        }
    }
}
