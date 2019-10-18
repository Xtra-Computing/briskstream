package applications.spout;

import constants.BaseConstants;
import helper.wrapper.StringStatesWrapper;
import util.OsUtils;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class MemFileSpout_Txn extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout_Txn.class);
    private static LinkedList<String> logger = new LinkedList<String>();
    protected final String splitregex = ",";
    protected File files;
    protected String[] array;
    protected int curFileIndex = 0;
    protected int curLineIndex = 0;
    //  protected int numTasks;
    protected int start_index = 0;
    protected int element = 0;
    transient protected BufferedWriter writer;
    int loop = 1;
    long start = 0, end = 0;
    int control_emit = 1;
    int cnt;
    private int taskId;
    //protected int end_index=1000000;//1M
    private int end_index = 0;//32M
    private int counter = 0;
    private boolean finished = false;

    private long last_emit = 0;
    private long msgID_start;
    private long msgID_end;
    private long msgID_counter;

    public MemFileSpout_Txn() {
        super();
    }

    @Override
    protected void initialize() {
//        LOG.info("Spout initialize is being called");
        loop = 1;
        cnt = 0;
        counter = 0;

        String OS_prefix = null;

        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }
        String path;
        if (OsUtils.isMac()) {
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_TEST_PATH)));
        } else {
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
        }

        String s = System.getProperty("user.home").concat("/Documents/data/app/").concat(path);

        List<String> str_l = new LinkedList<String>();

        try {
            openFile(s, str_l);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        int taskId = context.getThisTaskIndex();//context.getThisTaskId(); start from 0..

        msgID_start = (long) (1E9 * (taskId));
        msgID_end = (long) (1E9 * (taskId + 1));
        msgID_counter = msgID_start;

    }

    private void build(Scanner scanner, List<String> str_l) {
        cnt = 100;
        if (config.getInt("batch") == -1) {
            while (scanner.hasNext()) {
                str_l.add(scanner.next());//for micro-benchmark only
            }
        } else {

            if (!config.getBoolean("microbenchmark")) {
                //&& cnt-- > 0
                if (OsUtils.isWindows()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        str_l.add(scanner.nextLine());
                    }
                } else {
                    while (scanner.hasNextLine()) {
                        str_l.add(scanner.nextLine()); //normal..
                    }
                }

            } else {
                int tuple_size = config.getInt("size_tuple");
                LOG.info("Additional tuple size to emit:" + tuple_size);
                StringStatesWrapper wrapper = new StringStatesWrapper(tuple_size);
//                        (StateWrapper<List<StreamValues>>) ClassLoaderUtils.newInstance(parserClass, "wrapper", LOG, tuple_size);
                if (OsUtils.isWindows()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        construction(scanner, str_l, wrapper);
                    }
                } else {
                    while (scanner.hasNextLine()) {
                        construction(scanner, str_l, wrapper);
                    }
                }
            }
        }
        scanner.close();
    }

    private void construction(Scanner scanner, List<String> str_l, StringStatesWrapper wrapper) {

        String[] words = scanner.nextLine().split(splitregex);

        StringBuilder sb = new StringBuilder();

        for (String word : words) {
            sb.append(word + wrapper.getTuple_states() + splitregex);
        }

        str_l.add(sb.toString());


    }

    private void read(String prefix, int i, String postfix, List<String> str_l) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File((prefix + i) + "." + postfix), "UTF-8");
        build(scanner, str_l);
    }

    private void splitRead(String fileName, List<String> str_l) throws FileNotFoundException {
        int numSpout = context.getComponentTasks(context.getThisComponentId()).size();
        int range = 10 / numSpout;//original file is split into 10 sub-files.
        int offset = this.taskId * range + 1;
        String[] split = fileName.split("\\.");
        for (int i = offset; i < offset + range; i++) {
            read(split[0], i, split[1], str_l);
        }

        if (this.taskId == numSpout - 1) {//if this is the last executor of spout
            for (int i = offset + range; i <= 10; i++) {
                read(split[0], i, split[1], str_l);
            }
        }
    }

    private void openFile(String fileName, List<String> str_l) throws FileNotFoundException {
        boolean split;
        if (OsUtils.isMac()) {
            split = false;
        } else {
            split = config.getBoolean("split", true);
        }
        if (split) {
            splitRead(fileName, str_l);
        } else {
            Scanner scanner = new Scanner(new File(fileName), "UTF-8");
            build(scanner, str_l);
        }

        array = str_l.toArray(new String[str_l.size()]);
        end_index = array.length * config.getInt("count_number", 1);
        counter = 0;
        LOG.info("spout:" + this.taskId + " elements:" + end_index);
    }

    private void spout_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        LOG.info("JVM PID  = " + pid);

        FileWriter fw;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output")
                    + OsUtils.OS_wrapper("spout_threadId.txt")));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            //writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void reset_index() {
        if (counter == array.length) {
            counter = 0;
        }
    }

    /**
     * Flink poorly supports multi-streams..
     */
    @Override
    public void nextTuple() {
        counter++;
        reset_index();

//		final long currentTimeNanos = System.nanoTime();
//		if (currentTimeNanos - last_emit > 1E8) {//emit marker tuple per 100 mseconds
//			last_emit = currentTimeNanos;
//			collector.emit(new Values(new String(array[counter]), msgID_counter++ % msgID_end, currentTimeNanos));
//		} else {
//			collector.emit(new Values(new String(array[counter]), -1L, null));
//		}

        collector.emit(new Values(new String(array[counter])));

    }
}	
