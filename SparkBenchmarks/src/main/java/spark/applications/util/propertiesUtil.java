package spark.applications.util;

import applications.common.tools.OsUtils;
import org.apache.spark.SparkConf;
import spark.applications.util.config.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by szhang026 on 5/4/2016.
 */
public class propertiesUtil {
    public static String cfg_path = null;
    private static String CFG_PATH = null;

    private static Properties loadProperties(String filename, boolean classpath) throws IOException {
        Properties properties = new Properties();
        InputStream is;
        is = new FileInputStream(filename);
        properties.load(is);
        is.close();

        return properties;
    }

    public static SparkConf load(String application) {
        SparkConf sparkConf;

        if (OsUtils.isWindows()) {
            CFG_PATH = "C:\\Users\\I309939\\Documents\\NUMA-streamBenchmarks\\common\\src\\main\\resources\\config\\%s.properties";
        } else {
            CFG_PATH = System.getProperty("user.home").concat("/NUMA-streamBenchmarks/common/src/main/resources/config/%s.properties");
        }

        try {
            cfg_path = String.format(CFG_PATH, application);
            Properties p = loadProperties(cfg_path, true);
            sparkConf = Configuration.fromProperties(p);
        } catch (IOException ex) {
            throw new RuntimeException("Unable to load configuration file", ex);
        }
        return sparkConf;
    }

    private static List<String> changeValueOf(List<String> lines, String username, int newVal) {
        List<String> newLines = new ArrayList<String>();
        for (String line : lines) {
            if (line.contains(username)) {
                String[] vals = line.split("=");
                newLines.add(vals[0] + "=" + String.valueOf(newVal));
            } else {
                newLines.add(line);
            }

        }
        return newLines;
    }

    private static List<String> changeValueOf(List<String> lines, String username, String newVal) {
        List<String> newLines = new ArrayList<String>();
        for (String line : lines) {
            if (line.contains(username)) {
                String[] vals = line.split("=");
                newLines.add(vals[0] + "=" + newVal);
            } else {
                newLines.add(line);
            }

        }
        return newLines;
    }

    public static void replaceSelected(String filename, String target, int value) {
        File f = new File(filename);
        f.setWritable(true);
        try {
            List<String> lines = Files.readAllLines(f.toPath(), Charset.defaultCharset());
            Files.write(f.toPath(), changeValueOf(lines, target, value), Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void replaceSelected(String filename, String target, String value) {
        File f = new File(filename);
        f.setWritable(true);
        try {
            List<String> lines = Files.readAllLines(f.toPath(), Charset.defaultCharset());
            Files.write(f.toPath(), changeValueOf(lines, target, value), Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
