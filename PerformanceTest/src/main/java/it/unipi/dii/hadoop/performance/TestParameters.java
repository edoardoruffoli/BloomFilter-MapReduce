package it.unipi.dii.hadoop.performance;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;

public class TestParameters {
    private static int[] nReducerJob0 = {1};
    private static int[] nReducerJob1 = {1};
    private static int[] nReducerJob2 = {1, 2};
    private static int[] nLineSplits = {800000};
    private static double[] p = {0.01, 0.03, 0.05, 0.1};
    private static int reps = 10;

    public static void setConfigFile(String path, int nReducerJob0, int nReducerJob1, int nReducerJob2) {

        try {
            FileInputStream in = new FileInputStream(path);
            Properties prop = new Properties();
            prop.load(in);
            in.close();

            FileOutputStream out = new FileOutputStream(path);
            prop.setProperty("input.job0-n-reducer", String.valueOf(nReducerJob0));
            prop.setProperty("input.job1-n-reducer", String.valueOf(nReducerJob1));
            prop.setProperty("input.job2-n-reducer", String.valueOf(nReducerJob2));
            prop.store(out, null);
            out.close();
        }
        catch (IOException io) {
            io.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JSONArray results = new JSONArray();
        String cmd =  "hadoop jar hadoop-1.0-SNAPSHOT.jar it.unipi.dii.hadoop.mapreduce.Driver";

        for (int i0=0; i0<nReducerJob0.length; i0++) {
            for (int i1=0; i1<nReducerJob1.length; i1++) {
                for (int i2=0; i2<nReducerJob2.length; i2++) {
                    System.out.print("Stage: nReducerJob0 = " + nReducerJob0[i0] + ", ");
                    System.out.print("nReducerJob1 = " + nReducerJob1[i1] + ", ");
                    System.out.print("nReducerJob2 = " + nReducerJob2[i2] + ", ");
                    System.out.println(" ");

                    setConfigFile("config.properties", nReducerJob0[i0], nReducerJob1[i1], nReducerJob2[i2]);

                    Process process = Runtime.getRuntime().exec(cmd);

                    long startTime = System.currentTimeMillis();

                    process.waitFor();

                    long endTime = System.currentTimeMillis();

                    JSONObject iterationResult = new JSONObject();
                    iterationResult.put("executionTime", endTime - startTime);
                    iterationResult.put("nReducerJob0", nReducerJob0[i0]);
                    iterationResult.put("nReducerJob1", nReducerJob1[i1]);
                    iterationResult.put("nReducerJob2", nReducerJob2[i2]);
                    //iterationResult.put("repetition", r);

                    results.put(iterationResult);

                }
            }
        }
        try (PrintWriter out = new PrintWriter("hadoop.json")) {
            out.println(results.toString(4));
        } catch (Exception exception) {
            exception.printStackTrace();
            System.out.println(results.toString(4));
        }
    }
}
