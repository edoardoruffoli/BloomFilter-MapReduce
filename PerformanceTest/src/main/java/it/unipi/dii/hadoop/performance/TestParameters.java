package it.unipi.dii.hadoop.performance;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class TestParameters {
    private static int[] nReducerJob0 = {1000, 10000, 100000};
    private static int[] nReducerJob1 = {1000, 10000, 100000};
    private static int[] nReducerJob2 = {1000, 10000, 100000};
    private static int[] nLineSplits = {800000};
    private static double[] p = {0.01, 0.03, 0.05, 0.1};
    private static int reps = 10;

    public static void setConfigFile(String path, int nReducerJob0, int nReducerJob1, int nReducerJob2) {
        try (OutputStream output = new FileOutputStream(path)) {

            Properties prop = new Properties();

            prop.setProperty("input.job0-n-reducer", String.valueOf(nReducerJob0));
            prop.setProperty("input.job1-n-reducer", String.valueOf(nReducerJob1));
            prop.setProperty("input.job2-n-reducer", String.valueOf(nReducerJob2));

            prop.store(output, null);
        }
        catch (IOException io) {
            io.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String cmd =  "hadoop jar hadoop-1.0-SNAPSHOT.jar it.unipi.dii.hadoop.mapreduce.Driver";

        for (int i0=0; i0<nReducerJob0.length; i0++) {
            for (int i1=0; i1<nReducerJob1.length; i1++) {
                for (int i2=0; i2<nReducerJob2.length; i2++) {
                    System.out.print("Stage: nReducerJob0 = " + nReducerJob0[i0] + ", ");
                    System.out.print("nReducerJob1 = " + nReducerJob1[i1] + ", ");
                    System.out.print("nReducerJob2 = " + nReducerJob2[i2] + ", ");

                    setConfigFile("config.properties", nReducerJob0[i0], nReducerJob1[i1], nReducerJob2[i2]);

                    Process process = Runtime.getRuntime().exec(cmd);

                    process.waitFor();


                }
            }
        }
    }
}
