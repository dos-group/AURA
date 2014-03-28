package de.tuberlin.aura.demo.benchmark;

import java.io.*;

/**
 * Created by teots on 2/27/14.
 */
public class SanityEvaluator {

    private String path;

    public SanityEvaluator(String path) {
        this.path = path;
    }

    public void evaluate() {

        int sumSources = 0;
        int sumSinks = 0;

        File rootFolder = new File(this.path);
        for (File folder : rootFolder.listFiles()) {
            for (File file : folder.listFiles()) {

                System.out.println(folder.getName());

                try {
                    BufferedReader br = new BufferedReader(new FileReader(file));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        if (line.contains("SOURCE")) {
                            String[] tokens = line.split("\\|");
                            sumSources += Integer.parseInt(tokens[1]);
                            System.out.println(line);
                        } else if (line.contains("SINK")) {
                            String[] tokens = line.split("\\|");
                            sumSinks += Integer.parseInt(tokens[1]);
                            System.out.println(line);
                        } else if (line.toLowerCase().contains("error")) {
                            System.out.println(line);
                        } else if (line.toLowerCase().contains("exception")) {
                            System.out.println(line);
                        }
                    }

                    br.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                System.out.println();
            }
        }

        System.out.println("Buffer Diff: " + Integer.toString(sumSources - sumSinks));
    }

    public static void main(String[] args) {
        SanityEvaluator evaluator = new SanityEvaluator("/home/teots/Desktop/logs");
        evaluator.evaluate();
    }
}
