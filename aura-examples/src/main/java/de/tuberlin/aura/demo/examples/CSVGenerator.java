package de.tuberlin.aura.demo.examples;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

/**
 *
 */
public class CSVGenerator {



    public static void main(final String[] args) {


        final PrintWriter writer;
        try {

            writer = new PrintWriter("CSVData.csv", "UTF-8");
            for (long i = 0; i < 50000000L; ++i) {
                writer.println(i + "," + UUID.randomUUID());
            }
            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
