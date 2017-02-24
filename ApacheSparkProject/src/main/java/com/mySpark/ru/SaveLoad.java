package com.mySpark.ru;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

public class SaveLoad {

    private static FileWriter fileWriter;


    public static void saveToCsv(Dataset<Row> setToSave, String path) {
        List<Row> list = setToSave.collectAsList();

        try {

            fileWriter = new FileWriter(path);

            for (Row r : list) {
                fileWriter.write(r.get(0) + "," + r.get(1) + "," + r.get(2) + "\n");
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String csvParser(String input) {

        String header1;

        int numberOfColumns;

        String output = input.replace(".csv", "Parsed.csv");

        if (input.contains("clients.csv")) {
            header1 = "id,name,surname,account_id,date_of_birth,sex";
            numberOfColumns = 6;
        } else if (input.contains("transactions.csv")) {
            header1 = "tr_id,client_id,amount,date,terminal_id,mcc";
            numberOfColumns = 6;
        } else if (input.contains("terminals.csv")) {
            header1 = "terminal_id,category,merchant_name,x,y";
            numberOfColumns = 5;
        } else {
            throw new RuntimeException("Wrong name or path of csv file");
        }

        try {
            Scanner scanner = new Scanner(Paths.get(input));
            fileWriter = new FileWriter(output);

            fileWriter.write(header1 + "\n");

            while (scanner.hasNext()) {

                String[] line = scanner.nextLine().split(";");

                if (numberOfColumns != line.length) continue;

                for (int i = 0; i < line.length; i++) {
                    if (line.length - i > 1) {
                        String s = (line[i] + ",").replace("\"", "");
                        fileWriter.write(s);
                    } else {
                        String s = (line[i] + "\n").replace("\"", "");
                        fileWriter.write(s);
                    }
                }
            }
            fileWriter.flush();
            fileWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return output;
    }

    public static Dataset<Row> loadFromCsv(SparkSession session, String pathToCsv) {

        String path = SaveLoad.csvParser(pathToCsv);
        Dataset<Row> dataset = session.read()
                .option("header", "true")
                .csv(path);

        return dataset;
    }
}
