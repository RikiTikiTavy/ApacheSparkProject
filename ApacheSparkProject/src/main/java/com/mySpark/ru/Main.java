package com.mySpark.ru;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main {

    private static final String PATH_TO_WINUTILS = "D:\\Spark\\";
    private static final String PATH_TO_CLIENTS_CSV = "data/clients.csv";
    private static final String PATH_TO_TRANSACTIONS_CSV = "data/transactions.csv";
    private static final String PATH_TO_TERMINAL_CSV = "data/terminals.csv";
    private static final String FIRST_TABLE_JOIN_COLUMN = "id";
    private static final String SECOND_TABLE_JOIN_COLUMN = "client_id";
    private static final String THIRD_TABLE_JOIN_COLUMN = "terminal_id";
    private static final String FOURTH_TABLE_JOIN_COLUMN = "terminal_id";
    private static final String PATH_TO_SAVE = "data/ResultTable.csv";

    public static void main(String[] args) {


        //Clean the trash messages
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        //Create an helping class
        Repository repository =
                new Repository(PATH_TO_WINUTILS, PATH_TO_CLIENTS_CSV, PATH_TO_TRANSACTIONS_CSV, PATH_TO_TERMINAL_CSV);

        //Combine two datasets into one
        Dataset<Row> resultTable = repository.SQLQueries(FIRST_TABLE_JOIN_COLUMN, SECOND_TABLE_JOIN_COLUMN,
                THIRD_TABLE_JOIN_COLUMN, FOURTH_TABLE_JOIN_COLUMN);


        //Save as csv
         SaveLoad.saveToCsv(resultTable, PATH_TO_SAVE);
    }
}
