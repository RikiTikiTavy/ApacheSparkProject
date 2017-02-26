package com.mySpark.ru;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.Callable;


public class Repository {

    private SparkSession spSession;
    private Dataset<Row> clients;
    private Dataset<Row> transactions;
    private Dataset<Row> terminal;
    private Dataset<Row> resultTable;

    private static final int requiredAgeOfCustomer = 35;
    private static final String BAR_MCC = "(mcc = 5812 OR mcc = 5814)";
    private static final String PHARMACY_MCC = "(mcc = 5912 OR mcc = 5122)";

    Repository(String pathToWinutils, String pathToClientsCsv, String pathToTransactionsCsv, String pathToTerminalCsv) {

        //Get a session from Connection
        this.spSession = Connection.getSession(pathToWinutils);
        this.clients = createDataset(pathToClientsCsv);
        this.transactions = createDataset(pathToTransactionsCsv);
        this.terminal = createDataset(pathToTerminalCsv);

    }

    public Dataset<Row> createDataset(String pathToCsv) {
        Dataset<Row> dataset = SaveLoad.loadFromCsv(spSession, pathToCsv);

        return dataset;
    }

    public Dataset<Row> joinDataset(Dataset<Row> one, String columnOne, Dataset<Row> two, String columnTwo) {

        Dataset<Row> mergedDataset = one
                .join(two, one.col(columnOne)
                        .equalTo(two.col(columnTwo)));

        return mergedDataset;
    }

    //Now we have a few datasets, lets's merge them into big one
    public Dataset<Row> mergeDatasetsIntoOne(String columnOne, String columnTwo, String columnThird, String columnFourth) {
        Dataset<Row> mergedDataset1 = joinDataset(this.clients, columnOne, this.transactions, columnTwo);

        Dataset<Row> mergedDataset2 = joinDataset(mergedDataset1, columnThird, this.terminal, columnFourth);
        return mergedDataset2;
    }

    //All the SQL queries here
    public Dataset<Row> SQLQueries(String firstTableJoinColumn, String secondTableJoinColumn,
                                   String thirdTableJoinColumn, String fourthTableJoinColumn) {
        Dataset<Row> clientTransactionsTerminal = mergeDatasetsIntoOne(firstTableJoinColumn, secondTableJoinColumn,
                thirdTableJoinColumn, fourthTableJoinColumn);

        clientTransactionsTerminal.createOrReplaceTempView("basicTable");

        String currentAgeOfCustomer = "(SELECT YEAR(CURRENT_DATE()) - YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(date_of_birth, 'yyyy-MM-dd') AS TIMESTAMP))) " +
                "FROM basicTable)";


        //Number of month between last transaction and current date
        String numOfMonth = "months_between(CURRENT_DATE(), TO_DATE(CAST(UNIX_TIMESTAMP(date, 'yyyy-MM-dd') AS TIMESTAMP)))";
        //Make some calculates for customers <= 35 years
        Dataset<Row> barCustomers = spSession.sql("SELECT client_id, x, y" +
                " FROM basicTable " +
                "WHERE " + currentAgeOfCustomer + " <= " + requiredAgeOfCustomer +
                " AND " + numOfMonth + "<= 2  " +
                "AND  " + BAR_MCC +
                " GROUP BY client_id, x, y " +
                "HAVING SUM(amount) > 2000 AND COUNT(*) > 5");

        //Make helping query to bypass the SUM(MAX(*)) condition
        String helpingQuery = "(SELECT client_id, x, y, COUNT(client_id) AS numberOfVisits " +
                "FROM basicTable " +
                "WHERE " + currentAgeOfCustomer + " > " + requiredAgeOfCustomer +
                " AND " + numOfMonth + " <= 6 " +
                "AND " + PHARMACY_MCC +
                " GROUP BY client_id, x, y ORDER BY client_id DESC)";

        //Customers after 35 years
        Dataset<Row> PharmacyCustomers = spSession.sql("SELECT client_id ,x, y " +
                "FROM " + helpingQuery + " AS t1 " +
                " join (SELECT client_id AS id, MAX(numberOfVisits) AS numOfVis FROM " + helpingQuery + " GROUP BY client_id ) AS t2 " +
                "on t1.client_id = t2.id AND t1.numberOfVisits = t2.numOfVis");

        // Union all the results into one table
        resultTable = PharmacyCustomers.union(barCustomers).orderBy("client_id");

        return resultTable;
    }
}
