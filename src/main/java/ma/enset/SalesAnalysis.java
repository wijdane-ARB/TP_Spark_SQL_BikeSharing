package ma.enset;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SalesAnalysis {

    public static void main(String[] args) {

        // Initialisation de Spark
        SparkSession spark = SparkSession.builder()
                .appName("TP-Spark-SQL-SalesAnalysis")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Chemin du dataset
        String inputPath = args.length > 0
                ? args[0]
                : "sales.csv";

        try {

            // Lecture du fichier CSV
            Dataset<Row> sales = spark.read()
                    .option("header", true)
                    .option("inferSchema", true)
                    .csv(inputPath);

            System.out.println(
                    "========== SCHEMA DU DATAFRAME =========="
            );

            sales.printSchema();

            System.out.println(
                    "========== DONNEES ORIGINALES =========="
            );

            sales.show(false);

            System.out.println(
                    "========== NOMBRE DE PRODUITS =========="
            );

            System.out.println(
                    "Total : " + sales.count()
            );

            // Calcul du total des ventes par ville
            Dataset<Row> result = sales
                    .groupBy("CITY")
                    .agg(
                            sum("PRICE").alias("TOTAL_SALES")
                    )
                    .orderBy(desc("TOTAL_SALES"));

            System.out.println(
                    "========== TOTAL DES VENTES PAR VILLE =========="
            );

            result.show(false);

            // Calcul du revenu total
            System.out.println(
                    "========== REVENU TOTAL =========="
            );

            sales.agg(
                    sum("PRICE").alias("TOTAL_GENERAL")
            ).show(false);

            System.out.println(
                    "========== TP SALES ANALYSIS TERMINE AVEC SUCCES =========="
            );

        } finally {
            spark.stop();
        }
    }
}
