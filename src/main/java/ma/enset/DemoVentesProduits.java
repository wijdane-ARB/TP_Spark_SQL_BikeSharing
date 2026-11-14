package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/** Exemple du professeur, conserve separement du TP bike_sharing. */
public class DemoVentesProduits {
    public static void main(String[] args) {
        String input = args.length > 0 ? args[0] : "ventes_produits.csv";
        SparkSession spark = SparkSession.builder()
                .appName("Demo-Spark-SQL-Ventes")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            Dataset<Row> ventes = spark.read().option("header", "true")
                    .option("inferSchema", "true").csv(input);
            ventes.createOrReplaceTempView("ventes");
            ventes.printSchema();
            ventes.show();
            ventes.where("ville = 'Casablanca'").show();
            ventes.where("prix > 10000").show();
            ventes.orderBy(col("prix").asc()).limit(10).show();
            ventes.groupBy(col("ville")).count().show();
            spark.sql("SELECT ville, COUNT(*) AS Nombre FROM ventes GROUP BY ville").show();
        } finally {
            spark.stop();
        }
    }
}
