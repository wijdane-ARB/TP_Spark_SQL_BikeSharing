
package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {

        // =============================================
        // CONFIGURATION SPARK
        // =============================================

        SparkSession spark = SparkSession.builder()
                .appName("TP-Spark-SQL-BikeSharing")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        try {

            String csvPath = args.length > 0
                    ? args[0]
                    : "bike_sharing.csv";

            // =============================================
            // EXERCICE 1 : CHARGEMENT ET EXPLORATION
            // =============================================

            System.out.println(
                    "\n========== EXERCICE 1 : EXPLORATION =========="
            );

            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(csvPath);

            System.out.println("\n1. Schema du DataFrame :");
            df.printSchema();

            System.out.println("\n2. Les 5 premieres lignes :");
            df.show(5, false);

            System.out.println("\n3. Nombre total de locations :");
            System.out.println("Total : " + df.count());

            // =============================================
            // EXERCICE 2 : VUE TEMPORAIRE SQL
            // =============================================

            System.out.println(
                    "\n========== EXERCICE 2 : VUE TEMPORAIRE =========="
            );

            df.createOrReplaceTempView("bike_rentals_view");

            System.out.println(
                    "Vue temporaire bike_rentals_view creee."
            );

            // =============================================
            // DEMONSTRATION : VRAIES REQUETES SPARK SQL
            // =============================================

            System.out.println(
                    "\n========== REQUETES SPARK SQL =========="
            );

            System.out.println("\nSQL 1 : Nombre total de locations");

            spark.sql(
                    "SELECT COUNT(*) AS total_locations " +
                            "FROM bike_rentals_view"
            ).show();

            System.out.println("\nSQL 2 : Revenu total");

            spark.sql(
                    "SELECT ROUND(SUM(price), 2) AS revenu_total " +
                            "FROM bike_rentals_view"
            ).show();

            System.out.println("\nSQL 3 : Locations par station");

            spark.sql(
                    "SELECT start_station, " +
                            "COUNT(*) AS nb_locations " +
                            "FROM bike_rentals_view " +
                            "GROUP BY start_station " +
                            "ORDER BY nb_locations DESC, start_station"
            ).show();

            System.out.println("\nSQL 4 : Duree moyenne par station");

            spark.sql(
                    "SELECT start_station, " +
                            "ROUND(AVG(duration_minutes), 2) AS duree_moyenne " +
                            "FROM bike_rentals_view " +
                            "GROUP BY start_station " +
                            "ORDER BY start_station"
            ).show();

            // =============================================
            // EXERCICE 3 : REQUETES DE BASE
            // =============================================

            System.out.println(
                    "\n========== EXERCICE 3 : REQUETES DE BASE =========="
            );

            // Locations de plus de 30 minutes
            System.out.println("\n1. Locations > 30 minutes :");

            df.filter(
                    col("duration_minutes").gt(30)
            ).show(false);

            // Locations depuis Station A
            System.out.println("\n2. Locations depuis Station A :");

            df.filter(
                    col("start_station").equalTo("Station A")
            ).show(false);

            // Revenu total
            System.out.println("\n3. Revenu total :");

            df.agg(
                    round(sum("price"), 2)
                            .alias("total_revenue")
            ).show();

            // =============================================
            // EXERCICE 4 : AGREGATIONS
            // =============================================

            System.out.println(
                    "\n========== EXERCICE 4 : AGREGATIONS =========="
            );

            // Nombre de locations par station
            System.out.println("\n1. Locations par station :");

            Dataset<Row> stationCounts = df
                    .groupBy("start_station")
                    .count()
                    .withColumnRenamed(
                            "count",
                            "nombre_locations"
                    );

            stationCounts
                    .orderBy(
                            col("nombre_locations").desc(),
                            col("start_station").asc()
                    )
                    .show(false);

            // Duree moyenne par station
            System.out.println("\n2. Duree moyenne par station :");

            df.groupBy("start_station")
                    .agg(
                            round(
                                    avg("duration_minutes"), 2
                            ).alias("duree_moyenne")
                    )
                    .orderBy("start_station")
                    .show(false);

            // Station la plus populaire
            System.out.println("\n3. Station la plus populaire :");

            stationCounts
                    .orderBy(
                            col("nombre_locations").desc(),
                            col("start_station").asc()
                    )
                    .limit(1)
                    .show(false);

            // =============================================
            // EXERCICE 5 : ANALYSE TEMPORELLE
            // =============================================

            System.out.println(
                    "\n========== EXERCICE 5 : ANALYSE TEMPORELLE =========="
            );

            Dataset<Row> dfTime = df.withColumn(
                    "heure_debut",
                    hour(
                            to_timestamp(
                                    col("start_time"),
                                    "yyyy-MM-dd HH:mm:ss"
                            )
                    )
            );

            // Extraction de l'heure
            System.out.println("\n1. Heure de debut :");

            dfTime.select(
                    "rental_id",
                    "start_time",
                    "heure_debut"
            ).show(20, false);

            // Locations par heure
            System.out.println("\n2. Locations par heure :");

            dfTime.groupBy("heure_debut")
                    .count()
                    .withColumnRenamed(
                            "count",
                            "nombre_locations"
                    )
                    .orderBy(
                            col("nombre_locations").desc(),
                            col("heure_debut").asc()
                    )
                    .show(24, false);

            // Station la plus populaire le matin
            System.out.println(
                    "\n3. Station populaire le matin (7h-12h) :"
            );

            dfTime.filter(
                            col("heure_debut").between(7, 12)
                    )
                    .groupBy("start_station")
                    .count()
                    .withColumnRenamed(
                            "count",
                            "nombre_locations"
                    )
                    .orderBy(
                            col("nombre_locations").desc(),
                            col("start_station").asc()
                    )
                    .limit(1)
                    .show(false);

            // =============================================
            // EXERCICE 6 : COMPORTEMENT DES UTILISATEURS
            // =============================================

            System.out.println(
                    "\n========== EXERCICE 6 : UTILISATEURS =========="
            );

            // Age moyen
            System.out.println("\n1. Age moyen :");

            df.agg(
                    round(avg("age"), 2)
                            .alias("age_moyen")
            ).show();

            // Repartition par genre
            System.out.println("\n2. Locations par genre :");

            df.groupBy("gender")
                    .count()
                    .withColumnRenamed(
                            "count",
                            "nombre_locations"
                    )
                    .orderBy("gender")
                    .show(false);

            // Tranches d'age
            System.out.println("\n3. Locations par tranche d'age :");

            Dataset<Row> dfAges = df.withColumn(
                    "tranche_age",
                    when(
                            col("age").between(18, 30),
                            "18-30"
                    )
                            .when(
                                    col("age").between(31, 40),
                                    "31-40"
                            )
                            .when(
                                    col("age").between(41, 50),
                                    "41-50"
                            )
                            .when(
                                    col("age").geq(51),
                                    "51+"
                            )
                            .otherwise("Moins de 18 ans")
            );

            dfAges.groupBy("tranche_age")
                    .count()
                    .withColumnRenamed(
                            "count",
                            "nombre_locations"
                    )
                    .orderBy(
                            col("nombre_locations").desc(),
                            col("tranche_age").asc()
                    )
                    .show(false);

            // =============================================
            // FIN DU TP
            // =============================================

            System.out.println(
                    "\n============================================"
            );

            System.out.println(
                    "     TP SPARK SQL TERMINE AVEC SUCCES"
            );

            System.out.println(
                    "============================================"
            );

        } finally {
            spark.stop();
        }
    }
}
