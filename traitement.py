"""Programme PySpark du second PDF (atelier Spark Standalone), non exécuté ici.

Échantillon intégré dans l'énoncé du professeur. Requiert un Spark Standalone
configuré, ou un autre master passé via spark-submit.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg

spark = SparkSession.builder.appName("SDIA-Analyse-Ventes").getOrCreate()

data = [
    ("Casablanca", "PC", 5000),
    ("Rabat", "PC", 6000),
    ("Fes", "Clavier", 200),
    ("Marrakech", "PC", 4500),
    ("Casablanca", "Souris", 150),
    ("Rabat", "Ecran", 2200),
    ("Fes", "PC", 4200),
    ("Marrakech", "Ecran", 1800),
    ("Casablanca", "PC", 5500),
    ("Rabat", "Souris", 100),
]

df = spark.createDataFrame(data, ["ville", "produit", "prix"]).repartition(4)
print("Nombre de partitions :", df.rdd.getNumPartitions())
print("Nombre de ventes :", df.count())
print("=== Chiffre d'affaires par ville ===")
df.groupBy("ville").agg(count("*").alias("nb_ventes"), spark_sum("prix").alias("total")).orderBy(col("total").desc()).show()
print("=== Exercice 4 : ventes par produit ===")
df.groupBy("produit").count().orderBy(col("count").desc()).show()
print("=== Exercice 4 : prix moyen par ville ===")
df.groupBy("ville").agg(avg("prix").alias("prix_moyen")).show()
print("=== Exercice 4 : produit au chiffre d'affaires maximum ===")
df.groupBy("produit").agg(spark_sum("prix").alias("ca")).orderBy(col("ca").desc()).limit(1).show()
print("=== Exercice 4 : ventes avec prix > 3000 ===")
df.filter(col("prix") > 3000).show()
spark.stop()
