# Guide pas à pas et captures — TP Spark SQL

## Phase A — En local (à faire d'abord)

1. **Dézipper** le projet dans `D:\TP_Spark_SQL_BikeSharing` (ou un dossier de votre choix). Ouvrir dans IntelliJ le dossier avec `pom.xml`.
2. Charger Maven (icône `m`). Vérifier JDK 17/21 et le `pom.xml` (Spark SQL 4.0.1, compilation ciblée 17).
3. Ouvrir `Main.java` → Run → Edit Configurations : `VM options = -Dspark.master=local[*]`, `Working directory` = racine du projet.
4. Lancer `Main.java` et **ne pas interpréter les données comme des données réelles** : `bike_sharing.csv` est un échantillon synthétique fourni avec ce projet.
5. Captures : **C1** arborescence IntelliJ et Maven ; **C2** `printSchema()` + 5 lignes ; **C3** nombre total, durée >30 min, Station A, recette ; **C4** agrégations par station ; **C5** heures et fréquentation du matin ; **C6** statistiques des utilisateurs et groupes d'âge.
6. Lancer `DemoVentesProduits.java` séparément, avec les mêmes VM options ; **C7** : filtres Casablanca, prix > 10000 et agrégation SQL.
7. Maven → Lifecycle → `package` ; vérifier `BUILD SUCCESS` ; **C8** JAR dans `target`.

## Phase B — Cluster Oracle (après le local)

8. Depuis PowerShell Windows : `scp` du JAR et de `bike_sharing.csv` (commandes exactes dans le README). **C9** : transfert 100%.
9. Depuis Master : créer `/user/ubuntu/tp-spark-sql`, puis `hdfs dfs -put` et `hdfs dfs -ls` ; **C10** : listing HDFS.
10. Depuis Master : commande `spark-submit --master yarn` du README ; **C11** : résultats des requêtes et exécuteur Worker 1 si observé.
11. Depuis Master : `yarn application -list -appStates ALL` ; **C12** : statut `FINISHED / SUCCEEDED` de la nouvelle application.

## Phase C — Atelier Spark Standalone séparé

12. Suivre `ATELIER_SPARK_STANDALONE.md` seulement avec accord du groupe pour les changements de services et de pare-feu. Ne pas confondre **YARN** (déjà réalisé pour RDD) et **Standalone** (nouvel atelier).
13. Pour le TP Standalone : capturer Spark Master (`jps`, 7077), Worker 1 (`jps`, 8081), interface web privée via tunnel SSH, le script `traitement.py`, les résultats, et une capture de la panne contrôlée si la classe l'exige.
