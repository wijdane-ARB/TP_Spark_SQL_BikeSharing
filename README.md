# TP Spark SQL — Bike Sharing (Java, Spark 4.0.1)

**ENSET Mohammedia · Master SDA · Big Data**  
**Auteur : Wijdane Aarroub · Groupe 7 · Worker 1**

> **Jeu de données pédagogique SYNTHÉTIQUE** : le document `TP_Spark_SQL_en (2).pdf` décrit `bike_sharing.csv`, mais aucun CSV réel n'a été fourni. Le CSV inclus (60 locations) sert à exécuter et valider les requêtes ; ses chiffres ne doivent pas être présentés comme des résultats du dataset original du professeur.

## Objectif

Implémenter les **6 sections** de l'exercice Spark SQL de location de vélos :

| Partie | Travaux inclus |
|---|---|
| 1. Data Loading & Exploration | Charger CSV, afficher schéma, 5 lignes, compter les locations |
| 2. Temporary View | Créer `bike_rentals_view` |
| 3. Basic SQL Queries | Durée > 30 min ; Station A ; chiffre d'affaires total |
| 4. Aggregation Queries | Locations par station ; durée moyenne ; station la plus populaire |
| 5. Time-Based Analysis | Heure de départ ; fréquentation horaire ; station de 7 h à 12 h incluses |
| 6. User Behavior Analysis | Âge moyen ; utilisateurs distincts par genre ; groupes 18–30, 31–40, 41–50, 51+ |

Le fichier `DemoVentesProduits.java` reprend également le **code de démonstration de l'enseignant** (`ventes_produits.csv`, Casablanca, prix > 10000, tri, regroupement et SQL). Ce n'est pas l'exercice Bike Sharing.

## Architecture du projet

```text
TP_Spark_SQL_BikeSharing/
├── pom.xml
├── README.md
├── bike_sharing.csv              # DONNÉES SYNTHÉTIQUES
├── ventes_produits.csv           # Exemple pédagogique
├── .gitignore
├── src/main/java/ma/enset/
│   ├── Main.java                 # Exercices bike-sharing 1 à 6
│   └── DemoVentesProduits.java   # Exemple du professeur
└── docs/
    ├── GUIDE_ETAPES.md
    └── ATELIER_SPARK_STANDALONE.md
```

## Prérequis

- Windows / IntelliJ IDEA ; **JDK 17 ou 21** pour lancer Maven ; bytecode **Java 17**.
- Dépendance Maven `spark-sql_2.13:4.0.1` en portée `provided` (Spark est fourni par le lanceur à l'exécution).
- Oracle Cloud : Master `10.0.0.194` (Spark 4.0.1, Hadoop 3.4.2, Java 17) et Worker 1 `10.0.0.165` (Hadoop 3.5.0, Java 17).
- Ce projet est **distinct du TP RDD précédent**.

## 1. Ouvrir dans IntelliJ et exécuter localement

1. Extraire l'archive et **ouvrir le dossier qui contient `pom.xml`**.
2. Laisser IntelliJ importer Maven et choisir un JDK 17 ou 21.
3. Ouvrir `src/main/java/ma/enset/Main.java`.
4. Dans **Run → Edit Configurations → Main**, définir :
   - **VM options** : `-Dspark.master=local[*]`
   - **Working directory** : racine du projet `TP_Spark_SQL_BikeSharing` (là où se trouve `bike_sharing.csv`).
5. Exécuter `Main` et vérifier les 6 parties, puis prendre les captures demandées.
6. Exécuter `DemoVentesProduits` avec les mêmes réglages pour le code d'introduction du professeur.

**Important :** le code ne force pas `.master("local[*]")`. Le master se définit par IntelliJ en local et par `spark-submit --master ...` sur le cluster.

## 2. Compiler le projet (Maven)

Dans IntelliJ : **Maven → Lifecycle → package**. Le Maven système `mvn` n'est pas indispensable.

Ou si Maven est disponible dans votre terminal :

```powershell
mvn clean package -DskipTests
```

JAR attendu : `target/tp-spark-sql-bike-sharing-1.0-SNAPSHOT.jar`.

## 3. Copier les fichiers sur le Master OCI (PowerShell Windows)

La clé privée doit rester sur votre ordinateur et **ne jamais être ajoutée au dépôt**.

```powershell
$key = 'CHEMIN_DE_VOTRE_CLE_SSH_MASTER'  # A remplacer localement
$project = 'D:\TP_Spark_SQL_BikeSharing'  # Adapter au chemin exact

scp -i $key "$project\target\tp-spark-sql-bike-sharing-1.0-SNAPSHOT.jar" ubuntu@84.8.218.147:/home/ubuntu/
scp -i $key "$project\bike_sharing.csv" ubuntu@84.8.218.147:/home/ubuntu/
ssh -i $key ubuntu@84.8.218.147
```

## 4. Mettre les données dans HDFS (sur le Master)

```bash
hdfs dfs -mkdir -p /user/ubuntu/tp-spark-sql
hdfs dfs -put -f ~/bike_sharing.csv /user/ubuntu/tp-spark-sql/
hdfs dfs -ls /user/ubuntu/tp-spark-sql
hdfs dfs -cat /user/ubuntu/tp-spark-sql/bike_sharing.csv | head -n 6
```

## 5. Exécuter Spark SQL sur YARN (Master + Worker 1)

**Ce mode YARN est celui que nous avons déjà validé avec le TP Spark RDD. Il diffère du mode Spark Standalone du second PDF.**

```bash
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

/opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --class ma.enset.Main \
  --conf spark.driver.host=10.0.0.194 \
  --conf spark.driver.bindAddress=10.0.0.194 \
  --conf spark.driver.port=39000 \
  --conf spark.driver.blockManager.port=39001 \
  --conf spark.port.maxRetries=0 \
  --conf spark.executor.instances=1 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  /home/ubuntu/tp-spark-sql-bike-sharing-1.0-SNAPSHOT.jar \
  hdfs:///user/ubuntu/tp-spark-sql/bike_sharing.csv \
  2>&1 | tee ~/tp-spark-sql-yarn.log
```

Après exécution :

```bash
yarn application -list -appStates ALL
grep -E 'Total rentals:|total_revenue|FIN DU TP|hadoop-worker1|ERROR|Exception' ~/tp-spark-sql-yarn.log | tail -30
```

Avec **deux DataNodes et deux NodeManagers** précédemment validés, l'ordonnanceur YARN peut choisir n'importe quel nœud disponible. Pour attester l'utilisation effective de Worker 1, conserver les lignes d'exécuteur contenant `hadoop-worker1` (si elles apparaissent).

## 6. Résultats attendus sur les données synthétiques

Le fichier fourni contient **60 locations**. Les autres résultats dépendent de ses données. Ils doivent être vérifiés à l'exécution (ne pas supposer qu'ils correspondent au dataset original du professeur).

## 7. Deuxième atelier : Spark Standalone

Voir [`docs/ATELIER_SPARK_STANDALONE.md`](docs/ATELIER_SPARK_STANDALONE.md).

Le PDF `Spark_clutser.pdf` demande **1 Spark Master + 2 Spark Workers, le port 7077, l'interface 8080 et un traitement PySpark**, avec la version Spark **4.3.0** indiquée dans les consignes. Notre cluster existant utilise **Spark 4.0.1 sur Master, en mode YARN**. Ne pas prétendre que le TP Standalone est terminé : il faudra installer la même version de Spark sur les Workers, lancer les services Standalone, vérifier l'interface puis exécuter le traitement demandé. Worker 2 est géré par son propre responsable.

## Compte rendu

Captures à prévoir : projet et Maven, schéma/5 premières lignes, résultat des 6 parties, listing HDFS, statut YARN, lignes des exécuteurs et application réussie. Les scripts de ce projet ne modifient pas le cluster tant qu'ils ne sont pas soumis.

## Sécurité

Ne jamais publier de clé privée, jeton, fichier `.pem`/`.key`, archive de secrets, ni rendre publics les ports des services internes. Les IP citées sont les IP de démonstration du groupe à vérifier avant exécution.
