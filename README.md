# TP Apache Spark SQL — Analyse d'un système de location de vélos

**ENSET Mohammedia · Master SDA · Big Data**  
**Réalisé par : Wijdane Aarroub — Groupe 7**  
**Statut : exécution locale et exécution distribuée sur YARN validées (14 novembre 2026)**

## Présentation

Ce projet Java/Maven répond aux **six parties** du TP Spark SQL sur l'analyse d'un système de location de vélos. L'application utilise les API **DataFrame** et **Spark SQL**, puis s'exécute localement dans IntelliJ IDEA et sur un cluster Hadoop **YARN** hébergé sur Oracle Cloud Infrastructure (OCI).

> **Données synthétiques.** Le sujet du TP décrit la structure de `bike_sharing.csv` sans fournir le jeu de données. Le CSV fourni avec ce projet contient **60 lignes générées à des fins pédagogiques**. Les résultats ci-dessous ne représentent **pas** des données réelles ni un fichier original de l'enseignant.

## Objectifs et fonctionnalités

| Partie | Réalisation |
|---|---|
| 1. Exploration | Charger le CSV ; `printSchema()` ; cinq premières lignes ; nombre de locations |
| 2. Vue temporaire | Créer `bike_rentals_view` avec `createOrReplaceTempView()` |
| 3. Requêtes simples | Durée > 30 min ; départ de Station A ; recette totale |
| 4. Agrégations | Locations par station ; durée moyenne ; station la plus populaire |
| 5. Analyse temporelle | Extraction de l'heure ; heures de pointe ; station la plus populaire de 7 h à 12 h incluses |
| 6. Comportement utilisateur | Âge moyen ; répartition des locations par genre ; tranches d'âge 18–30, 31–40, 41–50 et 51+ |

L'application finale inclut aussi des requêtes `spark.sql(...)` sur la vue temporaire afin de montrer le traitement SQL en complément de l'API DataFrame.

## Technologies

- **Java 17** (cible de compilation), **JDK 21** utilisé pour l'exécution IntelliJ sous Windows ; ne pas utiliser JDK 24 avec cette configuration.
- **Apache Spark 4.0.1**, module Maven `spark-sql_2.13`.
- **Maven**, **IntelliJ IDEA**, fichiers CSV.
- **HDFS** pour le stockage du dataset, **YARN** pour l'ordonnancement, **Oracle Cloud** pour les VM.

## Organisation du projet

```text
TP_Spark_SQL_BikeSharing/
├── pom.xml
├── README.md
├── bike_sharing.csv                 # Dataset synthétique (60 locations)
├── ventes_produits.csv              # Exemple d'introduction
├── src/main/java/ma/enset/
│   ├── Main.java                    # TP Bike Sharing
│   └── DemoVentesProduits.java      # Exemple de ventes du professeur
├── docs/
│   ├── GUIDE_ETAPES.md
│   └── ATELIER_SPARK_STANDALONE.md
└── target/                         # Généré par Maven, non versionné
```

Le fichier `DemoVentesProduits.java` constitue un **exemple pédagogique distinct** (ventes par ville, filtres sur les prix, tri et regroupement).

## Exécuter localement dans IntelliJ

1. Ouvrir le dossier contenant `pom.xml` et importer le projet Maven.
2. Configurer le SDK du projet et le **JRE de la configuration `Main` sur JDK 21** (cible de compilation : Java 17).
3. Pour un lancement direct avec IntelliJ, inclure les dépendances Spark au **classpath d'exécution**. Si la dépendance Maven a le scope `provided`, configurer l'IDE pour inclure ce scope ou retirer temporairement `provided` pour le test local.
4. Dans **Run → Edit Configurations → Main**, définir la **working directory** sur la racine contenant `bike_sharing.csv`.
5. Si le `Main.java` utilise une session sans `.master(...)`, saisir dans les **VM options** :

   ```text
   -Dspark.master=local[*]
   ```

6. Lancer `ma.enset.Main` ; vérifier le schéma, les six parties, les requêtes SQL, puis `Process finished with exit code 0`.

**Dépannage IntelliJ :** les avertissements `Unable to resolve table 'bike_rentals_view'` relèvent des inspections SQL de l'IDE : la vue n'existe qu'au moment de l'exécution Spark. Les avertissements Windows concernant `winutils.exe` peuvent apparaître sans empêcher le TP de s'exécuter. Les erreurs `NoClassDefFoundError` indiquent un problème de classpath ; l'erreur `getSubject is not supported` a été corrigée en passant de JDK 24 à JDK 21.

## Construire le JAR

Dans PowerShell, à la racine du projet :

```powershell
mvn clean package -DskipTests
Test-Path '.\target\tp-spark-sql-bike-sharing-1.0-SNAPSHOT.jar'
```

**Résultat validé :** compilation Maven réussie et `Test-Path` renvoyant `True`.

Pour un JAR destiné à `spark-submit`, conserver la dépendance Spark en scope `provided` afin que Spark fourni par le cluster soit utilisé. Le build crée :

```text
target/tp-spark-sql-bike-sharing-1.0-SNAPSHOT.jar
```

## Infrastructure OCI utilisée

| VM | IP privée | Rôle dans cet exercice |
|---|---|---|
| Master | `10.0.0.194` | Client `spark-submit`, driver en mode client, HDFS et ResourceManager YARN |
| Worker 1 | `10.0.0.165` | DataNode et NodeManager YARN disponibles pour les conteneurs |

L'application est soumise **à YARN**. Le statut YARN confirme la réussite de l'application ; il ne prouve pas à lui seul sur quel Worker les executors ont été placés.

## Transférer le JAR et le dataset sur le Master

Les commandes suivantes sont à lancer depuis **PowerShell Windows** ; remplacer le chemin de clé par celui de votre propre machine. **Ne jamais publier la clé privée SSH.**

```powershell
$key = 'CHEMIN_LOCAL_VERS_LA_CLE_SSH_MASTER'
$project = 'D:\TP_Spark_SQL_BikeSharing'

scp -i $key "$project\target\tp-spark-sql-bike-sharing-1.0-SNAPSHOT.jar" ubuntu@84.8.218.147:/home/ubuntu/
scp -i $key "$project\bike_sharing.csv" ubuntu@84.8.218.147:/home/ubuntu/
ssh -i $key ubuntu@84.8.218.147
```

**Vérifié :** les deux transferts SCP se sont terminés avec `100 %`.

## Stocker le CSV dans HDFS

Sur la VM Master :

```bash
hdfs dfs -mkdir -p /user/ubuntu/tp-spark-sql
hdfs dfs -put -f /home/ubuntu/bike_sharing.csv /user/ubuntu/tp-spark-sql/
hdfs dfs -ls /user/ubuntu/tp-spark-sql/
```

**Vérifié :** le fichier `/user/ubuntu/tp-spark-sql/bike_sharing.csv` apparaît dans HDFS, taille **4 950 octets**.

## Exécuter l'application sur YARN

Sur la VM Master :

```bash
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
  hdfs:///user/ubuntu/tp-spark-sql/bike_sharing.csv
```

Cette commande utilise les **ports du driver déjà employés dans le TP Spark RDD**, sans modifier les règles réseau du cluster.

Vérifier le résultat YARN :

```bash
yarn application -list -appStates ALL
```

**Résultat observé le 14/11/2026 :** application `TP-Spark-SQL-BikeSharing`, **`FINISHED`**, **`SUCCEEDED`**, avancement **100 %**. Les résultats des six sections se sont affichés sur le terminal Master.

## Résultats validés — dataset synthétique

| Indicateur | Résultat |
|---|---:|
| Nombre total de locations | **60** |
| Recette totale | **469,08** (unités du dataset, dollars selon l'énoncé) |
| Station la plus populaire | **Station C — 13 locations** |
| Durée moyenne à Station E | **40,08 min** |
| Heure de pointe | **08 h — 10 locations** |
| Station la plus populaire entre 7 h et 12 h | **Station D — 8 locations** |
| Âge moyen | **37,5 ans** |
| Locations effectuées par des femmes | **31** |
| Locations effectuées par des hommes | **29** |
| Tranche d'âge la plus représentée | **18–30 ans — 20 locations** |

**Remarque :** les lignes par genre et âge comptent des **locations**, pas nécessairement des utilisateurs uniques. Les résultats du CSV synthétique ne doivent pas être généralisés à des usagers réels.

## Compte rendu et preuves

Le compte rendu final en français couvre : environnement local, projet Java et compilation Maven, vue temporaire SQL, six exercices, transfert SCP, HDFS, exécution sur OCI/YARN et contrôle `FINISHED / SUCCEEDED`.

Fichier produit séparément : `Compte_Rendu_TP_Spark_SQL_BikeSharing_Wijdane_Aarroub.pdf`. Il **n'est pas automatiquement présent dans ce dépôt** ; l'ajouter dans `docs/` uniquement si souhaité.

Les captures réalisées sont celles de l'exécution locale et du terminal Master. Conserver notamment le schéma, les analyses statistiques, la commande HDFS et le statut final YARN.

## Travail distinct : Spark Standalone

Le document `Spark_clutser.pdf` demande **un autre atelier** : un Master Spark Standalone et **deux Workers** connectés au port `7077`, supervisés via l'interface Web `8080`, avec un traitement PySpark. **Ce travail n'est pas attesté par l'exécution YARN ci-dessus.** Voir `docs/ATELIER_SPARK_STANDALONE.md` pour le guide de préparation.

## Sécurité et bonnes pratiques

- Ne jamais versionner les clés `.key`/`.pem`, secrets, fichiers de configuration privés ou identifiants cloud.
- Garder les interfaces et ports internes Spark/Hadoop accessibles uniquement via réseau privé ou tunnel SSH.
- Ne pas inclure le dossier `target/` dans Git ; reconstruire les artefacts avec Maven.
- Relever les preuves réelles d'exécution ; ne pas annoncer une exécution sur Worker 1 sans journal ou vue YARN confirmant l'Executor.
