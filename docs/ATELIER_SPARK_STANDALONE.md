# Atelier séparé — Spark Standalone sur Oracle Cloud

**Source de la consigne :** `Spark_clutser.pdf` (17 pages). Le document demande un **cluster Spark Standalone** à **1 Master + 2 Workers**, Spark **4.3.0**, Python 3, Java 17, interfaces 8080/8081 et protocole `spark://...:7077`. Cela **n'est pas équivalent** aux jobs précédents lancés avec `--master yarn` (Spark 4.0.1).

> **Attention** : Les étapes ci-dessous sont une feuille de route adaptée à Master (`10.0.0.194`) et Worker 1 (`10.0.0.165`). Elles ne sont PAS encore réalisées ou vérifiées. Le 2e Worker est géré par un autre membre du groupe. Ne pas effectuer les exercices d'arrêt de Worker 2 sans accord du groupe. Ne pas mettre à jour le lien `/opt/spark` avant d'avoir planifié l'impact sur les jobs YARN existants.

## A. Vérifications préalables, sans rien modifier

Sur Master ET Worker 1 :

```bash
hostname
hostname -I
java -version
python3 --version
free -h
df -h /opt
nproc
ls -l /opt/spark /opt/spark-4.3.0-bin-hadoop3 2>/dev/null
```

Sur Master uniquement :

```bash
sudo ss -lntp | grep -E ':(7077|8080)\b' || true
jps
```

**Ne pas présumer que Spark 4.3.0 est installé.** Le Master dispose de Spark 4.0.1 pour YARN ; il faudra harmoniser les versions sur les 3 VM pour le nouvel atelier, selon consigne du professeur.

## B. Installation Spark Standalone (après accord)

Sur **chacune** des 3 VM (avec le responsable de chaque machine), utiliser la version indiquée dans la consigne *après vérification de sa disponibilité réelle* :

```bash
cd /tmp
wget https://archive.apache.org/dist/spark/spark-4.3.0/spark-4.3.0-bin-hadoop3.tgz
wget https://archive.apache.org/dist/spark/spark-4.3.0/spark-4.3.0-bin-hadoop3.tgz.sha512
sha512sum -c spark-4.3.0-bin-hadoop3.tgz.sha512
sudo tar -xzf spark-4.3.0-bin-hadoop3.tgz -C /opt/
/opt/spark-4.3.0-bin-hadoop3/bin/spark-submit --version
```

**Si le téléchargement est indisponible ou si le checksum ne correspond pas : STOP, ne pas extraire.** Ne pas remplacer `/opt/spark` (utilisé par YARN) avant décision de l'équipe ; on peut utiliser le chemin versionné explicite pour le Standalone.

## C. Master Standalone (sur Master)

Configurer un nouveau `spark-env.sh` (conserver une copie de la configuration initiale si elle existe) sous `/opt/spark-4.3.0-bin-hadoop3/conf/` :

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_MASTER_HOST=10.0.0.194
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export PYSPARK_PYTHON=/usr/bin/python3
```

Ensuite, après les règles réseau privées convenues avec l'équipe :

```bash
/opt/spark-4.3.0-bin-hadoop3/sbin/start-master.sh
jps
sudo ss -lntp | grep ':7077'
curl -I http://127.0.0.1:8080
```

**Sécurité** : n'autoriser le TCP 7077 que depuis les adresses privées des Workers ; ouvrir les ports des interfaces web uniquement sur réseau privé ou via tunnel SSH. Ne pas exposer 7077/8080 à Internet.

## D. Worker 1 Standalone (sur Worker 1)

Dans le `spark-env.sh` de la **même version installée sur Worker 1** :

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_LOCAL_IP=10.0.0.165
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_WEBUI_PORT=8081
export PYSPARK_PYTHON=/usr/bin/python3
```

Puis :

```bash
/opt/spark-4.3.0-bin-hadoop3/sbin/start-worker.sh spark://10.0.0.194:7077
jps
```

Un processus Spark `Worker` devrait apparaître. Worker 2 suit la même procédure **avec sa propre IP**, sous la responsabilité de son administrateur. Sur le Master, vérifier l'interface Web : deux Workers doivent être « ALIVE » pour satisfaire l'énoncé intégral.

## E. Tunnel SSH pour consulter la Web UI depuis Windows

Avec la clé du Master sur votre ordinateur Windows :

```powershell
ssh -i 'CHEMIN_CLE_MASTER' -L 8080:127.0.0.1:8080 ubuntu@84.8.218.147
```

Navigateur Windows : `http://localhost:8080`. Conserver la connexion SSH ouverte. En cas de port local déjà utilisé, utiliser `-L 18080:127.0.0.1:8080` puis `http://localhost:18080`.

## F. Programme PySpark distribué demandé par le PDF

Le code complet se trouve dans `traitement.py` à la racine du projet. Copiez ce fichier sur le Master avant exécution, par exemple via `scp` depuis Windows. Une fois Spark Standalone **réellement validé**, l'exécuter sur le Master avec la version harmonisée :

```bash
/opt/spark-4.3.0-bin-hadoop3/bin/spark-submit \
  --master spark://10.0.0.194:7077 \
  --deploy-mode client \
  --conf spark.driver.host=10.0.0.194 \
  --conf spark.driver.bindAddress=10.0.0.194 \
  --conf spark.driver.port=39000 \
  --conf spark.driver.blockManager.port=39001 \
  --total-executor-cores 2 \
  --executor-memory 1g \
  traitement.py
```

Le PDF emploie les ports 37001/37002 pour le Driver et le BlockManager ; les ports 39000/39001 ci-dessus sont une **adaptation** correspondant aux règles déjà appliquées lors du TP YARN, à revalider pour Spark Standalone.

Exercices restant à documenter : ressources du Master/Workers, rôle des composants, capture de l'interface, traitement PySpark, modification des ressources d'un Worker, panne contrôlée de Worker 2, et analyses DataFrame complémentaires. Ne pas perturber la VM Worker 2 sans coordination.

## G. Questions orales essentielles

- **Master Spark Standalone** : enregistre les Workers et attribue des ressources ; en YARN, c'est le ResourceManager qui les gère.
- **Worker Spark** : fournit des cœurs CPU et de la RAM et héberge des exécuteurs.
- **IP privées** : limitent le trafic applicatif au réseau interne OCI.
- **Port 7077** : port de communication du Master Spark Standalone (ce n'est pas la Web UI).
- **Limiter la mémoire** : empêcher qu'une application monopolise les ressources et déstabilise la VM.
- **Panne Worker** : les tâches peuvent être replanifiées sur un Worker disponible, selon la capacité et les garanties de tolérance aux pannes ; avec un seul Worker, le cluster conserve une capacité réduite.
