# Analyse e-commerce avec Spark et Scala

Ce projet transforme les quatre fichiers du groupe 3 en résultats métier : qualité des données, ventes par marchand, rétention des clients, segmentation et transactions suspectes. Ce guide part de zéro et explique les commandes ainsi que les choix à présenter à l'oral.

Groupe : **Fotsing Dominique** (A, parties 1, 2 et 7), **Faye Ibrahim** (B, partie 3), **Gadji Mahamat** (C, parties 4, 5 et 6). 

Dépôt privé : [ecommerce-analytics-groupe-3](https://github.com/Fotsingsouop/spark-ecommerce-project). Le guide [TRAVAIL_EN_GROUPE.md](docs/TRAVAIL_EN_GROUPE.md) explique les fichiers attribués et la publication des contributions avec l'identité de chacun.

Sur le poste où les outils portables sont déjà préparés, si `sbt` n'est pas reconnu, utilisez directement :

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sbt.ps1 compile


1. Commencer par comprendre le résultat
Une transaction représente un achat. Elle contient les identifiants du client, du produit et du marchand. Le programme lit les quatre fichiers, vérifie les valeurs, rapproche les tables, puis calcule les statistiques. Spark répartit ce travail en partitions. Sur votre ordinateur, plusieurs tâches tournent dans la même machine avec local[2]. Sur un cluster, elles peuvent tourner sur plusieurs machines.
Les données originales ont été copiées localement dans src/main/resources/data/. Le dépôt GitHub contient uniquement le code et la documentation : après un clone, ajoutez les données du devoir en suivant les instructions locales. Elles contiennent volontairement des anomalies. Les résultats vont dans output/. Une nouvelle exécution remplace les résultats portant le même nom. Pour conserver une expérience, choisissez un autre chemin de sortie dans la configuration.
Parcours conseillé : installation, tests, ingestion seule, pipeline complet, lecture des résultats, puis préparation de l'explication orale. Ne cherchez pas à mémoriser tout le code avant votre première exécution.
Des exemples issus d'une exécution réelle sont commentés dans RESULTATS_OBTENUS.md.
2. Versions et prérequis
Outil
Version du projet
Rôle
JDK
17, distribution Temurin possible
Exécuter la JVM
Scala
2.12.18
Langage du code
Apache Spark
3.5.7 compilé pour Scala 2.12
Moteur de calcul
SBT
1.10.7
Télécharger les bibliothèques, compiler et tester
sbt-assembly
2.3.1
Produire le JAR pour spark-submit

Prévoyez Internet au premier lancement, environ 4 Go de mémoire disponible et plusieurs Go de disque pour les outils et leurs caches. Le dossier Tools and software du cours contient un installateur Java 21 et une archive Spark 4.2.0 : ces versions ne correspondent pas à ce projet. Utilisez le couple ci-dessus. La documentation Spark 3.5.7 indique Java 8/11/17 et impose la même version binaire Scala entre Spark et l'application.
Sources officielles : compatibilité Spark, installation SBT, Temurin, archives Spark.
Installation Windows
Installez un JDK 17 depuis Temurin. Notez le dossier, par exemple C:\Program Files\Eclipse Adoptium\jdk-17....
Installez SBT depuis le site officiel. project/build.properties fixe la version utilisée par ce projet.
Ouvrez une nouvelle fenêtre PowerShell et vérifiez :



PowerShell
java -version
sbt --script-version


La première commande doit annoncer Java 17. Si nécessaire, définissez JAVA_HOME avec le chemin réellement installé, puis ajoutez son sous-dossier bin au PATH dans les variables d'environnement Windows. Vous pouvez aussi le faire pour la session courante :



PowerShell
$env:JAVA_HOME = 'C:\chemin\reel\vers\votre\jdk-17'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
java -version


SBT télécharge Scala et Spark : aucune installation séparée de Scala ou Spark n'est nécessaire pour sbt run. Pour pratiquer Scala dans la console du projet : sbt console, puis :quit pour sortir. Pour spark-submit, téléchargez et extrayez spark-3.5.7-bin-hadoop3.tgz depuis les archives Apache, définissez SPARK_HOME vers le dossier extrait et ajoutez %SPARK_HOME%\bin au PATH. Vérifiez spark-submit --version.
Hadoop sous Windows : Le projet configure automatiquement un accès local grâce à l'instruction programmatique System.setProperty("hadoop.home.dir", "C:\\hadoop"). Sur les environnements Windows, assurez-vous de placer les binaires natifs winutils.exe et hadoop.dll dans le dossier configuré (C:\hadoop\bin) afin de garantir la bonne gestion des permissions et des fichiers par Spark.
Alternative WSL Ubuntu pour un environnement Linux
Dans une distribution WSL Ubuntu déjà disponible : installez OpenJDK 17 et SBT selon leurs documentations officielles. Spark sera téléchargé par SBT. Copiez le dossier du projet dans votre dossier personnel Linux pour éviter les chemins Dropbox et bénéficier du système de fichiers Linux. Puis lancez les mêmes commandes sbt décrites ci-dessous. La configuration fournie utilise des chemins relatifs compatibles Windows et Linux.
Lanceur portable fourni
Si .tools/jdk-17... et .tools/sbt-launch.jar sont présents, vous pouvez remplacer chaque commande sbt par powershell -ExecutionPolicy Bypass -File .\scripts\sbt.ps1. Le lanceur utilise d'abord ce JDK portable, puis JAVA_HOME, puis le Java du PATH. Les outils portables sont exclus de Git et du rendu.
Pour télécharger uniquement le lanceur SBT si nécessaire :



PowerShell
New-Item -ItemType Directory -Force .tools
Invoke-WebRequest '[https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/1.10.7/sbt-launch-1.10.7.jar](https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/1.10.7/sbt-launch-1.10.7.jar)' -OutFile .tools/sbt-launch.jar


3. Ouvrir le bon dossier
Dans PowerShell :



PowerShell
Set-Location 'C:\Users\FOTSING\Dropbox (Personal)\PC\Master DIT\BigData_DataEngIneering_Scala\Devoir\EcommerceAnalytics'
Get-ChildItem


Vous devez voir build.sbt, README.md et src. Les chemins de application.conf sont relatifs à ce dossier. Placez-vous toujours ici avant une commande SBT.
4. Compiler et tester



PowerShell
sbt compile
sbt test


compile transforme les fichiers Scala en bytecode JVM. test vérifie le calendrier, les valeurs nulles, la conservation des rejets, les limites des fenêtres et la rétention. Les premières commandes peuvent durer plusieurs minutes pendant les téléchargements. Une fin avec [success] indique que SBT a terminé sans erreur.
Avec le lanceur portable :



PowerShell
powershell -ExecutionPolicy Bypass -File .\scripts\sbt.ps1 compile
powershell -ExecutionPolicy Bypass -File .\scripts\sbt.ps1 test


5. Première exécution : ingestion et qualité



PowerShell
sbt run


Lisez les messages de validation et le tableau récapitulatif Quality Report. Pour chaque table, vérifiez :



Plaintext
nb_lignes_lues = nb_lignes_valides + nb_lignes_rejetees


Un taux de rejet non nul est attendu sur ces fichiers. Chaque ligne rejetée ou validée est exportée en respectant rigoureusement son format d'origine (CSV pour les transactions/marchands, JSON pour les utilisateurs, Parquet pour les produits) sous output/rejected/ et output/valid/, avec affichage automatique des 5 premières lignes en console. Le motif décrit toutes les règles violées, séparées par ;.
Le rapport de qualité final est quant à lui généré sous forme de fichier CSV unique (via java.nio, s'affranchissant de la structure de répertoires fragmentée part-xxx de Spark).
6. Exécuter le projet complet



PowerShell
sbt run


Sans argument, sbt run exécute le pipeline unifié complet. Les étapes successives réalisent :
L'ingestion multi-format sécurisée.
La validation des données (application des seuils configurés, calcul des métriques et dénombrement des orphelins de clés étrangères).
L'export et l'aperçu des jeux de données valides et rejetés dans leurs formats respectifs.
La vérification de l'intégrité référentielle et l'écriture des rapports finaux propres.
7. Comprendre les sorties
Chaque résultat et jeu de données intermédiaire respecte le format d'origine du fichier source ou est agrégé dans un fichier unique propre :
Dossier ou Fichier sous output/
Format
Rôle
quality_report.csv
CSV (Fichier unique)
Bilan des quatre jeux de données, nulls et références absentes
output/rejected/
CSV, JSON, Parquet
Enregistrements invalides par dataset avec leur motif de rejet
output/valid/
CSV, JSON, Parquet
Jeux de données nettoyés après validation
merchant_kpis, cohort_retention, etc.
CSV / Parquet
Analyses métier et indicateurs de performance

8. Expliquer le code pas à pas
models/Transaction.scala, User.scala, ProductItem.scala et Merchant.scala définit la structure d'une ligne avec des case class. Option distingue une valeur manquante d'un zéro.
DataIngestion.scala lit les différents formats (CSV, JSON, Parquet) de manière robuste.
DataValidation.scala applique les règles métiers, sépare les lignes valides des rejets, calcule les métriques de qualité et prend en charge l'intégrité référentielle ainsi que l'écriture via Java NIO.
MainApp.scala orchestre l'ensemble du processus, gère les aperçus console (5 premières lignes) et ferme proprement la SparkSession dans un bloc finally.
9. Modifier la configuration
Le fichier actif est src/main/resources/application.conf. Modifiez par exemple les seuils d'âge, les montants minimaux ou les chemins d'accès selon vos besoins, puis relancez.
10. Dépannage
Message ou symptôme
Vérification
java ou sbt introuvable
Rouvrez PowerShell après installation et vérifiez le PATH
Java 21 au lieu de 17
Corrigez JAVA_HOME ou utilisez le JDK portable
Erreur d'accès aux fichiers locaux Windows
Assurez-vous que winutils.exe et hadoop.dll sont bien présents dans C:\hadoop\bin
AccessDeniedException sur quality_report
Vérifiez que le chemin de sortie pointe bien vers un fichier .csv et non vers un dossier bloquant

État des vérifications réellement effectuées : VERIFICATION.md.
