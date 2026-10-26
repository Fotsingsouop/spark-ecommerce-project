# Pipeline e-commerce avec Spark et Scala

Ce projet lit des données e-commerce aux formats CSV, JSON et Parquet, contrôle leur qualité, enrichit les transactions et analyse les comportements d'achat. Il comprend également des indicateurs par marchand et une analyse de rétention par cohortes.

**Dossier du projet final :**

**Dépôt :** [Fotsingsouop/spark-ecommerce-project](https://github.com/Fotsingsouop/spark-ecommerce-project).

La répartition des responsabilités figure dans [EQUIPE.md](EQUIPE.md). Les contributions, difficultés, décisions et relectures sont consignées dans [CONTRIBUTIONS.md](CONTRIBUTIONS.md). Ce README reprend le document Word fourni et adapte les chemins, versions et commandes aux fichiers présents dans ce dossier.

## 1. Architecture du projet

```text
spark-ecommerce-project/
├── build.sbt
├── EQUIPE.md
├── CONTRIBUTIONS.md
├── README.md
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── src/main/resources/
│   ├── application.conf
│   └── data/
│       ├── transactions.csv
│       ├── users.json
│       ├── merchants.csv
│       └── products.parquet/
└── src/main/scala/com/ecommerce/
    ├── models/
    │   └── Models.scala
    ├── utils/
    │   └── AppConfig.scala
    └── analytics/
        ├── MainApp.scala
        ├── DataIngestion.scala
        ├── DataValidation.scala
        ├── TimeFeatures.scala
        ├── DataTransformation.scala
        ├── TransformationDemo.scala
        ├── Analytics.scala
        └── AnalyticsDemo.scala
```

Cette arborescence décrit les **emplacements physiques actuels**. Dans le code Scala, `MainApp.scala` déclare le package `com.ecommerce`, tandis que `DataIngestion.scala` et `DataValidation.scala` déclarent `com.ecommerce.ingestion`. Pour les commandes `runMain` et `spark-submit`, utilisez le nom du package déclaré, comme dans les exemples ci-dessous.

`Models.scala` centralise les modèles `Transaction`, `User`, `ProductItem`, `Merchant`, `ValidationResult` et `QualityMetrics`. Les champs numériques facultatifs utilisent `Option` afin de distinguer une valeur manquante d'une valeur égale à zéro.

## 2. Fonctionnalités et choix techniques

### Ingestion multi-format

- Transactions : lecture CSV avec un schéma explicite et le mode `PERMISSIVE`.
- Utilisateurs : lecture JSON, conversion de l'âge et du revenu, prise en charge de la liste `preferred_categories`.
- Produits : lecture du répertoire Parquet et conversion des colonnes numériques.
- Marchands : lecture CSV avec inférence du schéma.

Les chemins d'entrée proviennent de `application.conf` et sont relatifs au dossier depuis lequel l'application est lancée. `products.parquet` est un répertoire : conservez tous ses fichiers de données.

Les jeux de données du devoir restent locaux. Après un nouveau clone, copiez les quatre sources remises par l'enseignant dans `src/main/resources/data/`, ou adaptez leurs chemins dans la configuration. Ils ne doivent pas être publiés dans GitHub.

### Validation et qualité

Les règles principales portent sur les identifiants obligatoires, les montants positifs, la longueur des horodatages, l'âge, les revenus, les prix, les notes et les taux de commission. Chaque ligne rejetée conserve une colonne `rejection_reason` ; plusieurs motifs sont séparés par `;`.

Le rapport de qualité présente, pour chaque source :

- les nombres de lignes lues, valides et rejetées ;
- le taux de rejet, arrondi à deux décimales ;
- le nombre de cellules nulles des colonnes de premier niveau ;
- pour les transactions, les trois compteurs de références absentes : utilisateur, produit et marchand.

Les compteurs d'orphelins du rapport sont calculés par comparaison avec les référentiels bruts. Un contrôle distinct compare ensuite les transactions et référentiels validés et affiche les motifs `ORPHAN_USER_ID`, `ORPHAN_PRODUCT_ID` et `ORPHAN_MERCHANT_ID`.

### Enrichissement et détection de comportements

L'UDF `extractTimeFeatures` extrait l'heure, le jour de la semaine, le mois, le weekend, la période de journée et les heures ouvrées. Son parsing est strict ; une entrée incorrecte produit une structure nulle au lieu de faire échouer l'UDF.

Les jointures d'enrichissement sont de type `left` : une transaction validée est conservée même si une référence est absente ou invalide. Les informations correspondantes restent nulles et la tranche d'âge devient `Inconnu` si l'âge est absent.

Les fenêtres par utilisateur calculent le rang de l'achat, le nombre total de transactions, le montant sur sept jours glissants, le nombre de dates distinctes d'achat et le délai avec l'achat précédent. La fenêtre inclut les deux bornes `[t − 7 jours, t]`. Le délai exprimé par `datediff` est un nombre de jours calendaires entiers.

Une transaction est signalée comme suspecte lorsqu'au moins deux conditions sont réunies : montant élevé, achat de nuit, achat rapproché ou paiement `CRYPTO`. Dans cette version, le panier moyen utilise toutes les transactions validées de l'utilisateur, y compris l'achat courant. Le facteur par défaut est 4, soit un écart supérieur à 300 % par rapport à cette moyenne. Cet indicateur sert au signalement et ne constitue pas une preuve de fraude.

### Analytique métier

`Analytics.scala` calcule les KPI par marchand : chiffre d'affaires, transactions, clients uniques, panier moyen, commissions, ventes par tranche d'âge, taux de suspicion et classements par catégorie et région. Les KPI retiennent les transactions dont la catégorie du marchand de référence est renseignée.

L'analyse de cohortes regroupe les utilisateurs selon leur mois de premier achat, puis calcule les clients actifs, la rétention et le chiffre d'affaires par période. Le revenu moyen par utilisateur utilise la taille initiale de la cohorte. `period_index = 3` désigne trois mois après le mois de première transaction.

## 3. Versions et prérequis

Les versions ci-dessous sont celles de `build.sbt` et de `project/` dans le projet final.

| Outil | Version | Rôle |
|---|---|---|
| JDK conseillé pour ce parcours | 17 | Compiler et exécuter le code sur la JVM |
| Scala | 2.12.18 | Langage du projet, téléchargé par SBT |
| Apache Spark | 3.5.0 | Moteur de traitement, téléchargé par SBT |
| SBT | 1.9.7 | Compilation, exécution et tests |
| sbt-assembly | 2.1.5 | Génération du JAR |
| Typesafe Config | 1.4.3 | Lecture de la configuration |
| ScalaTest | 3.2.17 | Dépendance de test déclarée |

Installez Java et SBT, puis ouvrez une nouvelle fenêtre PowerShell :

```powershell
java -version
sbt --script-version
```

SBT télécharge Scala et les dépendances Spark lors du premier lancement. Une installation séparée de Spark est nécessaire pour utiliser la commande `spark-submit`, mais pas pour les commandes SBT du projet. Pour ce dossier final, les exemples utilisent **SBT installé et accessible dans le PATH**.



### Configuration Hadoop sous Windows

Le `build.sbt` actuel fixe `java.library.path` à `C:\hadoop\bin`. Si votre environnement Windows nécessite les bibliothèques natives Hadoop, placez dans ce dossier les binaires compatibles `winutils.exe` et `hadoop.dll` fournis ou validés pour votre environnement Spark.

Dans la session PowerShell utilisée pour lancer le programme :

```powershell
$env:HADOOP_HOME = 'C:\Hadoop'
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"
Test-Path "$env:HADOOP_HOME\bin\hadoop.dll"
```

`MainApp` contient aussi un réglage de `hadoop.home.dir` à partir de `AppConfig`. Toutefois, la lecture actuelle de cette clé dans `AppConfig.scala` doit être vérifiée : après `getConfig("app")`, elle recherche encore `app.spark.hadoop-home-dir` au lieu de `spark.hadoop-home-dir`. Ne comptez donc pas uniquement sur la modification de cette clé ; la variable `HADOOP_HOME` ci-dessus permet de fournir le chemin à Hadoop.

Les CSV, JSON et le rapport qualité sont écrits avec Java NIO. Les sorties Parquet utilisent toujours l'écriture Spark et peuvent rencontrer les contraintes de permissions ou de verrouillage Windows/Dropbox. En cas de verrouillage persistant, exécutez une copie du projet dans un dossier local hors synchronisation.

## 4. Ouvrir le dossier final et compiler

```powershell
Set-Location 'C:\Users\FOTSING\Dropbox (Personal)\PC\Master DIT\BigData_DataEngIneering_Scala\Projet\spark-ecommerce-project'
Get-ChildItem
sbt compile
```

Vous devez voir `build.sbt`, `project/`, `src/`, `EQUIPE.md` et `CONTRIBUTIONS.md`. Gardez ce dossier comme répertoire courant afin que les chemins relatifs d'entrée et de sortie restent corrects.

La commande suivante lance les tests disponibles
```powershell
sbt test
```

La présence de ScalaTest dans les dépendances ne garantit pas l'existence de tests : 

## 5. Exécuter le pipeline et ses modules

Plusieurs objets possèdent une méthode `main`. La commande explicite évite le menu de sélection que `sbt run` peut afficher.

### Ingestion, validation et rapport de qualité

```powershell
sbt "runMain com.ecommerce.MainApp ingestion"
```

Cette commande lit les sources, affiche leurs cinq premières lignes, applique les validations, exporte les jeux valides et rejetés, génère le rapport qualité et affiche le contrôle d'intégrité référentielle.

### Pipeline principal

```powershell
sbt "runMain com.ecommerce.MainApp all"
```

Le mode `all`, utilisé par défaut sans argument, exécute l'ingestion, la validation, les exports, l'enrichissement et la détection de transactions suspectes. **Les KPI marchands et les cohortes se lancent séparément avec `AnalyticsDemo`** dans la version actuelle.

```powershell
sbt "runMain com.ecommerce.MainApp"
```

### Autres modes de MainApp

| Mode | Commande | Résultat principal |
|---|---|---|
| Transformation | `sbt "runMain com.ecommerce.MainApp"` | Chargement des données, validation, qualité avec les exports complets des jeux valides et rejetés  |
| Détection de suspicion | `sbt "runMain com.AnalyticsDemo"` | Fenêtres de comportement et dénombrement |
| Rapport seul | `sbt "runMain com.ecommerce.TransformationDemo"` | Rapport d'enrichissement et nombre de transactions enrichies |

### Démonstrations dédiées

Partie 3, enrichissement, fenêtres et suspicion :

```powershell
sbt "runMain com.ecommerce.analytics.TransformationDemo"
```

Partie 4, KPI marchands et cohortes :

```powershell
sbt "runMain com.ecommerce.analytics.AnalyticsDemo"
```

Les deux démonstrations relisent et valident les sources avant leurs calculs. Elles affichent des durées globales et leurs résultats en console.

## 6. Comprendre les fichiers de sortie

Avec les chemins actuels, le mode `ingestion` ou `all` de `MainApp` produit :

| Chemin sous `output/` | Format | Contenu |
|---|---|---|
| `quality_report.csv` | Fichier CSV unique | Rapport de qualité des quatre sources |
| `valid/transactions_valid.csv` | Fichier CSV unique | Transactions après validation de leurs propres champs |
| `valid/merchants_valid.csv` | Fichier CSV unique | Marchands valides |
| `valid/users_valid.json` | Fichier JSON par lignes | Utilisateurs valides |
| `valid/products_valid.parquet/` | Répertoire Parquet | Produits valides |
| `rejected/transactions_rejected.csv` | Fichier CSV unique | Transactions invalides avec motif |
| `rejected/merchants_rejected.csv` | Fichier CSV unique | Marchands invalides avec motif |
| `rejected/users_rejected.json` | Fichier JSON par lignes | Utilisateurs invalides avec motif |
| `rejected/products_rejected.parquet/` | Répertoire Parquet | Produits invalides avec motif |

Pour lire le rapport depuis PowerShell :

```powershell
Import-Csv .\output\quality_report.csv | Format-Table -AutoSize
```

Pour chaque source, contrôlez :

```text
nb_lignes_lues = nb_lignes_valides + nb_lignes_rejetees
```

Les fichiers texte écrits avec NIO évitent les répertoires `part-...` pour ces exports. Les sorties Parquet conservent la structure distribuée de Spark. Les écritures remplacent les fichiers ou répertoires cibles concernés ; 

## 7. Configuration externalisée

Le fichier actif est [src/main/resources/application.conf](src/main/resources/application.conf).

```hocon
app {
  spark {
    app-name = "Ecommerce-Spark-Pipeline-Groupe3"
    master = "local[*]"
    shuffle-partitions = 8
    hadoop-home-dir = "C:\\Hadoop"
  }

  optimizations {
    enable-cache = true
    enable-broadcast = true
  }

  transformation {
    amount-multiplier = 4.0
    fast-purchase-seconds = 300
    min-conditions = 2
    window-days = 7
    active-days-min = 5
  }
}
```

Cet extrait présente les paramètres Spark, d'optimisation et de transformation ; le fichier complet contient aussi `app.paths` et `app.validation` et doit conserver ces sections.

| Clé | Utilisation |
|---|---|
| `app.paths.transactions`, `users`, `products`, `merchants` | Emplacements des quatre sources |
| `app.paths.output.quality-report` | Chemin du fichier CSV de qualité |
| `app.validation.*` | Seuils des contrôles de données |
| `app.spark.shuffle-partitions` | Nombre de partitions de shuffle |
| `app.optimizations.enable-broadcast` | Activation des hints de broadcast dans l'enrichissement |
| `app.optimizations.enable-cache` | Cache optionnel dans les démonstrations |
| `app.transformation.*` | Paramètres des fenêtres et de détection de suspicion |

Les chemins des exports `output/valid/` et `output/rejected/` sont encore écrits directement dans `MainApp.scala`. Modifier seulement `app.paths.output.base-dir` ne déplace donc pas tous les exports. De même, `MainApp` utilise directement le cache des sources ; le commutateur de cache n'y désactive pas toutes les mises en mémoire.

## 8. Optimisations et mesures de performance

Le pipeline utilise `persist(StorageLevel.MEMORY_AND_DISK_SER)` pour les transactions et `cache()` pour les référentiels. Les démonstrations peuvent mettre en cache le DataFrame enrichi. Les jointures disposent de hints `broadcast()` sur les petites tables de référence, et le nombre de partitions de shuffle est configurable. Le code libère les caches en fin de traitement normal et ferme Spark dans `finally`.

Les CSV et JSON sont collectés dans la mémoire du driver avant leur écriture NIO. Cette solution répond aux volumes du devoir et aux contraintes de fichiers locaux ; elle doit être revue pour de très gros volumes. Les CSV sont construits par concaténation : des champs contenant des virgules, guillemets ou sauts de ligne demanderaient une gestion d'échappement supplémentaire.

### Mesure du gain apporté par les optimisations

Les durées ci-dessous proviennent de deux series d'exécutions, avant et après optimisation sur 138 000 transactions. 
Les réductions de durée suivantes ont été constatées :

| Étape | Sans optimisation | Avec optimisation | Réduction de durée |
|---|---:|---:|---:|
| Ingestion multi-format | 48,42 s | 42,00 s | 13,26 % |
| Transformation | 56,87 s | 47,00 s | 17,36 % |
| Analytique | 41,65 s | 27,35 s | 34,33 % |
| Écriture et rapports | 9,20 s | 6,50 s | 29,35 % |
| **Somme des étapes** | **156,14 s** | **122,85 s** | **21,32 %** |

NB: Les élements suivants influencent la mesure des gains avant et après optimisation: la machine (hardware), les versions, les actions Spark chronométrées, les options actives et le nombre de répétitions. 

## 9. Générer le JAR et utiliser spark-submit

```powershell
sbt assembly
Get-ChildItem .\target\scala-2.12\*assembly*.jar
```

Avec le nom et la version déclarés dans ce projet, le nom attendu par défaut est `spark-ecommerce-project-assembly-1.0.0.jar`. Vérifiez le nom réellement généré avec la commande ci-dessus.

```powershell
spark-submit --class com.ecommerce.MainApp --master "local[*]" .\target\scala-2.12\spark-ecommerce-project-assembly-1.0.0.jar all
```

Pour lancer la démonstration analytique depuis le JAR :

```powershell
spark-submit --class com.ecommerce.analytics.AnalyticsDemo --master "local[*]" .\target\scala-2.12\spark-ecommerce-project-assembly-1.0.0.jar
```

Le `build.sbt` actuel déclare Spark comme une dépendance ordinaire, sans `provided` : l'assembly peut donc embarquer Spark et être volumineux. Il ne faut pas lui attribuer la taille d'un JAR léger d'une autre configuration. Les points d'entrée fixent aussi le master depuis `application.conf` ; adaptez cette configuration avant un déploiement sur cluster et utilisez des chemins accessibles aux exécuteurs.

## 10. Lecture du code

1. `models/Models.scala` : structure des données, valeurs facultatives et métriques.
2. `analytics/DataIngestion.scala` : lecture et typage des sources, package déclaré `com.ecommerce.ingestion`.
3. `analytics/DataValidation.scala` : règles métier, rejets, rapport qualité, références et exports NIO/Spark.
4. `analytics/TimeFeatures.scala` : fonction temporelle pure et UDF sécurisée.
5. `analytics/DataTransformation.scala` : jointures, fenêtres et conditions de suspicion.
6. `analytics/Analytics.scala` : indicateurs marchands et cohortes.
7. `utils/AppConfig.scala` : chargement de la configuration.
8. `analytics/MainApp.scala`, `TransformationDemo.scala` et `AnalyticsDemo.scala` : orchestration des parcours d'exécution.

## 11. Dépannage en cas d'erreur

| Symptôme | Vérification |
|---|---|
| `sbt` introuvable | Installez SBT, vérifiez le PATH et rouvrez PowerShell |
| Menu proposant plusieurs classes principales | Utilisez les commandes `runMain` explicites de ce README |
| Classe principale introuvable | MainApp se lance sous `com.ecommerce.MainApp` ; les démonstrations sont dans `com.ecommerce.analytics` |
| Fichier d'entrée introuvable | Vérifiez le répertoire courant et les chemins de `application.conf` |
| `winutils`, `HADOOP_HOME` ou erreur `NativeIO` | Vérifiez les binaires Hadoop, `HADOOP_HOME` et le chemin de DLL configuré dans `build.sbt` |
| Fichier utilisé par un autre processus | Vérifiez les fichiers ouverts et la synchronisation Dropbox, puis essayez dans un dossier local hors synchronisation |
| Aucun fichier de KPI ou de cohortes après la démonstration | Ces résultats sont actuellement affichés en console, sans export dans ces points d'entrée |
| SBT termine mais le journal contient `[ERROR]` ou `[ERREUR]` | Les points d'entrée capturent les exceptions ; lisez leur contenu et les résultats plutôt que le seul statut final de SBT |
