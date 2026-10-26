# Journal de contribution – Groupe 6

## 1\. Récapitulatif : question, responsable, relecteur

|Question|Contenu|Responsable|Relecteur|
|-|-|-|-|
|Partie 0|Organisation, EQUIPE.md, Git, CONTRIBUTIONS.md|Collectif|Collectif|
|Partie 1|Structure SBT, build.sbt, README|A (Dominique)|C (Mahamat)|
|Partie 2 (2.1 à 2.5)|Ingestion, validation, rapport de qualité, intégrité référentielle (bonus 2.5 traité)|A (Dominique)|B (Ibrahima)|
|Partie 3 (3.1 à 3.4)|UDF temporelle, enrichissement, fenêtres, transactions suspectes (bonus 3.4 traité)|B (Ibrahima)|A (Dominique)|
|Partie 4 (4.1 et 4.2)|KPI marchands, rétention par cohortes (reprise par Ibrahima le 26/10/2026, la partie de Mahamat n'ayant pas été livrée)|B (Ibrahima), initialement C (Mahamat)|A (Dominique)|
|Partie 5 (5.1 à 5.3)|Cache, broadcast, mesures avant/après|C (Mahamat)|A (Dominique) et B (Ibrahima)|
|Partie 6 (6.1 et 6.2)|Application principale, mesure des durées|C (Mahamat)|Intégration validée par A, B et C|
|Partie 7|application.conf|A (Dominique)|C (Mahamat)|
|Partie 8|Livrables et soutenance|Collectif|Collectif|

## 2\. Charge de travail et difficultés

### Membre A – Dominique

* Charge estimée : 30 heures
* Difficultés : 
Problèmes d'E/S et de verrouillage de fichiers (Windows / Dropbox) : Tous mes travaux sont toujours sauvergardés sous Dropbox, l'exécution de Spark sur un environnement Windows synchronisé avec Dropbox a provoqué des échecs de renommage atomique dans les répertoires temporaires (IOException: Could not rename file dans les dossiers _temporary), ainsi que des exceptions d'accès refusé (AccessDeniedException) lors de l'écriture des fichiers JSON. Cela a nécessité l'adaptation des méthodes d'export via java.nio et un nettoyage des répertoires cibles.   
Restructuration des packages et des chemins : Initialement j'ai mal nommé un repertoire, le changement de nom du répertoire contenant la configuration (passant de config à utils) a cassé les importations de packages, générant des erreurs de compilation qu'il a fallu corriger dans l'ensemble des fichiers du projet (comme MainApp.scala et DataValidation.scala).   
Centralisation et lecture dynamique de la configuration Hadoop : Le déplacement du paramètre hadoop.home.dir depuis le code en dur vers le fichier de configuration (application.conf) a nécessité d'ajuster la structure des classes SparkConfig et AppConfig pour éviter des erreurs de types et de membres non trouvés à la compilation.   

### Membre B – Ibrahima

* Charge estimée : 27 heures
* Difficultés : installation de Spark et de sbt sous Windows (réseau d'entreprise, versions de Java) ; compatibilité des versions Scala (2.12 contre 2.13) avec `spark-submit` ; affichage des accents dans la console Windows (problème d'affichage uniquement, les données sont correctes) ; dates invalides (700 timestamps inexploitables) : l'UDF renvoie un `Option`, donc une valeur nulle au lieu d'une erreur, avec un parsing strict (le 31 février est refusé) ; compréhension des fenêtres glissantes (`rangeBetween` en secondes pour de vrais 7 jours) et contournement de `countDistinct`, interdit dans une fenêtre, par `size(collect\_set(...))` ; ambiguïtés du sujet (âge de 25 ans, bornes de `is\_working\_hours`) ; alignement des modèles de données et de la configuration des trois membres sur une base commune ; nettoyage du dépôt Git partagé (`.git` de près de 200 Mo à moins de 4 Mo) ; collaboration à distance avec des fuseaux horaires différents.

### Membre C – Mahamat

* Charge estimée : <à compléter> heures
* Difficultés : <à compléter>

## 3\. Décisions techniques du groupe

1. **Versions : Scala 2.12.18 et Spark 3.5.0.** Scala 2.12 est la version des distributions Spark 3.5 (`spark-shell`, `spark-submit`) : un JAR compilé en 2.13 ne serait pas compatible avec un Spark installé localement. sbt télécharge les dépendances, donc le projet se compile sans installation de Spark. Le JAR a été testé avec `spark-submit` sur Spark 3.5.9.
2. **JAR exécutable avec `sbt-assembly`, Spark en `provided`.** Spark est fourni par `spark-submit` à l'exécution, donc le JAR n'embarque pas Spark : il pèse environ 6 Mo au lieu de 230 Mo, ce qui permet de l'envoyer par e-mail et évite deux copies de Spark (3.5.0 compilée, 3.5.9 installée). Les fichiers de données (`src/main/resources/data`) sont aussi exclus du JAR, car le programme les lit sur le disque d'après `application.conf`. Les fichiers `META-INF` en conflit sont écartés et `reference.conf` est concaténé (Typesafe Config). Pour que `sbt run` fonctionne malgré `provided`, `build.sbt` remet Spark sur le classpath de `run` et `runMain`.
3. **Configuration externalisée avec valeurs par défaut.** Chemins, seuils de validation, paramètres Spark et options d'optimisation sont dans `application.conf`. `AppConfig` renvoie une valeur par défaut quand une clé est absente (`hasPath`), donc le programme ne plante pas pour un oubli. Le cache et le broadcast peuvent être forcés par les variables d'environnement `ENABLE\_CACHE` et `ENABLE\_BROADCAST` (substitution optionnelle `${?VARIABLE}`), ce qui a permis de mesurer les durées avec et sans optimisation sans modifier le fichier.
4. **Règle de validation : toutes les règles violées sont notées dans `rejection\_reason`** (motifs séparés par « ; »). Une valeur nulle compte comme une violation. Les lignes rejetées sont gardées dans un DataFrame, ce qui permet d'expliquer chaque rejet. Les bornes (par exemple une note entre 1 et 5) viennent de la configuration.
5. **Format de sortie du rapport de qualité : un seul fichier CSV** (`output/rapport\_qualite.csv`), écrit sans dossier `part-xxx` et sans dépendre de Hadoop. Il contient les trois compteurs d'orphelins (bonus 2.5) sur la ligne des transactions.
6. **Transactions orphelines : comptées dans le rapport mais conservées.** Dans l'enrichissement, les jointures sont de type `left`, avec les transactions à gauche, pour les utilisateurs, produits et marchands. Les transactions sont les faits à analyser : un `inner` supprimerait les ventes dont la référence est absente (400 `user\_id`, 300 `product\_id` et 250 `merchant\_id` orphelins). Avec `left`, ces ventes restent, avec des valeurs `null` sur les colonnes de référence (tranche d'âge « Inconnu »).
7. **Tranches d'âge : 25 ans range dans « Adulte » (25 à 44).** Le sujet définit « moins de 25 » et « 26 à 44 », ce qui laisse l'âge 25 sans tranche.
8. **`is\_working\_hours` : 1 pour 9h ≤ heure < 17h.** Le sujet dit « entre 9h et 17h » ; 17h00 est la fin de la journée de travail. Les noms de jours et de mois sont en anglais, comme `day\_period`. Les heures de 22h à 5h59 sont classées « Night ».
9. **Fenêtre glissante de 7 jours : de (t − 7 jours) à t, transaction courante incluse**, la même pour le montant cumulé et pour l'utilisateur actif (jours distincts via `size(collect\_set(date))`, car `countDistinct` est interdit dans une fenêtre). Le délai entre achats utilise `lag` et `datediff` (en jours entiers) ; il est `null` pour la première transaction d'un utilisateur.
10. **Panier moyen d'un utilisateur : moyenne de toutes ses transactions validées** (la transaction courante comprise). Une transaction est suspecte quand au moins 2 des 4 conditions sont réunies (montant supérieur à 4 fois le panier moyen, période « Night », délai inférieur à 5 minutes, paiement `CRYPTO`). Ces seuils sont lus dans `application.conf` (bloc `app.transformation`, valeurs par défaut dans le code) ; l'affichage du top 20 indique le nombre de conditions et les raisons.
11. **KPI marchands : seuls les marchands connus sont analysés.** Les transactions dont le marchand est orphelin (250) n'ont ni catégorie ni région ni commission : elles sont exclues du calcul des KPI, mais restent dans les transactions enrichies. Le résultat compte 586 marchands, soit les marchands valides.
12. **Rétention par cohortes : revenu moyen par utilisateur calculé sur la taille initiale de la cohorte.** `avg\_revenue\_per\_user` est le revenu de la période divisé par la taille de la cohorte, et non par le nombre d'actifs de la période, ce qui rend les périodes comparables entre elles. La cohorte d'un utilisateur est le mois de sa première transaction ; `period\_index` compte les mois écoulés depuis ce mois.
13. **Cache : `persist(MEMORY\_AND\_DISK\_SER)` sur le DataFrame enrichi, `cache()` sur les petits résultats.** Le DataFrame enrichi est le plus gros et il est réutilisé pour les KPI, les cohortes et deux écritures. La sérialisation et le repli sur disque évitent de saturer la mémoire d'un poste local. Tous les caches sont libérés (`unpersist`) en fin d'exécution, dans le bloc `finally`, avec l'arrêt de la session Spark.
14. **Broadcast : hint explicite sur les petites tables de référence** (utilisateurs, produits, marchands) lors de l'enrichissement, activable par configuration. Sur ces données, Spark diffuse déjà ces tables automatiquement (moins de 10 Mo) : le gain mesuré est donc faible, mais le hint garde le choix explicite si les volumes augmentent.
15. **Écriture des résultats : CSV et Parquet, un fichier par format** (`coalesce(1)`) dans `output/<résultat>/csv` et `output/<résultat>/parquet`. Les volumes sont petits ; un seul fichier simplifie la lecture et la démonstration.

## 4\. Mesures d'optimisation (question 5.3)

Médianes de 3 exécutions par mode, même session, poste Windows 11, Spark 3.5.0 en mode local, 8 partitions de shuffle.

|Étape|Sans optimisation|Avec cache + broadcast|Gain|
|-|-|-|-|
|Ingestion + validation|12,54 s|14,25 s|−13,6 % (bruit de mesure)|
|Transformation|5,13 s|10,99 s|−114 % (coût de matérialisation du cache)|
|Analytique|12,96 s|7,68 s|+40,7 %|
|Écriture CSV + Parquet|30,26 s|5,70 s|+81,2 %|
|**Total**|**64,75 s**|**38,68 s**|**+40,3 %**|

Le cache est un investissement : la transformation est plus lente, puis le gain apparaît dans l'analytique et surtout l'écriture, où le DataFrame enrichi est sinon recalculé à chaque action. La variabilité d'une exécution à l'autre atteint environ 20 %, d'où l'usage de la médiane.

## 5\. Relectures croisées

|Date|Module relu|Auteur|Relecteur|Remarques|
|-|-|-|-|-|
|20/10/2026|Partie 2 (première version : ingestion, validation, rapport)|Dominique|Ibrahima|Code exécuté sous Windows (Java 11, sbt 1.9.8) : les quatre sources se chargent avec les bons volumes (600, 138 047, 12 000, 6 000). Corrections proposées : erreur `$"coalesce"(...)` remplacée par `coalesce(...)` ; lignes rejetées conservées avec leur motif (DataFrame) ; âge en `Int` au lieu de `BigInt` ; ajout du comptage des nulles.|
|21/10/2026|Partie 2 (deuxième version)|Dominique|Ibrahima|Version exécutée avec les mêmes chiffres de validation que mes calculs de contrôle (586, 136 157, 11 655 valides pour merchants, transactions, users). Corrections proposées pour respecter le sujet : placement des fichiers dans `analytics`, `models` et `utils` ; valeur par défaut des clés de configuration (7.1) ; note entre 1 et 5 (et non 0 à 5) ; try-catch par fichier avec nombre de lignes lues (2.3) ; colonne `dataset` du rapport (2.4) ; rapport écrit en un seul fichier CSV ; trois compteurs d'orphelins ajoutés au rapport (2.5) ; suppression du chemin `C:\\hadoop` écrit en dur ; passage de `reference.conf` en `concat` dans la configuration d'assembly.|
|26/10/2026|Partie 3 (UDF, enrichissement, fenêtres, suspectes)|Ibrahima|Dominique|Code relu et jugé « excellent » avec trois points à revoir. (1) Seuils de `detectSuspicious` (facteur 4 du panier moyen, 300 secondes) en dur : déplacés dans `application.conf`, bloc `app.transformation`, avec valeurs par défaut dans le code. (2) Mode de paiement autre que `CRYPTO` dans le top 20 : ce n'est pas une erreur, une transaction est suspecte avec 2 conditions sur 4 et `CRYPTO` n'en est qu'une ; colonnes `nb_conditions` et `suspicion_raisons` ajoutées à l'affichage. (3) Catégorie de marchand `null` après le `LEFT JOIN` : affichée `INCONNU` dans le top 20 (affichage seulement, les données gardent `null`). Corrections dans le commit cd89c6c.|
|<à compléter>|Partie 4 (KPI marchands, cohortes)|Ibrahima (reprise de la partie de Mahamat)|Dominique|<à compléter par Dominique>|
|<à compléter>|Partie 5 (optimisations)|Mahamat|Dominique et Ibrahima|<à compléter>|
|<à compléter>|Partie 1 et Partie 7 (build.sbt, README, configuration)|Dominique|Mahamat|<à compléter par Mahamat>|
|<à compléter>|Partie 6 (application principale)|Mahamat|Dominique, Ibrahima, Mahamat|<à compléter>|

## 6\. Historique Git de Ibrahima (partie 3, puis partie 4 reprise)

|Commit|Contenu|Date|
|-|-|-|
|5eacdbf|Ajout de l'UDF extractTimeFeatures (question 3.1)|20/10/2026|
|4ef0546|Ajout de enrichTransactionData : jointures, UDF, rang et tranche d'age (question 3.2)|20/10/2026|
|27df720|Ajout des fenetres glissantes 7 jours, utilisateur actif et delai entre achats (question 3.3)|21/10/2026|
|bc07cf4|Ajout de la detection des transactions suspectes (bonus 3.4)|21/10/2026|
|b31c3a4|Ajout du broadcast optionnel dans les jointures d'enrichissement (question 5.2)|21/10/2026|
|c274764|Ajout de l'affichage du top 20 des transactions suspectes (question 3.4)|22/10/2026|
|347274a|Partie 3 : DataTransformation autonome et point d'entree TransformationDemo|24/10/2026|
|e7cafe6|TransformationDemo : adaptation au package utils|24/10/2026|
|cd89c6c|Partie 3 : corrections suite a la relecture de Dominique (seuils dans application.conf, raisons de suspicion, categorie INCONNU a l'affichage)|26/10/2026|
|6c593e9|Partie 4 : Analytics (rapport par marchand, cohortes de retention) et AnalyticsDemo|26/10/2026|

Les commits de la partie 3 ne touchent que `TimeFeatures.scala`, `DataTransformation.scala`, `TransformationDemo.scala` et `application.conf` (bloc des seuils) ; le dernier commit ajoute `Analytics.scala` et `AnalyticsDemo.scala` (partie 4). Le 24/10/2026, les huit premiers commits ont été rejoués par-dessus l'historique du dépôt partagé (`git cherry-pick`), ce qui explique leurs identifiants différents de ceux du journal précédent ; leur auteur, leur date et leur message sont conservés.

## 7\. Résultats de contrôle (données validées)

**Partie 2 (validation)**

* Lignes lues : merchants 600, transactions 138 047, users 12 000, products 6 000.
* Lignes valides : 586, 136 157, 11 655 et 5 821. Valeurs nulles : 9, 940, 160 et 55.
* Orphelins dans les transactions : 400 `user\_id`, 300 `product\_id`, 250 `merchant\_id`.

**Partie 3 (transformation)**

* UDF : 137 347 timestamps lisibles et 700 structures nulles (140 vides, 136 « NA », 424 formats incorrects).
* Enrichissement : 136 157 transactions avant et après les jointures ; tranches d'âge : Jeune 18 457, Adulte 59 833, Âge Moyen 46 571, Senior 7 023, Inconnu 4 273.
* Fenêtres : 1 355 transactions avec `is\_active\_user = 1` et 10 906 premières transactions (délai nul).
* Transactions suspectes : 2 999, dont les plus gros montants sont 28 279,41, 28 156,15 et 24 894,33. Le top 20 est affiché par la méthode `showTopSuspicious` : 19 des 20 transactions sont de nuit et payées en `CRYPTO`.
* Exécution de l'étape `transformation` depuis le JAR (`spark-submit`, Spark 3.5.9) le 22/10/2026 : 136 157 transactions enrichies, 2 999 suspectes, durée totale de 22 à 27 s.

**Partie 4 (analytique)**

* KPI : 586 marchands, 133 024 transactions rattachées à un marchand connu, chiffre d'affaires de 52 592 586,01 et commission d'environ 2 743 507,69. Meilleur marchand : M00225, 592 105,25.
* Cohortes : 300 lignes (24 cohortes) ; meilleure rétention à 3 mois : cohorte 2025-06-01, 122 actifs sur 337, soit 36,2 %.

**Exécution du JAR**

* `sbt assembly` génère `EcommerceAnalytics-assembly-1.0.0.jar` (environ 6 Mo).
* `spark-submit --class com.ecommerce.analytics.MainApp --master "local\[\*]" ... all` (Spark 3.5.9, Windows 11, Java 11) redonne exactement les chiffres ci-dessus, en environ 37 s. Les erreurs `Failed to delete ... .jar` affichées en fin d'exécution sous Windows concernent le nettoyage du dossier temporaire de Spark et n'affectent pas les résultats.

