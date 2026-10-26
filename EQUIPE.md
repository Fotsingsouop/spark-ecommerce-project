# Équipe – Groupe 6

Projet final Spark & Scala : système d'analyse de données e-commerce.

## Membres et rôles

| Rôle | Nom et prénom | E-mail | Nom Git (`git config user.name`) |
|---|---|---|---|
| Membre A – Data Ingestion & Platform Engineer | FOTSING SOUOP Dominique | dominique.fotsing@mail.dit.sn | Fotsing Souop |
| Membre B – Data Transformation Engineer | FAYE Ibrahima | westafricafarmsitofficer@gmail.com | Ibrahima Faye |
| Membre C – Analytics & Performance Engineer | FAYE Ibrahima (partie 4) / FOTSING SOUOP Dominique (parties 5 et 6) | westafricafarmsitofficer@gmail.com / dominique.fotsing@mail.dit.sn | Ibrahima Faye / Fotsing Souop |

Note : GADJI Mahamat, initialement membre C, n'a pas participé au projet. Ses parties 4, 5 et 6 ont été reprises le 26/10/2026 par Ibrahima (partie 4) et Dominique (parties 5 et 6), qui forment donc le membre C.

## Questions traitées par chacun

### Membre A – Dominique (Parties 1, 2 et 7)
- Partie 1 : 1.1 (structure SBT, case classes), 1.2 (`build.sbt`), 1.3 (`README.md`)
- Partie 2 : 2.1 (ingestion multi-format), 2.2 (validation), 2.3 (gestion d'erreurs), 2.4 (rapport de qualité), 2.5 (bonus, intégrité référentielle)
- Partie 7 : 7.1 (`application.conf`)

### Membre B – Ibrahima (Partie 3)
- 3.1 : UDF `extractTimeFeatures` (`TimeFeatures.scala`)
- 3.2 : `enrichTransactionData` (`DataTransformation.scala`)
- 3.3 : fonctions de fenêtrage (montant cumulé sur 7 jours, utilisateur actif, délai entre achats)
- 3.4 (bonus) : détection de transactions suspectes

### Membre C – Ibrahima / Dominique (Parties 4, 5 et 6, reprises de Mahamat)
- Partie 4 (Ibrahima) : 4.1 (KPI par marchand), 4.2 (cohortes et rétention)
- Partie 5 (Dominique) : 5.1 (cache, `persist`, `unpersist`), 5.2 (broadcast, partitions de shuffle), 5.3 (bonus, mesure avant/après)
- Partie 6 (Dominique) : 6.1 (application principale), 6.2 (bonus, exécution par étape)

## Parties collectives
- Partie 0 : organisation, `EQUIPE.md`, Git, `CONTRIBUTIONS.md`
- Partie 8 : livrables et soutenance

## Relecteurs
| Module | Auteur | Relecteur |
|---|---|---|
| Parties 1 et 7 | A (Dominique) | B (Ibrahima) |
| Partie 2 | A (Dominique) | B (Ibrahima) |
| Partie 3 | B (Ibrahima) | A (Dominique) |
| Partie 4 | C (Ibrahima) | A (Dominique) |
| Partie 5 | C (Dominique) | B (Ibrahima) |
| Partie 6 | C (Dominique) | B (Ibrahima), intégration validée par Dominique et Ibrahima |

Le détail des relectures (dates, remarques) est dans `CONTRIBUTIONS.md`.
