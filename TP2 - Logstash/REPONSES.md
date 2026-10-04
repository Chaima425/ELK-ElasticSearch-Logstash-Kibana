# TP2 — Réponses et compte rendu

## 1. Mise en place de Logstash

### 1.1 Rôle et utilisateur Logstash

Un rôle Elasticsearch dédié `logstash_writer` a été créé avec des privilèges limités aux index utilisés par Logstash :

* `offres`
* `logs-web-*`

Les privilèges accordés sont :

* `create`
* `create_index`
* `write`
* `view_index_metadata`

Le compte `logstash_internal` utilise ce rôle pour l'ingestion des données.

Les mots de passe ne sont pas stockés dans les fichiers de configuration versionnés. Ils sont fournis par les variables d'environnement définies dans `.env`.

Le fichier `.env` est exclu du dépôt Git et ne doit pas être versionné.

---

## 1.2 Pipeline `offres`

Le pipeline `offres` lit le fichier NDJSON contenant les offres avec le plugin `file`.

La configuration utilise notamment :

```text
mode => "read"
sincedb_path => "/dev/null"
codec => "json"
```

Le codec JSON permet de transformer chaque ligne du fichier NDJSON en événement Logstash.

### Suppression des champs parasites

Avant l'envoi vers Elasticsearch, les champs ajoutés automatiquement par Logstash sont supprimés :

```text
@timestamp
@version
event
log
host
```

Cela permet de conserver une structure proche du document métier d'origine et d'éviter l'ajout de champs inutiles dans l'index `offres`.

### Identifiant métier

Le champ `id` de l'offre est utilisé comme identifiant Elasticsearch :

```text
document_id => "%{id}"
```

Ainsi, par exemple, l'offre `OFF-00001` est stockée avec :

```text
_id = OFF-00001
```

### Idempotence

L'utilisation de l'identifiant métier permet de rendre la réingestion des offres idempotente.

Si la même offre est relue, Elasticsearch utilise le même `_id` et le document existant est remplacé au lieu de créer une nouvelle offre avec un identifiant différent.

Cela évite la multiplication des doublons lors d'une nouvelle ingestion du fichier `offres.ndjson`.

---

# 2. Superviser et fiabiliser Logstash

## 2.1 Supervision des pipelines

Le fichier `pipelines.yml` définit deux pipelines indépendants :

1. `offres`
2. `web`

Cette séparation permet de traiter séparément les offres et les logs web.

Le pipeline `offres` fonctionne avec :

* **8 workers**
* taille de batch : **125**
* délai de batch : **50 ms**

Les statistiques observées depuis le dernier démarrage du pipeline `offres` sont :

* `in` : **5000**
* `filtered` : **5000**
* `out` : **5000**

Cela signifie que 5000 événements sont entrés dans le pipeline, ont traversé le traitement et ont été envoyés en sortie.

Le plugin ayant consommé le plus de temps est le plugin de sortie Elasticsearch :

* durée du filtre `mutate` : environ **1100 ms**
* durée de sortie Elasticsearch : environ **16801 ms**

La majeure partie du temps est donc consacrée à l'envoi des documents vers Elasticsearch.

L'output Elasticsearch indique également :

* 5000 documents traités avec succès ;
* 41 réponses bulk HTTP 200.

---

## 2.2 Dead Letter Queue

La Dead Letter Queue (DLQ) a été activée dans la configuration de Logstash avec :

```text
DEAD_LETTER_QUEUE_ENABLE=true
```

Un document de test contenant un champ supplémentaire `prime` a été envoyé vers l'index `offres`.

L'index `offres` utilise un mapping strict. Le champ `prime`, qui n'est pas autorisé par ce mapping, provoque donc une erreur de mapping.

Le document de test `OFF-99999` n'est pas présent dans l'index `offres`.

Il est placé dans la Dead Letter Queue afin de ne pas perdre silencieusement l'événement rejeté.

### Intérêt de la DLQ

La DLQ permet de conserver les événements qui n'ont pas pu être correctement traités ou indexés.

Elle permet ensuite :

1. d'identifier la cause du rejet ;
2. de corriger le problème ;
3. de réinjecter les événements corrigés.

### Correction et réinjection

La procédure générale est :

1. identifier la cause de l'erreur dans les métadonnées de la DLQ ;
2. corriger la donnée ou le mapping responsable du rejet ;
3. relire les événements de la DLQ et les réinjecter dans Elasticsearch.

Cela permet d'éviter de perdre les événements simplement parce qu'une erreur de mapping ou de traitement s'est produite.

---

## 2.3 Rôle de `pipelines.yml`

Sans `pipelines.yml`, Logstash utiliserait son pipeline `main` et les fichiers `.conf` présents dans le répertoire de configuration pourraient être regroupés dans ce même pipeline.

Cela poserait un problème important dans ce TP car les deux flux sont différents :

* `offres.conf` traite des fichiers NDJSON d'offres ;
* `web.conf` traite des logs HTTP Apache.

Les séparer permet notamment :

* d'éviter de mélanger les traitements des deux flux ;
* d'avoir une supervision indépendante ;
* de redémarrer ou modifier un pipeline sans perturber inutilement l'autre ;
* de faciliter le diagnostic d'une erreur ;
* de pouvoir faire évoluer séparément les performances et la configuration de chaque flux.

---

## 2.4 Queue mémoire et Queue persistante

La configuration actuelle utilise une queue mémoire.

En cas d'arrêt brutal de Logstash pendant la lecture d'un gros fichier, les événements encore présents uniquement en mémoire peuvent être perdus.

Une queue persistante peut être activée avec :

```text
queue.type: persisted
```

Elle permet de conserver les événements sur disque afin qu'ils puissent être repris après un redémarrage.

Elle améliore donc la garantie de livraison des événements, notamment dans une logique de traitement **au moins une fois**.

Dans ce contexte, l'utilisation d'un identifiant métier comme `_id` reste importante : si un événement est traité une deuxième fois après un redémarrage, le même identifiant permet d'éviter la création de doublons dans l'index `offres`.

---

# 3. Pipeline des logs web

## 3.1 Lecture des logs

Le fichier `data/access.log` est lu par le pipeline `web`.

Le plugin `file` utilise :

```text
mode => "read"
sincedb_path => "/dev/null"
```

Les événements sont ensuite envoyés vers le data stream :

```text
logs-web-default
```

---

## 3.2 Parsing des logs

Le filtre :

```text
COMBINEDAPACHELOG
```

permet notamment d'extraire des informations telles que :

* adresse IP du client ;
* identifiant utilisateur éventuel ;
* timestamp ;
* méthode HTTP ;
* URL demandée ;
* code de réponse HTTP ;
* taille de la réponse ;
* referer ;
* User-Agent.

Le champ `http.response.status_code` est un champ numérique.

Le timestamp présent dans le log doit néanmoins être traité explicitement : il est d'abord extrait du texte et doit ensuite être converti en véritable champ de date Elasticsearch avec le filtre `date`.

La configuration utilise :

```text
match => ["timestamp", "dd/MMM/yyyy:HH:mm:ss Z"]
```

Le champ `timestamp` est ensuite supprimé.

---

## 3.3 Extraction de l'identifiant d'offre

Pour les URLs de la forme :

```text
/offres/OFF-01468
```

une expression régulière personnalisée permet d'extraire l'identifiant de l'offre dans :

```text
labels.offre_id
```

Par exemple :

```text
/offres/OFF-01468
```

donne :

```text
labels.offre_id = OFF-01468
```

Cela permet ensuite d'effectuer des agrégations Elasticsearch sur les offres consultées.

---

## 3.4 Vérifications du data stream

Le data stream utilisé est :

```text
logs-web-default
```

Il contient actuellement :

**21 400 documents.**

Le sujet du TP indiquait 20 700 lignes attendues, mais le fichier effectivement généré dans notre environnement contient **21 400 lignes**. Le résultat Elasticsearch correspond donc au fichier réellement ingéré.

Le nombre d'événements contenant `_grokparsefailure` est :

```text
0
```

Le parsing est donc réussi pour tous les événements.

### Backing index

Le data stream utilise actuellement le backing index :

```text
.ds-logs-web-default-2026.10.01-000001
```

Le data stream utilise :

```text
index_mode = logsdb
```

Le champ temporel du data stream est :

```text
@timestamp
```

### Premier événement

Le premier événement retrouvé contient notamment :

```text
@timestamp = 2026-09-22T22:00:39.000Z
```

soit :

```text
23/09/2026 à 00:00:39
```

en heure française d'été.

Le timestamp a donc bien été converti en champ date Elasticsearch.

Le champ `http.response.status_code` est numérique, ce qui permet de réaliser correctement des comparaisons et agrégations sur les codes HTTP.

---

## 3.5 Redémarrage de Logstash et duplication

Avec :

```text
sincedb_path => "/dev/null"
```

Logstash ne conserve pas la position de lecture du fichier entre deux redémarrages.

Lors d'un redémarrage, le fichier peut donc être relu depuis le début.

Pour `offres`, cette situation est maîtrisée grâce à :

```text
document_id => "%{id}"
```

car une même offre possède toujours le même `_id`.

Pour les logs web, les événements sont envoyés dans un data stream. La logique est différente : un data stream est conçu pour l'ingestion de nouveaux événements et ne fonctionne pas comme un index métier dans lequel on remplace facilement un document avec un `_id` métier.

Deux solutions peuvent être envisagées :

1. utiliser une `sincedb` persistante afin que Logstash mémorise la position de lecture ;
2. calculer une empreinte déterministe du contenu du log avec le filtre `fingerprint` et l'utiliser comme identifiant afin d'identifier les événements déjà traités.

---

# 4. Enquête Elasticsearch

## 4.1 Répartition du trafic

Le jeu de données contient **21 400 requêtes**.

### Codes HTTP

| Code | Nombre | Interprétation                      |
| ---: | -----: | ----------------------------------- |
|  200 | 18 432 | Succès                              |
|  201 |  1 548 | Création réussie                    |
|  404 |    512 | Ressource introuvable               |
|  304 |    501 | Ressource non modifiée              |
|  503 |    402 | Service temporairement indisponible |
|  500 |      5 | Erreur interne                      |

Les erreurs 5xx représentent :

```text
402 + 5 = 407
```

soit environ :

```text
1,90 %
```

du trafic total.

### Méthodes HTTP

| Méthode | Nombre |   Part |
| ------- | -----: | -----: |
| GET     | 19 852 | 92,8 % |
| POST    |  1 548 |  7,2 % |

Le trafic est donc très majoritairement constitué de requêtes GET.

### Volume quotidien

| Date       | Requêtes |
| ---------- | -------: |
| 22/09/2026 |      239 |
| 23/09/2026 |    2 949 |
| 24/09/2026 |    2 953 |
| 25/09/2026 |    2 960 |
| 26/09/2026 |    3 218 |
| 27/09/2026 |    3 012 |
| 28/09/2026 |    3 391 |
| 29/09/2026 |    2 678 |

La moyenne sur la période analysée est d'environ :

```text
2 675 requêtes/jour
```

Cette moyenne doit être interprétée avec prudence car le 22 septembre et le 29 septembre sont des journées partielles.

---

# 4.2 Incident HTTP 503

L'analyse des logs fait apparaître **402 réponses HTTP 503**.

L'incident commence le :

```text
2026-09-28 12:00:09 UTC
```

et se termine le :

```text
2026-09-28 12:44:53 UTC
```

En heure française :

```text
28/09/2026 de 14:00:09 à 14:44:53
```

La durée est donc d'environ :

```text
44 min 44 s
```

Les requêtes concernées sont principalement des requêtes :

```text
GET
```

Les URLs touchées correspondent principalement à des endpoints :

```text
/api/offres/...
```

avec notamment des recherches de villes comme :

* Paris ;
* Bordeaux ;
* Montpellier ;
* Lille ;
* Toulouse ;
* Lyon.

La répartition observée des principales routes pendant l'incident est :

| Route / recherche | 503 |
| ----------------- | --: |
| Paris             |  62 |
| Bordeaux          |  59 |
| Montpellier       |  59 |
| Lille             |  58 |
| Toulouse          |  58 |
| Lyon              |  57 |

Le comportement observé montre donc que le problème touche principalement l'accès aux fonctionnalités de recherche des offres plutôt qu'une URL unique.

Le fait que plusieurs requêtes GET vers les API d'offres renvoient simultanément 503 indique une indisponibilité ou une dégradation temporaire du service utilisé pour répondre à ces requêtes.

---

# 4.3 Activité suspecte

Une activité particulièrement suspecte est associée à l'adresse IP :

```text
203.0.113.66
```

Cette adresse génère :

```text
300 requêtes
```

sur une période d'environ :

```text
4 min 59 s
```

entre :

```text
2026-09-26T01:12:00Z
```

et :

```text
2026-09-26T01:16:59Z
```

Toutes ces requêtes correspondent à des réponses :

```text
404
```

### Ressources recherchées

L'IP tente notamment d'accéder à :

```text
/.env
/.git/config
/phpmyadmin/
/wp-login.php
/admin
/server-status
```

Ces chemins sont particulièrement significatifs.

Ils correspondent notamment à la recherche :

* de fichiers de configuration contenant potentiellement des secrets ;
* d'un dépôt Git exposé ;
* d'interfaces d'administration ;
* de composants WordPress ;
* de pages de supervision ou d'administration serveur.

Le User-Agent observé est :

```text
zgrab/0.x
```

Ce comportement ne ressemble pas à celui d'un utilisateur naviguant normalement sur le site.

Il s'agit plutôt d'une activité automatisée de reconnaissance ou de scan visant à identifier des services, fichiers ou interfaces potentiellement exposés.

L'activité est donc considérée comme **suspecte**.

Les 404 restants ne proviennent pas de cette adresse : sur les 512 réponses 404 totales, 300 sont associées à cette IP, soit 212 autres réponses 404 provenant d'autres requêtes.

---

# 4.4 Offres les plus consultées

Le classement est calculé uniquement sur les requêtes :

```text
GET
```

ayant obtenu :

```text
HTTP 200
```

Les 10 offres les plus consultées sont :

| Rang | Offre     | Consultations | Titre                                | Ville    | Contrat   |
| ---: | --------- | ------------: | ------------------------------------ | -------- | --------- |
|    1 | OFF-04662 |             8 | Développeur Front-end Senior         | Bordeaux | Freelance |
|    2 | OFF-01153 |             7 | Développeur Java Confirmé            | Toulouse | Freelance |
|    3 | OFF-03141 |             7 | Développeur Python Confirmé          | Bordeaux | CDI       |
|    4 | OFF-00289 |             6 | Data Scientist Lead                  | Lyon     | CDI       |
|    5 | OFF-00306 |             6 | Administrateur Systèmes Lead         | Toulouse | CDI       |
|    6 | OFF-00539 |             6 | Développeur Python Senior            | Paris    | Freelance |
|    7 | OFF-00901 |             6 | Développeur Java Junior              | Paris    | CDI       |
|    8 | OFF-01275 |             6 | Administrateur Bases de Données Lead | Paris    | CDI       |
|    9 | OFF-01660 |             6 | Architecte Cloud Senior              | Lyon     | CDI       |
|   10 | OFF-02525 |             6 | Architecte Cloud Lead                | Lille    | CDI       |

L'offre la plus consultée est donc :

**OFF-04662 — Développeur Front-end Senior à Bordeaux — Freelance**, avec 8 consultations réussies.

---

# 4.5 Utilisateurs mobiles et navigateurs

La répartition des systèmes d'exploitation montre :

```text
8412 utilisateurs/requêtes mobiles
```

pour iOS et Android sur 21 400 événements.

La part mobile est donc d'environ :

```text
39,3 %
```

Le trafic mobile représente ainsi une part importante du trafic total.

### Top 3 des navigateurs

| Rang | Navigateur    | Nombre |
| ---: | ------------- | -----: |
|    1 | Safari        |  4 278 |
|    2 | Mobile Safari |  4 243 |
|    3 | Chrome        |  4 226 |

Safari et Mobile Safari représentent ensemble une part importante du trafic, tandis que Chrome arrive en troisième position.

---

# 5. Tableau de bord Kibana

Un tableau de bord Kibana nommé :

```text
Site de recrutement — trafic
```

a été créé.

Il contient les six visualisations demandées.

## 5.1 Nombre de requêtes

Un indicateur affiche :

```text
21 400
```

requêtes au total.

---

## 5.2 Taux d'erreur serveur

Un indicateur utilise une formule calculant :

```text
nombre de réponses >= 500 / nombre total de requêtes
```

Le résultat obtenu est :

```text
1,90 %
```

---

## 5.3 Trafic par statut HTTP

Une visualisation en barres empilées présente l'évolution du trafic dans le temps.

Elle utilise :

* `@timestamp` sur l'axe temporel ;
* le nombre d'enregistrements comme métrique ;
* `http.response.status_code` comme répartition.

Elle permet notamment de visualiser les périodes d'erreurs 5xx.

---

## 5.4 Offres les plus consultées

Une visualisation présente les 10 offres ayant obtenu le plus de consultations.

La dimension utilisée est :

```text
labels.offre_id
```

avec le nombre d'enregistrements comme métrique.

---

## 5.5 Navigateurs

Une visualisation en donut présente les principaux navigateurs utilisés.

Le champ utilisé est :

```text
user_agent.name
```

Les trois premiers sont :

1. Safari ;
2. Mobile Safari ;
3. Chrome.

---

## 5.6 Offres par localisation

Une carte Kibana a été créée à partir de l'index :

```text
offres
```

Le champ géographique utilisé est :

```text
localisation
```

La carte permet de visualiser la répartition géographique des offres.

---

# 6. Alerte bonus

Une règle Kibana a également été créée :

```text
Plus de 50 erreurs 5xx en 5 minutes
```

Configuration :

* type : **Log threshold** ;
* champ : `http.response.status_code` ;
* condition : supérieur à `499` ;
* seuil : supérieur à `50` ;
* fenêtre : **5 minutes** ;
* exécution : toutes les **1 minute** ;
* regroupement : aucun.

La règle est active.

Aucune action de notification n'a été configurée.

Le compteur d'alertes reste actuellement à 0 car la règle surveille les événements récents et ne déclenche pas rétroactivement sur l'incident historique du 28 septembre.

Pour tester réellement son déclenchement, il faudrait générer ou ingérer plus de 50 réponses 5xx dans une fenêtre de cinq minutes correspondant à la période courante.

---

# Conclusion

Ce TP a permis de mettre en place une chaîne complète d'ingestion et d'analyse de données avec Elasticsearch, Logstash et Kibana.

Deux pipelines Logstash indépendants ont été configurés :

* un pipeline pour les offres ;
* un pipeline pour les logs web.

Le pipeline des offres utilise l'identifiant métier comme `_id`, ce qui permet de limiter les doublons lors des réingestions.

Le pipeline web transforme les logs HTTP en événements ECS et les envoie dans le data stream `logs-web-default`. Le parsing est entièrement réussi avec **0 `_grokparsefailure`**.

L'analyse Elasticsearch a ensuite permis d'identifier :

* **21 400 requêtes** ;
* un taux d'erreurs serveur de **1,90 %** ;
* un incident de **402 réponses 503** pendant environ **44 min 44 s** ;
* une activité automatisée suspecte provenant de `203.0.113.66` ;
* les principales offres consultées ;
* les principaux navigateurs et la part du trafic mobile.

Enfin, un tableau de bord Kibana regroupant les principaux indicateurs ainsi qu'une alerte de détection des erreurs 5xx ont été mis en place.