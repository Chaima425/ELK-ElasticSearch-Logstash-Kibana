"""Partie 3a — Crée l'index `offres` avec un mapping explicite puis ingère le NDJSON en bulk.

Usage : python ingest.py [--fichier data/offres.ndjson] [--reset]
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterator
from pathlib import Path

from elasticsearch import helpers

from es_client import INDEX, get_client

SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0}

MAPPINGS = {
    "dynamic": "strict",
    "properties": {
        "id": {"type": "keyword"},
        "entreprise": {"type": "keyword"},
        "ville": {"type": "keyword"},
        "contrat": {"type": "keyword"},
        "teletravail": {"type": "keyword"},
        "titre": {
            "type": "text",
            "analyzer": "french",
            "fields": {
                "brut": {"type": "keyword"}
            },
        },
        "description": {
            "type": "text",
            "analyzer": "french",
        },
        "competences": {
            "type": "keyword",
            "fields": {
                "texte": {
                    "type": "text",
                    "analyzer": "french",
                }
            },
        },
        "localisation": {"type": "geo_point"},
        "experience_annees": {"type": "integer"},
        "salaire_min": {"type": "integer"},
        "salaire_max": {"type": "integer"},
        "date_publication": {"type": "date"},
    },
}


def lire_actions(fichier: Path) -> Iterator[dict]:
    """Lit le fichier NDJSON ligne par ligne et produit les actions bulk."""
    with fichier.open("r", encoding="utf-8") as f:
        for ligne in f:
            ligne = ligne.strip()

            if not ligne:
                continue

            document = json.loads(ligne)

            yield {
                "_index": INDEX,
                "_id": document["id"],
                "_source": document,
            }

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fichier", type=Path, default=Path("data/offres.ndjson"))
    parser.add_argument("--reset", action="store_true", help="supprime l'index s'il existe")
    args = parser.parse_args()

    es = get_client()
    print("Cluster :", es.info()["version"]["number"])

    # TODO 3 : si --reset, supprimer l'index (sans erreur s'il n'existe pas)
    if args.reset:
        es.indices.delete(index=INDEX, ignore_unavailable=True)
        print(f"Index '{INDEX}' supprimé.")
    # TODO 4 : créer l'index s'il n'existe pas, avec SETTINGS et MAPPINGS
    if not es.indices.exists(index=INDEX):
        es.indices.create(
            index=INDEX,
            settings=SETTINGS,
            mappings=MAPPINGS,
        )
        print(f"Index '{INDEX}' créé.")
    else:
        print(f"Index '{INDEX}' existe déjà.")
    # TODO 5 : ingérer avec helpers.bulk (chunk_size=1000, raise_on_error=False), afficher les erreurs
    succes, erreurs = helpers.bulk(
        es,
        lire_actions(args.fichier),
        chunk_size=1000,
        raise_on_error=False,
    )

    print(f"{succes} documents indexés.")

    if erreurs:
        print(f"{len(erreurs)} erreurs :")
        for erreur in erreurs:
            print(erreur)

    # TODO 6 : rafraîchir l'index puis afficher le nombre de documents (es.count)
    es.indices.refresh(index=INDEX)

    total = es.count(index=INDEX)["count"]
    print(f"{total} documents dans '{INDEX}'.")


if __name__ == "__main__":
    main()
