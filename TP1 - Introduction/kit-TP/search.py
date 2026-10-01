"""Mini-défi — moteur de recherche d'offres en ligne de commande.

Attendu :
  python search.py "développeur python"
  python search.py "données spark" --ville Lyon --contrat CDI --salaire-min 45000
  python search.py "kubernetes" --autour "43.6108,3.8767"--rayon 50km --teletravail partiel
"""

from __future__ import annotations

import argparse

from es_client import INDEX, get_client


def construire_requete(args: argparse.Namespace) -> dict:
    """Construit la requête bool du moteur de recherche."""
    filtres = []

    if args.ville:
        filtres.append({"term": {"ville": args.ville}})

    if args.contrat:
        filtres.append({"term": {"contrat": args.contrat}})

    if args.teletravail:
        filtres.append({"term": {"teletravail": args.teletravail}})

    if args.salaire_min is not None:
        filtres.append({
            "range": {
                "salaire_max": {
                    "gte": args.salaire_min
                }
            }
        })

    if args.autour:
        filtres.append({
            "geo_distance": {
                "distance": args.rayon,
                "localisation": args.autour
            }
        })

    return {
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "query": args.texte,
                        "fields": [
                            "titre^3",
                            "competences.texte^2",
                            "description"
                        ],
                        "fuzziness": "AUTO"
                    }
                }
            ],
            "filter": filtres
        }
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("texte")
    p.add_argument("--ville")
    p.add_argument("--contrat", choices=["CDI", "CDD", "Alternance", "Freelance", "Stage"])
    p.add_argument("--teletravail", choices=["aucun", "partiel", "total"])
    p.add_argument("--salaire-min", type=int)
    p.add_argument("--autour", help="lat,lon")
    p.add_argument("--rayon", default="30km")
    p.add_argument("--page", type=int, default=1)
    p.add_argument("--taille", type=int, default=10)
    args = p.parse_args()

    es = get_client()
    # TODO : appeler es.search avec la requête, la pagination (from_, size), un highlight sur
    # description et trois facettes (aggs terms) : ville, contrat, compétences.
    # Afficher : total, puis pour chaque résultat score, titre, entreprise, ville, contrat, salaire,
    # l'extrait surligné, et enfin les facettes.
    requete = construire_requete(args)

    reponse = es.search(
        index=INDEX,
        from_=(args.page - 1) * args.taille,
        size=args.taille,
        query=requete,
        highlight={
            "fields": {
                "description": {}
            }
        },
        aggs={
            "villes": {
                "terms": {
                    "field": "ville",
                    "size": 10
                }
            },
            "contrats": {
                "terms": {
                    "field": "contrat",
                    "size": 10
                }
            },
            "competences": {
                "terms": {
                    "field": "competences",
                    "size": 10
                }
            }
        }
    )

    total = reponse["hits"]["total"]["value"]
    print(f"\n{total} résultat(s)\n")

    for hit in reponse["hits"]["hits"]:
        source = hit["_source"]

        titre = source.get("titre", "")
        entreprise = source.get("entreprise", "")
        ville = source.get("ville", "")
        contrat = source.get("contrat", "")
        salaire_min = source.get("salaire_min", "N/A")
        salaire_max = source.get("salaire_max", "N/A")

        highlight = hit.get("highlight", {}).get("description", [])
        extrait = " ... ".join(highlight) if highlight else source.get("description", "")

        print(f"[score={hit['_score']}] {titre}")
        print(f"  Entreprise : {entreprise}")
        print(f"  Ville      : {ville}")
        print(f"  Contrat    : {contrat}")
        print(f"  Salaire    : {salaire_min} - {salaire_max}")
        print(f"  Extrait    : {extrait[:300]}")
        print()

    print("=== Facettes ===")

    for nom, titre in [
        ("villes", "Villes"),
        ("contrats", "Contrats"),
        ("competences", "Compétences"),
    ]:
        print(f"\n{titre} :")
        for bucket in reponse["aggregations"][nom]["buckets"]:
            print(f"  {bucket['key']} : {bucket['doc_count']}")


if __name__ == "__main__":
    main()
