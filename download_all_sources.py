"""
Téléchargement de toutes les sources de données - POC Electio-Analytics
Rennes Métropole (43 communes, dept 35)

Sources : data.gouv.fr, INSEE, Rennes Métropole Open Data, interieur.gouv.fr

Usage:
    cd /chemin/vers/01MSPR
    python -m venv venv
    source venv/bin/activate   (Linux/Mac)
    pip install requests tqdm
    python src/download_all_sources.py
"""

import hashlib
import logging
import sys
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"

# Toutes les URLs ont été vérifiées manuellement le 2026-02-10
SOURCES = [
    # =============================================
    # ÉLECTIONS PRÉSIDENTIELLES (par communes)
    # =============================================
    {
        "nom": "Présidentielle 2022 - Tour 1",
        "url": "https://static.data.gouv.fr/resources/election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-1er-tour/20220414-152459/resultats-par-niveau-subcom-t1-france-entiere.txt",
        "fichier": "pres_2022_t1.txt",
        "source": "data.gouv.fr",
        "format": "TXT (ISO-8859-1, sep=;)",
    },
    {
        "nom": "Présidentielle 2022 - Tour 2",
        "url": "https://static.data.gouv.fr/resources/election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-2nd-tour/20220428-142333/resultats-par-niveau-subcom-t2-france-entiere.txt",
        "fichier": "pres_2022_t2.txt",
        "source": "data.gouv.fr",
        "format": "TXT (ISO-8859-1, sep=;)",
    },
    {
        "nom": "Présidentielle 2017 - Tour 1",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/77ed6b2f-c48f-4037-8479-50af74fa5c7a",
        "fichier": "pres_2017_t1.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Présidentielle 2017 - Tour 2",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/be8faff4-dedf-44be-92c7-e77feb9df335",
        "fichier": "pres_2017_t2.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Présidentielle 2012 - Tours 1 et 2",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/f81b2215-b297-4616-acbf-d8790ee38197",
        "fichier": "pres_2012.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Présidentielle 2007 - Tours 1 et 2",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/3976cd54-a785-457c-9eb2-4a8619e87bcf",
        "fichier": "pres_2007.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Présidentielle 2002 - Tour 1",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/690a0753-0fb5-4359-8cff-f70c63f51300",
        "fichier": "pres_2002_t1.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Présidentielle 2002 - Tour 2",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/cd1920b5-792f-4884-9a2a-cf44ffdad6a6",
        "fichier": "pres_2002_t2.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    # =============================================
    # ÉLECTIONS LÉGISLATIVES (par communes)
    # =============================================
    {
        "nom": "Législatives 2022 - Tour 1 (communes)",
        "url": "https://static.data.gouv.fr/resources/elections-legislatives-des-12-et-19-juin-2022-resultats-definitifs-du-premier-tour/20220614-082515/resultats-par-niveau-subcom-t1-france-entiere.txt",
        "fichier": "legis_2022_t1.txt",
        "source": "data.gouv.fr",
        "format": "TXT (sep=;)",
        "note": "URL alternative si 404 : https://www.data.gouv.fr/api/1/datasets/r/a9a82bcc-304e-491f-a4f0-c06575113745",
    },
    {
        "nom": "Législatives 2022 - Tour 2 (bureaux de vote)",
        "url": "https://static.data.gouv.fr/resources/elections-legislatives-des-12-et-19-juin-2022-resultats-du-2nd-tour/20220620-092955/resultats-par-niveau-burvot-t2-france-entiere.txt",
        "fichier": "legis_2022_t2_burvot.txt",
        "source": "data.gouv.fr",
        "format": "TXT (sep=;)",
        "note": "Pas de fichier commune dispo pour le T2 2022 — agréger par code commune",
    },
    {
        "nom": "Législatives 2017 - Tour 1",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/7b613086-b5f5-4745-82e8-11b7397f9334",
        "fichier": "legis_2017_t1.xlsx",
        "source": "data.gouv.fr",
        "format": "XLSX",
    },
    {
        "nom": "Législatives 2017 - Tour 2",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/a6df654f-11ad-4dcc-872c-d3fd94ca3e49",
        "fichier": "legis_2017_t2.xlsx",
        "source": "data.gouv.fr",
        "format": "XLSX",
    },
    {
        "nom": "Législatives 2012 - Tours 1 et 2",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/6808d1ff-6a6f-4e5b-837a-960ccc847d6d",
        "fichier": "legis_2012.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Législatives 2007 - Tour 1",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/4c483a2a-58ce-4c3e-93a0-d410016fd317",
        "fichier": "legis_2007_t1.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Législatives 2007 - Tour 2",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/4be44fe8-1881-4a4d-ab46-2fd7f5c171e5",
        "fichier": "legis_2007_t2.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Législatives 2002 - Tour 1",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/b385e9b8-6d6e-4c34-9f29-251fc55c208f",
        "fichier": "legis_2002_t1.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    {
        "nom": "Législatives 2002 - Tour 2",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/3d6791fa-5ad8-4cd9-9338-efd0d2565097",
        "fichier": "legis_2002_t2.xls",
        "source": "data.gouv.fr",
        "format": "XLS (Excel 97)",
    },
    # =============================================
    # DONNÉES SOCIO-ÉCONOMIQUES
    # =============================================
    {
        "nom": "Stats communes Rennes Métropole (43 communes)",
        "url": "https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets/donnee_statistique_commune_rennes_metropole/exports/csv",
        "fichier": "stats_communes_rm.csv",
        "source": "data.rennesmetropole.fr",
        "format": "CSV (UTF-8, sep=;)",
    },
    {
        "nom": "Délinquance - niveau communes (France)",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/6252a84c-6b9e-4415-a743-fc6a631877bb",
        "fichier": "delinquance_communes.csv.gz",
        "source": "data.gouv.fr / SSMSI",
        "format": "CSV.GZ",
    },
    {
        "nom": "Délinquance - niveau départements",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/2b27a675-e3bf-41ef-a852-5fb9ab483967",
        "fichier": "delinquance_departements.csv",
        "source": "data.gouv.fr / SSMSI",
        "format": "CSV",
    },
    {
        "nom": "FILOSOFI 2021 - Revenus communes",
        "url": "https://www.insee.fr/fr/statistiques/fichier/7756855/indic-struct-distrib-revenu-2021-COMMUNES_csv.zip",
        "fichier": "filosofi_2021_communes.zip",
        "source": "INSEE",
        "format": "ZIP (contient CSV)",
    },
    {
        "nom": "INSEE Dossier complet - ~1900 indicateurs/commune",
        "url": "https://www.insee.fr/fr/statistiques/fichier/5359146/dossier_complet.zip",
        "fichier": "dossier_complet_insee.zip",
        "source": "INSEE",
        "format": "ZIP (contient CSV 673 Mo)",
    },
    {
        "nom": "RNA Waldec - Associations (national)",
        "url": "https://media.interieur.gouv.fr/rna/rna_waldec_20250901.zip",
        "fichier": "rna_waldec.zip",
        "source": "interieur.gouv.fr",
        "format": "ZIP (1 CSV par département)",
    },
    # =============================================
    # RÉFÉRENTIELS
    # =============================================
    {
        "nom": "COG 2024 - Code Officiel Géographique communes",
        "url": "https://www.data.gouv.fr/api/1/datasets/r/7acc46ad-1c79-43d9-8f2d-d0a8ec78c068",
        "fichier": "ref_cog_2024.csv",
        "source": "data.gouv.fr / INSEE",
        "format": "CSV",
    },
]


def setup_logging():
    """Configure le logging fichier + console."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"download_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return log_file


def sha256_file(path: Path) -> str:
    """Calcule le SHA256 d'un fichier pour traçabilité."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(source: dict, session: requests.Session) -> dict:
    """Télécharge un fichier. Skip si déjà présent."""
    dest = RAW_DIR / source["fichier"]
    nom = source["nom"]
    url = source["url"]

    # Skip si déjà téléchargé
    if dest.exists() and dest.stat().st_size > 0:
        taille = dest.stat().st_size
        logging.info(f"SKIP  {nom} — déjà présent ({taille:,} octets)")
        return {
            "nom": nom,
            "fichier": source["fichier"],
            "statut": "SKIP",
            "taille": taille,
            "sha256": sha256_file(dest),
        }

    logging.info(f"START {nom}")
    logging.info(f"      URL: {url}")
    try:
        resp = session.get(url, stream=True, timeout=600)
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=nom[:45],
            leave=True,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                bar.update(len(chunk))

        taille = dest.stat().st_size
        sha = sha256_file(dest)
        logging.info(f"OK    {nom} — {taille:,} octets — SHA256: {sha[:16]}...")
        return {
            "nom": nom,
            "fichier": source["fichier"],
            "statut": "OK",
            "taille": taille,
            "sha256": sha,
        }

    except requests.exceptions.HTTPError as e:
        # Tenter l'URL alternative si présente
        alt_url = source.get("note", "")
        if "URL alternative" in alt_url:
            alt = alt_url.split("URL alternative si 404 : ")[-1].strip()
            logging.warning(f"RETRY {nom} avec URL alternative : {alt}")
            try:
                resp2 = session.get(alt, stream=True, timeout=600)
                resp2.raise_for_status()
                total = int(resp2.headers.get("content-length", 0))
                with open(dest, "wb") as f, tqdm(
                    total=total, unit="B", unit_scale=True, desc=nom[:45], leave=True
                ) as bar:
                    for chunk in resp2.iter_content(chunk_size=65536):
                        f.write(chunk)
                        bar.update(len(chunk))
                taille = dest.stat().st_size
                sha = sha256_file(dest)
                logging.info(f"OK    {nom} (alt) — {taille:,} octets")
                return {
                    "nom": nom,
                    "fichier": source["fichier"],
                    "statut": "OK (alt)",
                    "taille": taille,
                    "sha256": sha,
                }
            except Exception as e2:
                logging.error(f"FAIL  {nom} — {e2}")

        logging.error(f"FAIL  {nom} — HTTP {e}")
        if dest.exists():
            dest.unlink()
        return {"nom": nom, "fichier": source["fichier"], "statut": f"ERREUR: {e}", "taille": 0, "sha256": ""}

    except Exception as e:
        logging.error(f"FAIL  {nom} — {e}")
        if dest.exists():
            dest.unlink()
        return {"nom": nom, "fichier": source["fichier"], "statut": f"ERREUR: {e}", "taille": 0, "sha256": ""}


def main():
    log_file = setup_logging()

    logging.info("=" * 65)
    logging.info("  ELECTIO-ANALYTICS — Téléchargement des sources de données")
    logging.info(f"  Date       : {datetime.now():%Y-%m-%d %H:%M:%S}")
    logging.info(f"  Destination: {RAW_DIR}")
    logging.info(f"  Sources    : {len(SOURCES)} fichiers")
    logging.info("=" * 65)

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "Electio-Analytics-POC/1.0 (MSPR-Bloc3)"})

    resultats = []
    for i, source in enumerate(SOURCES, 1):
        logging.info(f"\n[{i}/{len(SOURCES)}] {source['nom']}")
        r = download_file(source, session)
        resultats.append(r)

    # --- Rapport final ---
    ok = [r for r in resultats if r["statut"] in ("OK", "OK (alt)", "SKIP")]
    ko = [r for r in resultats if r["statut"].startswith("ERREUR")]

    logging.info("\n" + "=" * 65)
    logging.info("  RAPPORT FINAL")
    logging.info("=" * 65)
    logging.info(f"{'Statut':<12} {'Taille':>10}   {'Fichier'}")
    logging.info("-" * 65)
    for r in resultats:
        mo = f"{r['taille'] / 1_000_000:.1f} Mo" if r["taille"] else "—"
        logging.info(f"{r['statut']:<12} {mo:>10}   {r['fichier']}")

    logging.info("-" * 65)
    total_mo = sum(r["taille"] for r in resultats) / 1_000_000
    logging.info(f"Total : {total_mo:.0f} Mo | OK : {len(ok)}/{len(resultats)} | Erreurs : {len(ko)}")
    logging.info(f"Log   : {log_file}")

    if ko:
        logging.warning("\nSOURCES EN ERREUR :")
        for r in ko:
            logging.warning(f"  ✗ {r['nom']} — {r['statut']}")
        sys.exit(1)

    logging.info("\nTous les téléchargements terminés avec succès.")


if __name__ == "__main__":
    main()
