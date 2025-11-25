import requests
import hashlib
import csv
import os
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import argparse
from urllib.parse import urlsplit, quote

METADATA_PATH = "data/metadata.csv"

# os.makedirs("data", exist_ok=True)
# os.makedirs("data/raw", exist_ok=True)



# --------------------------------------------------------
# Utility functions
# --------------------------------------------------------

def sha256_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def load_existing_hashes():
    if not os.path.exists(METADATA_PATH):
        return set()
    with open(METADATA_PATH, newline="", encoding="utf-8") as f:
        return {row["hash"] for row in csv.DictReader(f)}


def save_metadata(row):
    file_exists = os.path.exists(METADATA_PATH)

    with open(METADATA_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id", "title", "source", "url", "timestamp", "file_path", "hash"]
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def save_file(file_path, content):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(content)


# --------------------------------------------------------
# 1. DGSI
# --------------------------------------------------------

def fetch_dgsi_latest(limit=40, url="https://www.dgsi.pt/jstj.nsf/"):
    print("[DGSI] Checking latest rulings...")
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    links = soup.select("a")[:limit]
    existing = load_existing_hashes()
    new_docs = 0

    for link in links:
        href = link.get("href")

        if not href or "OpenDocument" not in href:
            continue
        # if not href or "jstj" not in href:
        #     continue

        doc_url = "https://www.dgsi.pt" + href
        doc_html = requests.get(doc_url).text.encode("utf-8")

        # use hash if uniqueness is more important
        file_name = hashlib.sha256(href.encode('utf-8')).hexdigest() + ".html"

        file_path = f"data/raw/dgsi/{file_name}"

        save_file(file_path, doc_html)

        h = sha256_file(file_path)
        if h not in existing:
            new_docs += 1
            save_metadata({
                "id": file_name,
                "title": link.text.strip(),
                "source": "DGSI",
                "url": doc_url,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "file_path": file_path,
                "hash": h,
            })

    return new_docs

def fetch_constituicao_latest():

    url = "https://www.parlamento.pt/Legislacao/Documents/constpt2005.pdf"
    print("[Constituição] Downloading the Constitution document...")
    response = requests.get(url)
    file_name = "constituicao.pdf"
    file_path = f"data/raw/constituicao/{file_name}"
    save_file(file_path, response.content)

    h = sha256_file(file_path)
    existing = load_existing_hashes()
    if h not in existing:
        save_metadata({
            "id": file_name,
            "title": "Constituição da República Portuguesa",
            "source": "Parlamento.pt",
            "url": url,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "file_path": file_path,
            "hash": h,
        })

        return 1
    return 0


def fetch_tc_all(limit=40):
    print("[TC] Fetching ALL Acórdãos from Tribunal Constitucional...")

    BASE = "https://www.tribunalconstitucional.pt"
    LISTING_URL = BASE + "/tc/acordaos/?p="

    existing_hashes = load_existing_hashes()
    new_docs = 0
    page = 1

    while new_docs < limit:
        url = LISTING_URL + str(page)
        resp = requests.get(url)
        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        table = soup.find("table")
        if not table:
            break   # no more pages

        rows = table.find_all("tr")[1:]  # skip header
        if not rows:
            break

        for row in rows:
            cols = row.find_all("td")

            if len(cols) != 6:
                continue

            acordao_label = cols[0].text.strip()         # ex: "Acórdão 587/2024"


            link = cols[0].find("a")
            if not link or not link.get("href"):
                continue

            href = link["href"].split("/")[-1]   # ex: 20240587.html
            acordao_url = f"{BASE}/tc/acordaos/{href}"

            # Download full HTML
            try:
                acordao_html = requests.get(acordao_url).content
            except Exception:
                continue

            # Unique filename
            file_name = hashlib.sha256(acordao_url.encode("utf-8")).hexdigest() + ".html"
            file_path = f"data/raw/tc/{file_name}"

            save_file(file_path, acordao_html)

            h = sha256_file(file_path)
            if h not in existing_hashes:
                new_docs += 1
                save_metadata({
                    "id": file_name,
                    "title": acordao_label,
                    "source": "Tribunal Constitucional",
                    "url": acordao_url,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "file_path": file_path,
                    "hash": h,
                })


        page += 1

    print(f"[TC] Completed. Pages crawled: {page - 1}")
    return new_docs

def fetch_tc_pdf_by_patterns(start_year=2000, end_year=None):
    print("[TC-PDF] Fetching Tribunal Constitucional PDFs using filename patterns...")

    if end_year is None:
        end_year = datetime.now().year

    BASE = "https://www.tribunalconstitucional.pt/tc/content/files"
    existing_hashes = load_existing_hashes()
    new_docs = 0

    # Known PDF naming patterns used by TC
    PATTERNS = [
        "AcOrdaosTC{year}.pdf",
        "Relatorio_de_Atividades_{year}.pdf",
        "Relatorio_{year}.pdf",
        "Relatorio_Atividades_TC_{year}.pdf",
        "Vol_{year}.pdf",
    ]

    save_dir = "data/raw/tc_pdf"
    os.makedirs(save_dir, exist_ok=True)

    for year in range(start_year, end_year + 1):
        for pattern in PATTERNS:
            filename = pattern.format(year=year)
            pdf_url = f"{BASE}/{year}/{filename}"

            try:
                r = requests.get(pdf_url, timeout=10)
            except Exception:
                continue

            # Reject bad responses
            if r.status_code != 200 or len(r.content) < 20_000:
                continue

            # Hash and save
            local_filename = f"{year}_{filename}"
            file_path = os.path.join(save_dir, local_filename)
            save_file(file_path, r.content)

            h = sha256_file(file_path)
            if h not in existing_hashes:
                save_metadata({
                    "id": local_filename,
                    "title": filename,
                    "source": "Tribunal Constitucional",
                    "url": pdf_url,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "file_path": file_path,
                    "hash": h,
                })
                new_docs += 1
                print(f"[TC-PDF] Downloaded {pdf_url}")

    print(f"[TC-PDF] Completed with {new_docs} new PDFs.")
    return new_docs

# --------------------------------------------------------
# MAIN Execution
# --------------------------------------------------------

def run_daily_download(limit=40):
    new_docs = 0

    new_docs += fetch_dgsi_latest(limit=limit, url="https://www.dgsi.pt/jstj.nsf/") # Supremo Tribunal de Justiça
    new_docs += fetch_dgsi_latest(limit=limit, url="https://www.dgsi.pt/jsta.nsf/") # Supremo Tribunal Administrativo
    new_docs += fetch_dgsi_latest(limit=limit, url="https://www.dgsi.pt/jtrp.nsf/") # Tribunal da Relação do Porto
    new_docs += fetch_dgsi_latest(limit=limit, url="https://www.dgsi.pt/jtrl.nsf/") # Tribunal da Relação do Lisboa
    new_docs += fetch_dgsi_latest(limit=limit, url="https://www.dgsi.pt/jtrc.nsf/") # Tribunal da Relação do Coimbra
    new_docs += fetch_dgsi_latest(limit=limit, url="https://www.dgsi.pt/jtca.nsf/") # Tribunal Central Administrativo Sul
    new_docs += fetch_dgsi_latest(limit=limit, url="https://www.dgsi.pt/jtcn.nsf/") # Tribunal Central Administrativo Norte

    new_docs += fetch_constituicao_latest()

    new_docs += fetch_tc_pdf_by_patterns(start_year=2000, end_year=datetime.now().year)

    new_docs += fetch_tc_all(limit=limit)


    return new_docs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download legal documents")
    parser.add_argument("--limit", type=int, default=40, help="Number of latest documents to fetch from each source")
    args = parser.parse_args()

    call_download = run_daily_download(args.limit)
    if call_download:
        print(f"NEW_DOCUMENTS: {call_download}")
    else:
        print("NO_CHANGE")
