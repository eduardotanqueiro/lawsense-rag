import requests
import hashlib
import csv
import os
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import argparse
from urllib.parse import urlsplit, quote

METADATA_PATH = os.path.join("data", "metadata_raw.csv")


# Utils

# def sha256_file(path):
#     with open(path, "rb") as f:
#         return hashlib.sha256(f.read()).hexdigest()
    
def sha256_content(content):
    return hashlib.sha256(content).hexdigest()


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


# Funcs

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

        # uniqueness is more important
        file_name = hashlib.sha256(href.encode('utf-8')).hexdigest() + ".html"
        file_path = os.path.join("data", "raw", "dgsi", file_name)

        h = sha256_content(doc_html)

        if h not in existing:
            save_file(file_path, doc_html)
   
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
    file_path = os.path.join("data", "raw", "constituicao", file_name)

    existing = load_existing_hashes()
    h = sha256_content(response.content)

    if h not in existing:
        save_file(file_path, response.content)

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
            file_path = os.path.join("data", "raw", "tc", file_name)


            h = sha256_content(acordao_html)
            if h not in existing_hashes:

                if os.path.exists(file_path):
                    # Avoid overwriting different files with same name
                    file_name = f"{hashlib.sha256(acordao_url.encode('utf-8')).hexdigest()}_{file_name}"
                    file_path = os.path.join("data", "raw", "tc", file_name)
                
                save_file(file_path, acordao_html)

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

def fetch_tc_ebook_pdfs():
    print("[TC-PDF] Crawling PDFs...")

    base_links = ["https://www.tribunalconstitucional.pt/tc/home.html", 
             "https://www.tribunalconstitucional.pt/tc/ebook/"]


    existing_hashes = load_existing_hashes()
    new_docs = 0

    save_dir = os.path.join("data", "raw", "tc_pdf")
    os.makedirs(save_dir, exist_ok=True)

    pdf_links = []


    for base_link in base_links:

        # Fetch HTML index
        try:
            resp = requests.get(base_link, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"[TC-PDF] Failed to fetch index: {e}")
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find links ending with .pdf
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()

            if href.lower().endswith(".pdf"):
                if href.startswith("http"):
                    pdf_url = href
                else:
                    pdf_url = base_link + href  # normalize relative link

                label = a.text.strip() or pdf_url.split("/")[-1]
                pdf_links.append((pdf_url, label))

        print(f"[TC-PDF] Found {len(pdf_links)} PDF links")

    # Deduplicate
    seen = set()
    pdf_links = [(u, t) for (u, t) in pdf_links if not (u in seen or seen.add(u))]

    # Download and validate each PDF
    for pdf_url, title in pdf_links:
        try:
            r = requests.get(pdf_url, timeout=20)
            if r.status_code != 200:
                print(f"[TC-PDF] Skipping (status {r.status_code}): {pdf_url}")
                continue

            # Must be PDF
            ctype = r.headers.get("Content-Type", "").lower()
            if "pdf" not in ctype:
                print(f"[TC-PDF] Skipping non-PDF: {pdf_url}")
                continue

            # Must begin with %PDF
            if not r.content.startswith(b"%PDF"):
                print(f"[TC-PDF] Skipping HTML disguised as PDF: {pdf_url}")
                continue

            # Basic size check
            if len(r.content) < 20_000:
                print(f"[TC-PDF] Skipping too-small PDF ({len(r.content)} bytes): {pdf_url}")
                continue

            # Save
            filename = os.path.basename(urlsplit(pdf_url).path)
            local_filename = filename
            file_path = os.path.join(save_dir, local_filename)

            h = sha256_content(r.content)

            if h not in existing_hashes:
                save_file(file_path, r.content)

                save_metadata({
                    "id": local_filename,
                    "title": title,
                    "source": "Tribunal Constitucional",
                    "url": pdf_url,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "file_path": file_path,
                    "hash": h,
                })
                new_docs += 1
                print(f"[TC-PDF] Downloaded: {pdf_url}")

            else:
                print(f"[TC-PDF] Already exists (hash match): {pdf_url}")

        except Exception as e:
            print(f"[TC-PDF] Error downloading {pdf_url}: {e}")

    print(f"[TC-PDF] Completed. New PDFs added: {new_docs}")
    return new_docs



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

    new_docs += fetch_tc_ebook_pdfs()

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
