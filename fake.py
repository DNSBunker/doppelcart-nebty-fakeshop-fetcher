"""
Scraper fuer die Fake-Domain-Liste von investigations.nebty-id.com.

Verbesserungen ggue. der Ursprungsversion:
  - Doppelte Retry-Absicherung: urllib3-Retry-Adapter + manueller Backoff-Loop
    fuer Faelle, die ueber die Session-Retries hinausgehen (z.B. harte
    Verbindungsabbrueche oder ein 429, das trotz Adapter durchrutscht).
  - Respektiert den 'Retry-After'-Header bei 429 statt starr zu warten.
  - Proaktive kleine Pause zwischen Requests, um das Rate-Limit gar nicht
    erst zu reizen.
  - Automatische Fortsetzung: Fortschritt (aktuelle Seite + bereits
    gesammelte Domains) wird in einer State-Datei gesichert. Bei einem
    Absturz, Strg+C oder einfach einem erneuten Start wird dort
    weitergemacht statt von vorne zu beginnen.
  - Domains werden als Set gesammelt -> keine Duplikate, auch wenn nach
    einem Abbruch eine Seite doppelt abgerufen wird.
  - total_pages/total wird bei jeder Seite neu aus der Antwort gelesen,
    falls sich der Datenbestand waehrend des Laufs aendert.
  - Nach einem vollstaendigen Lauf: Vergleich mit dem Snapshot des
    letzten Laufs -> neue/entfernte Domains landen in eigenen Dateien,
    damit du bei jedem kuenftigen Abgleich sofort siehst, was sich
    veraendert hat.
"""

import json
import logging
import random
import signal
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry

BASE = "https://investigations.nebty-id.com/doppelcart/api/rows"

OUTFILE = Path("domains.txt")              # aktuelles Ergebnis
STATE_FILE = Path("scrape_state.json")     # Zwischenstand fuer Fortsetzung nach Abbruch
PREV_FILE = Path("domains_prev.txt")       # Snapshot des letzten vollstaendigen Laufs
NEW_FILE = Path("domains_new.txt")         # seit letztem Lauf neu hinzugekommen
REMOVED_FILE = Path("domains_removed.txt") # seit letztem Lauf verschwunden

PAGE_SIZE = 100
REQUEST_DELAY = 0.3        # Sekunden Pause zwischen Requests (proaktiv gegen 429)
MAX_PAGE_ATTEMPTS = 8      # zusaetzliche Versuche pro Seite oberhalb der Adapter-Retries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.0,   # 1s, 2s, 4s, 8s, 16s ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "domain-scraper/1.1"})
    return session


def load_state():
    """Laedt Fortschritt aus einem vorherigen, abgebrochenen Lauf."""
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            page, domains = data["page"], set(data["domains"])
            log.info(
                f"Fortsetzung erkannt: Seite {page}, "
                f"{len(domains)} Domains bereits gesammelt."
            )
            return page, domains
        except (json.JSONDecodeError, KeyError):
            log.warning("State-Datei beschaedigt - starte von vorne.")
    return 1, set()


def save_state(page: int, domains: set) -> None:
    STATE_FILE.write_text(
        json.dumps({"page": page, "domains": sorted(domains)}),
        encoding="utf-8",
    )


def fetch_page(session: requests.Session, page: int) -> dict:
    """Holt eine Seite. Faengt zusaetzlich Fehler ab, gegen die die
    Retry-Strategie der Session allein nicht mehr ankommt."""
    for attempt in range(1, MAX_PAGE_ATTEMPTS + 1):
        try:
            r = session.get(
                BASE,
                params={
                    "page": page,
                    "size": PAGE_SIZE,
                    "sort": "domain",
                    "dir": "asc",
                },
                timeout=30,
            )
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 5))
                log.warning(f"429 erhalten (Seite {page}) - warte {wait}s ...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            wait = min(60, 2 ** attempt + random.uniform(0, 1))
            log.warning(
                f"Fehler bei Seite {page}: {e} "
                f"(Versuch {attempt}/{MAX_PAGE_ATTEMPTS}) - warte {wait:.1f}s"
            )
            time.sleep(wait)

    raise RuntimeError(f"Seite {page} nach {MAX_PAGE_ATTEMPTS} Versuchen nicht abrufbar.")


def check_for_changes(current: set) -> None:
    """Vergleicht das aktuelle Ergebnis mit dem Snapshot des letzten Laufs."""
    if PREV_FILE.exists():
        previous = set(PREV_FILE.read_text(encoding="utf-8").splitlines())
        new = current - previous
        removed = previous - current

        if new:
            NEW_FILE.write_text("\n".join(sorted(new)) + "\n", encoding="utf-8")
            log.info(f"{len(new)} neue Domains seit letztem Lauf -> {NEW_FILE}")
        else:
            NEW_FILE.unlink(missing_ok=True)

        if removed:
            REMOVED_FILE.write_text("\n".join(sorted(removed)) + "\n", encoding="utf-8")
            log.info(f"{len(removed)} entfernte Domains seit letztem Lauf -> {REMOVED_FILE}")
        else:
            REMOVED_FILE.unlink(missing_ok=True)

        if not new and not removed:
            log.info("Keine Aenderungen zum letzten Lauf.")
    else:
        log.info("Erster Lauf - kein Vergleich moeglich.")

    PREV_FILE.write_text("\n".join(sorted(current)) + "\n", encoding="utf-8")


def main() -> None:
    session = build_session()
    page, domains = load_state()

    stop = False

    def on_sigint(signum, frame):
        nonlocal stop
        log.warning("Abbruch angefordert - speichere Zwischenstand ...")
        stop = True

    signal.signal(signal.SIGINT, on_sigint)

    try:
        while not stop:
            data = fetch_page(session, page)

            for row in data.get("rows", []):
                d = row.get("domain")
                if d:
                    domains.add(d)

            total_pages = data.get("pages", page)
            total_count = data.get("total", len(domains))
            log.info(f"Seite {page}/{total_pages} - {len(domains)}/{total_count} Domains")

            if page >= total_pages:
                break

            page += 1
            save_state(page, domains)
            time.sleep(REQUEST_DELAY)

    except Exception:
        save_state(page, domains)
        log.exception("Abgebrochen - Fortschritt gespeichert. Skript kann einfach erneut gestartet werden.")
        sys.exit(1)

    if stop:
        save_state(page, domains)
        log.info("Angehalten. Ein erneuter Start setzt automatisch fort.")
        sys.exit(1)

    # vollstaendig durchgelaufen
    OUTFILE.write_text("\n".join(sorted(domains)) + "\n", encoding="utf-8")
    STATE_FILE.unlink(missing_ok=True)
    log.info(f"Fertig: {len(domains)} Domains -> {OUTFILE}")

    check_for_changes(domains)


if __name__ == "__main__":
    main()
