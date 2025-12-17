import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urldefrag


UNSUPPORTED_HOST_HINTS = (
    "share.google",
    "scribd.com",
)


def _normalize_url(u: str) -> str:
    return urldefrag(u.strip())[0]


def _is_probably_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


def parse_pairs(text: str) -> List[Tuple[str, str]]:
    """
    Extracts (chapter_name, url) pairs from messy pasted text.
    Strategy:
    - Find every URL in the text
    - Use the nearest non-empty line above it as the chapter name
    """
    lines = [ln.strip() for ln in text.splitlines()]
    pairs: List[Tuple[str, str]] = []

    url_re = re.compile(r"https?://\S+")
    for idx, ln in enumerate(lines):
        m = url_re.search(ln)
        if not m:
            continue
        url = _normalize_url(m.group(0))

        # Scan upward for chapter label (skip blank lines and generic "Link")
        name: Optional[str] = None
        j = idx
        while j >= 0:
            cand = lines[j].strip()
            j -= 1
            if not cand:
                continue
            if cand.lower() == "link":
                continue
            if _is_probably_url(cand):
                continue
            # if line is like "BNI Azpire https://..." keep only left side
            cand = url_re.sub("", cand).strip()
            if cand:
                name = cand
                break

        if not name:
            name = f"Chapter_{len(pairs) + 1}"

        pairs.append((name, url))

    return pairs


def build_chapters(pairs: List[Tuple[str, str]]) -> Tuple[List[Dict[str, str]], List[Tuple[str, str, str]]]:
    """
    Returns:
    - CHAPTERS list
    - skipped list: (name, url, reason)
    """
    dedup: Dict[str, Dict[str, str]] = {}
    skipped: List[Tuple[str, str, str]] = []

    for name, url in pairs:
        if any(h in url for h in UNSUPPORTED_HOST_HINTS):
            skipped.append((name, url, "unsupported host"))
            continue

        # Keep everything else; scraper will resolve /memberlist where possible.
        if url in dedup:
            continue
        dedup[url] = {"chapter": name, "url": url}

    return list(dedup.values()), skipped


def write_bni_chapters_py(chapters: List[Dict[str, str]], out_path: Path) -> None:
    out_lines = ["CHAPTERS = ["]
    for c in chapters:
        chap = c["chapter"].replace('"', '\\"')
        url = c["url"].replace('"', '\\"')
        out_lines.append(f'    {{"chapter": "{chap}", "url": "{url}"}},')
    out_lines.append("]")
    out_lines.append("")
    out_path.write_text("\n".join(out_lines), encoding="utf-8")


def main() -> None:
    raw_path = Path("chapters_raw.txt")
    if not raw_path.exists():
        raise SystemExit("Missing chapters_raw.txt. Paste your chapter list text into that file.")

    text = raw_path.read_text(encoding="utf-8", errors="ignore")
    pairs = parse_pairs(text)
    chapters, skipped = build_chapters(pairs)
    write_bni_chapters_py(chapters, Path("bni_chapters.py"))

    print(f"Found URLs: {len(pairs)}")
    print(f"Wrote chapters: {len(chapters)} -> bni_chapters.py")
    if skipped:
        print("\nSkipped:")
        for name, url, reason in skipped:
            print(f"- {name}: {url} ({reason})")


if __name__ == "__main__":
    main()


