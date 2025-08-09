from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    import urllib.request as urlreq
except Exception:  # pragma: no cover
    urlreq = None  # type: ignore[assignment]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="only check drift; non-zero on drift")
    parser.add_argument(
        "--source",
        default="https://raw.githubusercontent.com/ghostweasellabs/meridian-runtime/main/pyproject.toml",
        help="pyproject.toml source (path or URL)",
    )
    return parser.parse_args()


def load_text(path_or_url: str) -> str:
    if path_or_url.startswith("http"):
        if urlreq is None:
            raise SystemExit("urllib not available to fetch remote file")
        with urlreq.urlopen(path_or_url) as resp:  # type: ignore[reportUnknownMemberType]
            return resp.read().decode("utf-8")
    return Path(path_or_url).read_text(encoding="utf-8")


def extract_min_version(pkg: str, text: str) -> str | None:
    # Find occurrences like "pkg>=X.Y.Z" optionally with upper bounds
    m = re.search(rf"["']?{re.escape(pkg)}>=([^,"'\]]+)", text)
    return m.group(1).strip() if m else None


def update_dependency_min(pkg: str, min_ver: str, content: str) -> tuple[str, bool]:
    pattern = re.compile(rf"({re.escape(pkg)}>=)([^,"'\]]+)")
    new_content, n = pattern.subn(rf"\g<1>{min_ver}", content)
    return new_content, n > 0


def main() -> int:
    args = parse_args()
    src_text = load_text(args.source)
    desired: dict[str, str] = {}
    for pkg in ["pytest"]:
        v = extract_min_version(pkg, src_text)
        if v:
            desired[pkg] = v

    ex_py = Path(__file__).resolve().parents[1] / "pyproject.toml"
    txt = ex_py.read_text(encoding="utf-8")
    changed = False
    for pkg, v in desired.items():
        txt, did = update_dependency_min(pkg, v, txt)
        changed = changed or did

    if args.check:
        print(json.dumps({"changed": changed, "desired": desired}, indent=2))
        return 1 if changed else 0

    if changed:
        ex_py.write_text(txt, encoding="utf-8")
        print("[sync] updated pyproject.toml")
    else:
        print("[sync] no changes required")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
