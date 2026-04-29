import argparse
from datetime import datetime
from pathlib import Path

CONTEXT_FILE = Path(__file__).resolve().parent / "context.md"


def append_context(title: str, summary: str) -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    entry = (
        f"### {timestamp} — {title}\n"
        f"\n"
        f"{summary.strip()}\n"
        f"\n"
    )

    if not CONTEXT_FILE.exists():
        CONTEXT_FILE.write_text("# Session Context\n\n")

    with CONTEXT_FILE.open("a", encoding="utf-8") as fh:
        fh.write(entry)

    print(f"Appended session context to {CONTEXT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Append a session note to context.md")
    parser.add_argument("--title", required=True, help="Short title for the session entry")
    parser.add_argument("--summary", required=True, help="Summary of what changed or was done")

    args = parser.parse_args()
    append_context(args.title, args.summary)
