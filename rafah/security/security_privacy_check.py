import json
import os
import re
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT / ".env"
OUT_DIR = ROOT / "docs_evidence/security"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SECRET_KEYS = [
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_BOT_TOKEN_MODELLING",
    "TELEGRAM_CHAT_ID",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "NEWSAPI_KEY",
    "GUARDIAN_API_KEY",
]

SCAN_PATHS = [
    ROOT / "rafah/modelling/market_flow_outputs",
    ROOT / "rafah/orchestration/logs",
    ROOT / "rafah/orchestration/audit",
    ROOT / "rafah/dashboard-react/public/market_flow_outputs",
]

PII_PATTERNS = {
    "email": r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
    "indonesian_phone": r"(\+62|62|0)8[0-9]{8,12}",
    "telegram_token_like": r"\b\d{8,12}:[A-Za-z0-9_-]{30,}\b",
    "api_key_like": r"(?i)(api[_-]?key|secret|token|password)\s*[:=]\s*['\"]?[^'\"\s]+",
}


def mask_value(value: str) -> str:
    if not value:
        return "<empty>"
    value = str(value).strip()
    if len(value) <= 8:
        return "*" * len(value)
    return value[:4] + "*" * (len(value) - 8) + value[-4:]


def read_env_file():
    result = {}
    if not ENV_PATH.exists():
        return result

    for line in ENV_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.split("#", 1)[0].strip()
        result[key.strip()] = value
    return result


def scan_file(path: Path):
    findings = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return findings

    for name, pattern in PII_PATTERNS.items():
        matches = re.findall(pattern, text)
        if matches:
            findings.append({
                "file": str(path.relative_to(ROOT)),
                "type": name,
                "count": len(matches),
            })
    return findings


def main():
    env_values = read_env_file()

    secret_report = []
    for key in SECRET_KEYS:
        value = env_values.get(key) or os.getenv(key, "")
        secret_report.append({
            "key": key,
            "present": bool(value),
            "masked_value": mask_value(value) if value else "<not set>",
            "storage": ".env / environment variable",
        })

    pii_findings = []
    scanned_files = 0

    for base in SCAN_PATHS:
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_file() and path.suffix.lower() in [".csv", ".json", ".jsonl", ".log", ".txt"]:
                scanned_files += 1
                pii_findings.extend(scan_file(path))

    report = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "purpose": "Secrets masking and PII scan evidence for IPBD project documentation.",
        "secrets_policy": {
            "real_values_are_not_printed": True,
            "env_file_is_gitignored": ".env should be listed in .gitignore",
            "example_file_uses_placeholders": ".env.example"
        },
        "secret_variables": secret_report,
        "pii_scan": {
            "scanned_files": scanned_files,
            "finding_count": len(pii_findings),
            "findings": pii_findings,
            "status": "passed" if len(pii_findings) == 0 else "review_needed"
        }
    }

    json_path = OUT_DIR / "security_privacy_report.json"
    txt_path = OUT_DIR / "08_secrets_pii_masking.txt"

    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = []
    lines.append("=" * 80)
    lines.append("SECRETS & PII MASKING CHECK")
    lines.append("=" * 80)
    lines.append(f"Created at   : {report['created_at']}")
    lines.append(f"PII status   : {report['pii_scan']['status']}")
    lines.append(f"Files scanned: {scanned_files}")
    lines.append("")
    lines.append("Masked secret variables:")
    for item in secret_report:
        lines.append(f"- {item['key']}: present={item['present']} | value={item['masked_value']}")
    lines.append("")
    lines.append("PII findings:")
    if pii_findings:
        for item in pii_findings:
            lines.append(f"- {item['file']} | {item['type']} | count={item['count']}")
    else:
        lines.append("- No email/phone/token-like PII found in scanned output files.")
    lines.append("=" * 80)

    txt_path.write_text("\n".join(lines), encoding="utf-8")

    print("\n".join(lines))
    print()
    print(f"[OK] Saved: {json_path}")
    print(f"[OK] Saved: {txt_path}")


if __name__ == "__main__":
    main()
