"""Sizes as people read them, in decimal units as RNS writes them ("1.50 KB"), so the CLI, the features' reports and
the log say a size alike. No RNS (the CLI imports this module)."""


def human_size(num_bytes: float | None, unknown: str = "?") -> str:
    """A size in decimal units: "512 B", "1.50 KB", "12.3 KB", "123 KB", "3.20 MB"; unknown for None."""
    if num_bytes is None:
        return unknown
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(value) < 1000 or unit == "TB":
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.2f} {unit}" if abs(value) < 10 else f"{value:.1f} {unit}" if abs(value) < 100 \
                else f"{value:.0f} {unit}"
        value /= 1000
    return f"{value:.0f} TB"  # not reached
