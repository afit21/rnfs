"""Format out/<exp>.json into markdown tables. Usage: python tables.py <exp> [cols...]"""
import json
import os
import sys

OUT = "out"  # relative: run from this folder (docs/simulations/README.md)

FMT = {
    "found": lambda v: f"{100 * v:.0f}%",
    "local": lambda v: f"{100 * v:.0f}%",
}


def fmt(col, v):
    if v is None:
        return "-"
    for suffix, f in FMT.items():
        if col.endswith(suffix):
            return f(v)
    if abs(v) >= 100:
        return f"{v:.0f}"
    return f"{v:.1f}"


def table(rows, keys, cols, title=None):
    lines = []
    if title:
        lines.append(f"**{title}**\n")
    lines.append("| " + " | ".join(keys + cols) + " |")
    lines.append("|" + "---|" * (len(keys) + len(cols)))
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(k)) for k in keys) + " | "
                     + " | ".join(fmt(c, r.get(c)) for c in cols) + " |")
    return "\n".join(lines)


def load(exp):
    with open(os.path.join(OUT, exp + ".json")) as f:
        return json.load(f)


if __name__ == "__main__":
    exp = sys.argv[1]
    cols = sys.argv[2].split(",")
    keys = sys.argv[3].split(",") if len(sys.argv) > 3 else ["S", "strat"]
    filt = dict(kv.split("=") for kv in sys.argv[4].split(",") if kv) if len(sys.argv) > 4 else {}
    rows = load(exp)
    rows = [r for r in rows if all(str(r.get(k)) == v for k, v in filt.items())]
    order = sys.argv[5].split(",") if len(sys.argv) > 5 else None
    def sk(r):
        out = []
        for k in keys:
            v = r[k]
            if isinstance(v, (int, float)):
                out.append((0, v, ""))
            else:
                out.append((1, order.index(v) if order and v in order else 99, str(v)))
        return out
    rows.sort(key=sk)
    print(table(rows, keys, cols))
