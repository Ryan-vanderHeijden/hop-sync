"""Small formatting helpers shared by the core messages."""


def human_size(num: float) -> str:
    """1536 -> '1.5 KB'."""
    value = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.0f} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
