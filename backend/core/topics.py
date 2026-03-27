"""Topic slugs, human-readable labels, and brand colours for the observatory."""

from typing import Final

TOPIC_CLIMATE: Final[str] = "climate"
TOPIC_HEALTH: Final[str] = "health"
TOPIC_ECONOMICS: Final[str] = "economics"
TOPIC_POLITICS: Final[str] = "politics"
TOPIC_GENERAL: Final[str] = "general"

TOPIC_LABELS: Final[dict[str, str]] = {
    TOPIC_CLIMATE: "Climate",
    TOPIC_HEALTH: "Health",
    TOPIC_ECONOMICS: "Economics",
    TOPIC_POLITICS: "Politics",
    TOPIC_GENERAL: "General",
}

# Hex colours aligned with frontend/lib/topics.ts
TOPIC_COLOURS: Final[dict[str, str]] = {
    TOPIC_CLIMATE: "#1D9E75",
    TOPIC_HEALTH: "#D85A30",
    TOPIC_ECONOMICS: "#378ADD",
    TOPIC_POLITICS: "#7F77DD",
    TOPIC_GENERAL: "#888780",
}

ALL_TOPIC_SLUGS: Final[tuple[str, ...]] = (
    TOPIC_CLIMATE,
    TOPIC_HEALTH,
    TOPIC_ECONOMICS,
    TOPIC_POLITICS,
    TOPIC_GENERAL,
)


def label_for_slug(slug: str) -> str:
    """Return the display label for a topic slug, or the slug if unknown."""
    return TOPIC_LABELS.get(slug, slug)


def colour_for_slug(slug: str) -> str:
    """Return the hex colour for a topic slug, or general grey if unknown."""
    return TOPIC_COLOURS.get(slug, TOPIC_COLOURS[TOPIC_GENERAL])
