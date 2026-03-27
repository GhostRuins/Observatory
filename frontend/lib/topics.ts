/** Topic metadata shared with the backend colour palette. */

export type TopicSlug =
  | "climate"
  | "health"
  | "economics"
  | "politics"
  | "general";

export const TOPICS: { slug: TopicSlug; label: string; color: string }[] = [
  { slug: "climate", label: "Climate", color: "#1D9E75" },
  { slug: "health", label: "Health", color: "#D85A30" },
  { slug: "economics", label: "Economics", color: "#378ADD" },
  { slug: "politics", label: "Politics", color: "#7F77DD" },
  { slug: "general", label: "General", color: "#888780" },
];

export function topicColor(slug: string): string {
  const found = TOPICS.find((t) => t.slug === slug);
  return found?.color ?? "#888780";
}
