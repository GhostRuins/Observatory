import Link from "next/link";

import { TOPICS, type TopicSlug } from "@/lib/topics";

type TopicFilterProps = {
  /** Currently active topic slug, or all. */
  active: TopicSlug | "all";
};

/**
 * Pill buttons for switching topic views via App Router routes.
 */
export function TopicFilter({ active }: TopicFilterProps) {
  const items: { slug: TopicSlug | "all"; label: string; href: string }[] = [
    { slug: "all", label: "All", href: "/" },
    ...TOPICS.map((t) => ({ slug: t.slug, label: t.label, href: `/${t.slug}` })),
  ];

  return (
    <div className="flex flex-wrap gap-2">
      {items.map((item) => {
        const isActive = item.slug === active;
        const color =
          item.slug === "all"
            ? "#e5e7eb"
            : TOPICS.find((t) => t.slug === item.slug)?.color ?? "#e5e7eb";
        return (
          <Link
            key={item.slug}
            href={item.href}
            className={`rounded-full border px-3 py-1 text-sm transition ${
              isActive
                ? "border-white/40 bg-white/10"
                : "border-white/10 bg-black/20 hover:bg-white/5"
            }`}
            style={{ color }}
          >
            {item.label}
          </Link>
        );
      })}
    </div>
  );
}
