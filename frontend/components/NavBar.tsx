import Link from "next/link";

import { TOPICS } from "@/lib/topics";

/**
 * Top navigation with topic shortcuts aligned to backend slugs.
 */
export function NavBar() {
  return (
    <header className="border-b border-white/10 bg-black/20 backdrop-blur">
      <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3 px-4 py-4">
        <Link href="/" className="text-lg font-semibold tracking-tight text-white">
          Living Data Observatory
        </Link>
        <nav className="flex flex-wrap items-center gap-2 text-sm">
          <Link
            href="/"
            className="rounded-full px-3 py-1 text-slate-200 hover:bg-white/10"
          >
            All topics
          </Link>
          {TOPICS.map((t) => (
            <Link
              key={t.slug}
              href={`/${t.slug}`}
              className="rounded-full px-3 py-1 hover:bg-white/10"
              style={{ color: t.color }}
            >
              {t.label}
            </Link>
          ))}
        </nav>
      </div>
    </header>
  );
}
