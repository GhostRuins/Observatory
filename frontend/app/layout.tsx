import type { Metadata } from "next";
import { Inter } from "next/font/google";

import { NavBar } from "@/components/NavBar";

import "./globals.css";

const inter = Inter({ subsets: ["latin"], variable: "--font-geist-sans" });

export const metadata: Metadata = {
  title: "Living Data Observatory",
  description:
    "Public data dashboard ingesting free sources daily with automated cleaning and charting.",
};

/**
 * Root layout applying global typography, navigation, and topic CSS variables.
 */
export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${inter.variable} min-h-screen bg-slate-950 font-sans antialiased`}
        style={
          {
            "--topic-climate": "#1D9E75",
            "--topic-health": "#D85A30",
            "--topic-economics": "#378ADD",
            "--topic-politics": "#7F77DD",
            "--topic-general": "#888780",
          } as React.CSSProperties
        }
      >
        <NavBar />
        <main className="mx-auto max-w-6xl px-4 py-8">{children}</main>
      </body>
    </html>
  );
}
