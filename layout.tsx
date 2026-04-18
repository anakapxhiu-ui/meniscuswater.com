import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Meniscus — Know what's in your water",
  description:
    "Enter your address. We pull EPA SDWIS, UCMR5, Superfund, and state data to tell you exactly what's in your tap water — and what to do about it.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <link
          href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,500;0,9..144,700;1,9..144,500&family=Inter:wght@400;500;600&display=swap"
          rel="stylesheet"
        />
      </head>
      <body className="bg-surface text-ink font-body antialiased">{children}</body>
    </html>
  );
}
