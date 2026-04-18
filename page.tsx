"use client";

import { useState } from "react";

type SeverityLabel = "not detected" | "trace" | "elevated" | "above legal limit" | "significantly above legal limit";

type Finding = {
  code: string;
  name: string;
  value: number;
  unit: string;
  severity: 0 | 1 | 2 | 3 | 4;
  severity_label: SeverityLabel;
  ratio_to_guideline?: number | null;
  ratio_to_mcl?: number | null;
  summary: string;
};

type LookupResponse = {
  address: string;
  normalized_address: string | null;
  utility: {
    name: string;
    state: string;
    source_type: string;
    population_served: number;
  } | null;
  water_profile: {
    max_severity: 0 | 1 | 2 | 3 | 4;
    max_severity_label: string;
    headline: string;
    by_category: Record<string, Finding[]>;
  };
  narrative: string;
  recommended_products: Array<{
    brand: string;
    model: string;
    form_factor: string;
    price_usd: number;
    coverage_pct: number;
    match_reason: string;
    affiliate_url_template?: string;
  }>;
};

const severityTone: Record<number, { bg: string; text: string; label: string }> = {
  0: { bg: "bg-severity-0/10", text: "text-severity-0", label: "No concern" },
  1: { bg: "bg-severity-1/10", text: "text-severity-1", label: "Within guidelines" },
  2: { bg: "bg-severity-2/10", text: "text-severity-2", label: "Above guidelines" },
  3: { bg: "bg-severity-3/10", text: "text-severity-3", label: "Above legal limit" },
  4: { bg: "bg-severity-4/10", text: "text-severity-4", label: "Urgent" },
};

export default function Home() {
  const [address, setAddress] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<LookupResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function lookUp(e: React.FormEvent) {
    e.preventDefault();
    if (!address.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch(`${process.env.NEXT_PUBLIC_API_URL || ""}/api/lookup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ address }),
      });
      if (!resp.ok) throw new Error(`API error ${resp.status}`);
      setResult(await resp.json());
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Something went wrong");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="min-h-screen">
      {/* Hero */}
      <section className="bg-ink text-white">
        <div className="mx-auto max-w-5xl px-6 py-20 md:py-28">
          <div className="mb-3 text-sm font-medium uppercase tracking-wider text-meniscus-200">
            Meniscus
          </div>
          <h1 className="font-display text-5xl font-bold leading-tight md:text-7xl">
            Know what&apos;s in <br /> your water.
          </h1>
          <p className="mt-6 max-w-2xl text-lg text-meniscus-100">
            Enter your address. We pull EPA data, translate it into plain English,
            and match you to the filter or installer that actually removes what&apos;s
            in your tap water.
          </p>

          <form
            onSubmit={lookUp}
            className="mt-10 flex flex-col gap-3 md:flex-row md:gap-2"
          >
            <input
              type="text"
              required
              value={address}
              onChange={(e) => setAddress(e.target.value)}
              placeholder="123 Main St, Austin, TX 78747"
              className="flex-1 rounded-lg bg-white px-5 py-4 text-ink placeholder-ink-muted/60 outline-none ring-meniscus-400 focus:ring-2"
            />
            <button
              type="submit"
              disabled={loading}
              className="rounded-lg bg-meniscus-500 px-6 py-4 font-semibold text-white transition hover:bg-meniscus-400 disabled:opacity-50"
            >
              {loading ? "Checking…" : "Check my water"}
            </button>
          </form>
          {error && <p className="mt-4 text-sm text-red-200">{error}</p>}

          <p className="mt-4 text-sm text-meniscus-200">
            Free. Sources: EPA SDWIS, UCMR5, Superfund, and state agencies.
          </p>
        </div>
      </section>

      {/* Result */}
      {result && (
        <section className="mx-auto max-w-5xl px-6 py-12">
          <ReportView result={result} />
        </section>
      )}

      {/* How it works */}
      {!result && (
        <section className="mx-auto max-w-5xl px-6 py-16">
          <h2 className="font-display text-3xl font-bold text-ink">How it works</h2>
          <div className="mt-8 grid gap-6 md:grid-cols-3">
            {[
              { n: "01", t: "Know", d: "Enter your address. Instant utility + risk profile." },
              { n: "02", t: "Test", d: "We pick the right lab panel — not the $600 kitchen-sink kit." },
              { n: "03", t: "Solve", d: "Matched to the exact filter or local installer that fits your water." },
            ].map((s) => (
              <div key={s.n} className="rounded-2xl bg-meniscus-50 p-6">
                <div className="font-display text-3xl font-bold text-meniscus-500">{s.n}</div>
                <div className="mt-2 font-display text-xl font-bold text-ink">{s.t}</div>
                <p className="mt-2 text-ink-muted">{s.d}</p>
              </div>
            ))}
          </div>
        </section>
      )}

      <footer className="border-t border-meniscus-100 bg-meniscus-50 py-8">
        <div className="mx-auto max-w-5xl px-6 text-sm text-ink-muted">
          Meniscus · Data from EPA SDWIS, UCMR5, SEMS, and state agencies · Not medical advice.
        </div>
      </footer>
    </main>
  );
}

function ReportView({ result }: { result: LookupResponse }) {
  const tone = severityTone[result.water_profile.max_severity];

  return (
    <div className="space-y-8">
      {/* Headline */}
      <div>
        <div className="text-sm uppercase tracking-wide text-ink-muted">
          Report for {result.normalized_address || result.address}
        </div>
        {result.utility && (
          <div className="mt-1 text-sm text-ink-muted">
            {result.utility.name} · {result.utility.source_type} · serves{" "}
            {result.utility.population_served.toLocaleString()}
          </div>
        )}
        <div className={`mt-6 inline-block rounded-lg px-3 py-1 text-sm font-semibold ${tone.bg} ${tone.text}`}>
          {result.water_profile.max_severity_label}
        </div>
        <h2 className="mt-3 font-display text-3xl font-bold text-ink md:text-4xl">
          {result.water_profile.headline}
        </h2>
      </div>

      {/* Narrative */}
      <div className="prose prose-lg max-w-none text-ink-muted">
        {result.narrative.split("\n\n").map((p, i) => (
          <p key={i}>{p}</p>
        ))}
      </div>

      {/* Findings by category */}
      {Object.entries(result.water_profile.by_category).map(([cat, findings]) => (
        <div key={cat} className="rounded-2xl border border-meniscus-100 p-6">
          <h3 className="font-display text-xl font-bold capitalize text-ink">
            {cat.replace(/_/g, " ")}
          </h3>
          <div className="mt-4 divide-y divide-meniscus-100">
            {findings.map((f) => (
              <div key={f.code} className="flex items-center justify-between py-3">
                <div className="flex items-center gap-3">
                  <div className={`h-2 w-2 rounded-full ${severityTone[f.severity].bg.replace("/10", "")}`} />
                  <div className="font-medium text-ink">{f.name}</div>
                </div>
                <div className="text-right text-sm text-ink-muted">
                  {f.value} {f.unit}
                  {f.ratio_to_guideline && f.ratio_to_guideline > 1 && (
                    <span className="ml-2 font-semibold text-severity-2">
                      {f.ratio_to_guideline.toFixed(1)}× guideline
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}

      {/* Products */}
      {result.recommended_products.length > 0 && (
        <div>
          <h3 className="font-display text-2xl font-bold text-ink">Recommended for your water</h3>
          <div className="mt-4 grid gap-4 md:grid-cols-2">
            {result.recommended_products.slice(0, 4).map((p, i) => (
              <div key={i} className="rounded-2xl border border-meniscus-100 p-5">
                <div className="text-xs uppercase tracking-wide text-meniscus-500">
                  {p.form_factor.replace(/_/g, " ")}
                </div>
                <div className="mt-1 font-display text-lg font-bold text-ink">
                  {p.brand} {p.model}
                </div>
                <div className="mt-2 text-sm text-ink-muted">{p.match_reason}</div>
                <div className="mt-4 flex items-center justify-between">
                  <div className="font-semibold text-ink">${p.price_usd}</div>
                  <div className="text-xs text-meniscus-500">{p.coverage_pct}% cert match</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
