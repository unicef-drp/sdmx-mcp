// Builds the Ankara deck from stats.json + proprietary.json.
// Numbers are read, never typed: re-run stats.py and rebuild to refresh.
const path = require("path");
const fs = require("fs");
const pptxgen = require("pptxgenjs");
const { applyTheme } = require(path.join(
  process.env.HOME,
  ".claude/skills/synced/0bad8595-8858-485a-8219-0675b5da4f4a_055caf2d-1f33-40fa-a7b5-3c433b88242f/pptx/scripts/apply_theme.js"
));

const DIR = __dirname;
// Computed figures come from wherever the analysis layer wrote them, which is
// the run directory rather than this folder. One variable for the whole chain:
// build_pdf.py and context.py read the same SDMX_EVAL_DATA.
const DATA = process.env.SDMX_EVAL_DATA
  ? path.resolve(DIR, process.env.SDMX_EVAL_DATA)
  : DIR;
const S = JSON.parse(fs.readFileSync(path.join(DATA, "stats.json"), "utf8"));
const POOLED = JSON.parse(fs.readFileSync(path.join(DATA, "pooled.json"), "utf8"));

// Deep navy dominant, UNICEF cyan as the single sharp accent, amber and red
// reserved for "this went wrong" so colour carries meaning across slides.
const THEME = {
  name: "SDMX MCP Ankara",
  headFontFace: "Cambria",
  bodyFontFace: "Calibri",
  colors: {
    dk1: "0B1F33", lt1: "FFFFFF",
    dk2: "1E3A52", lt2: "EEF4F8",
    accent1: "1CABE2", accent2: "0B6E99", accent3: "E2A32D",
    accent4: "C5283D", accent5: "2E8B57", accent6: "6B7A8F",
    hlink: "1CABE2", folHlink: "0B6E99",
  },
};

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE"; // 13.333 x 7.5 — set before any slide
pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
pres.author = "Yves Jaques";
pres.title = "A Year of SDMX MCP";
pres.subject = "Cost, capability and constraint in putting an LLM over a statistical registry";
const C = pres.SchemeColor;

// ---- slides come from master.md -------------------------------------------
// The words live in the markdown; the visual treatment for each `layout:`
// lives here. One file to edit, two deliverables out of it.
const CTX = JSON.parse(fs.readFileSync(path.join(DIR, "context.json"), "utf8"));

function fill(text) {
  return text.replace(/\{\{([a-z0-9_]+)\}\}/g, (_, k) => {
    if (!(k in CTX)) {
      throw new Error(`master.md: unresolved placeholder {{${k}}} — add it to context.py`);
    }
    return CTX[k];
  });
}

function parseOutline() {
  let md = fs.readFileSync(path.join(DIR, "master.md"), "utf8");
  md = md.replace(/<!--[\s\S]*?-->/g, "");               // comments first:
  const parts = md.split(/^# Presentation outline\s*$/m);  // the notes mention
  if (parts.length < 2) throw new Error("master.md has no '# Presentation outline'");
  const slides = [];
  for (const block of parts[1].split(/^### /m).slice(1)) {
    const lines = block.split("\n");
    const slide = { title: fill(lines[0].trim()), notes: "", opts: {} };
    for (const line of lines.slice(1)) {
      const d = line.match(/^-\s*(\w+):\s*(.+)$/);
      if (d) { slide.opts[d[1]] = fill(d[2].trim()); continue; }
      const n = line.match(/^>\s*(.+)$/);
      if (n) slide.notes += (slide.notes ? " " : "") + fill(n[1].trim());
    }
    if (!slide.opts.layout) throw new Error(`slide "${slide.title}" has no layout:`);
    slides.push(slide);
  }
  return slides;
}

const SLIDES = parseOutline();
function slideFor(layout) {
  const s = SLIDES.find(x => x.opts.layout === layout);
  if (!s) throw new Error(`master.md has no slide with layout: ${layout}`);
  return s;
}

const FOOTER = "SDMX Experts Conference, Ankara — December 2026";

// ---------------------------------------------------------------- layouts
pres.defineSlideMaster({
  title: "TITLE_DARK",
  background: { color: THEME.colors.dk1 },
  objects: [
    { placeholder: { options: { name: "title", type: "title", x: 0.9, y: 2.3, w: 11.5, h: 1.5,
      fontSize: 46, bold: true, color: C.background1 }, text: "" } },
    { placeholder: { options: { name: "body", type: "body", x: 0.9, y: 3.9, w: 11.5, h: 1.6,
      fontSize: 18, color: THEME.colors.accent1 }, text: "" } },
  ],
});

pres.defineSlideMaster({
  title: "SECTION",
  background: { color: THEME.colors.dk2 },
  objects: [
    { placeholder: { options: { name: "title", type: "title", x: 0.9, y: 2.8, w: 11.5, h: 1.2,
      fontSize: 40, bold: true, color: C.background1 }, text: "" } },
    { placeholder: { options: { name: "body", type: "body", x: 0.9, y: 4.1, w: 11.5, h: 1.2,
      fontSize: 17, color: THEME.colors.accent1 }, text: "" } },
  ],
});

pres.defineSlideMaster({
  title: "CONTENT",
  background: { color: C.background1 },
  slideNumber: { x: 12.6, y: 7.05, fontSize: 10, color: THEME.colors.accent6 },
  objects: [
    { placeholder: { options: { name: "title", type: "title", x: 0.6, y: 0.42, w: 12.1, h: 0.75,
      fontSize: 32, bold: true, color: C.text1 }, text: "" } },
    { placeholder: { options: { name: "body", type: "body", x: 0.6, y: 1.35, w: 12.1, h: 5.4,
      fontSize: 15, color: C.text1 }, text: "" } },
    { text: { text: FOOTER, options: { x: 0.6, y: 7.05, w: 7, h: 0.3, fontSize: 10,
      color: THEME.colors.accent6, isTextBox: true, margin: 0 } } },
  ],
});

pres.defineSlideMaster({
  title: "CHART",
  background: { color: C.background1 },
  slideNumber: { x: 12.6, y: 7.05, fontSize: 10, color: THEME.colors.accent6 },
  objects: [
    { placeholder: { options: { name: "title", type: "title", x: 0.6, y: 0.42, w: 12.1, h: 0.75,
      fontSize: 32, bold: true, color: C.text1 }, text: "" } },
    { placeholder: { options: { name: "chart", type: "chart", x: 0.6, y: 1.35, w: 8.1, h: 5.1,
      color: C.text1 }, text: "" } },
    { placeholder: { options: { name: "body", type: "body", x: 9.0, y: 1.35, w: 3.7, h: 5.1,
      fontSize: 14, color: C.text1 }, text: "" } },
    { text: { text: FOOTER, options: { x: 0.6, y: 7.05, w: 7, h: 0.3, fontSize: 10,
      color: THEME.colors.accent6, isTextBox: true, margin: 0 } } },
  ],
});

// ------------------------------------------------------------ chart styling
// One styling object per chart — pptxgenjs mutates options in place.
function chartBase() {
  return {
    showLegend: false,
    catAxisLabelColor: THEME.colors.dk2,
    valAxisLabelColor: THEME.colors.dk2,
    catAxisLabelFontSize: 11,
    valAxisLabelFontSize: 11,
    catAxisLabelFontFace: "+mn-lt",
    valAxisLabelFontFace: "+mn-lt",
    dataLabelFontFace: "+mn-lt",
    dataLabelFontSize: 11,
    valGridLine: { color: "DCE6EC", size: 1 },
    catGridLine: { style: "none" },
    showTitle: false,
  };
}

// --------------------------------------------------------------- helpers
function statBlock(slide, { x, y, w, value, label, color }) {
  slide.addText(value, {
    x, y, w, h: 0.95, fontSize: 48, bold: true, color: color || THEME.colors.accent1,
    align: "left", isTextBox: true, margin: 0, objectName: `stat-${label}`,
  });
  slide.addText(label, {
    x, y: y + 0.92, w, h: 0.6, fontSize: 12, color: THEME.colors.accent6,
    align: "left", isTextBox: true, margin: 0, charSpacing: 0.6,
  });
}

function bullets(items) {
  return items.map((t, i) => ({
    text: t,
    options: { bullet: true, breakLine: i !== items.length - 1, paraSpaceAfter: 8 },
  }));
}

// =================================================================== SLIDES

pres.addSection({ title: "Opening" });

// --- 1. title -------------------------------------------------------
{
  const s = pres.addSlide({ masterName: "TITLE_DARK", sectionTitle: slideFor("title").opts.section });
  s.addText(slideFor("title").title, { placeholder: "title" });
  s.addText(slideFor("title").opts.subtitle, { placeholder: "body" });
  s.addText("Yves Jaques  ·  Chief, Geospatial and Computational Analytics Unit  ·  UNICEF Innocenti",
    { x: 0.9, y: 6.2, w: 11.5, h: 0.4, fontSize: 13, color: "8FA6B8", isTextBox: true, margin: 0 });
  s.addText(FOOTER, { x: 0.9, y: 6.6, w: 11.5, h: 0.4, fontSize: 11,
    color: THEME.colors.accent6, isTextBox: true, margin: 0 });
  s.addNotes(slideFor("title").notes);
}


pres.addSection({ title: "2 — The server" });

// --- 2. two-experiments ---------------------------------------------
{
  const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: slideFor("two-experiments").opts.section });
  s.addText(slideFor("two-experiments").title, { placeholder: "title" });
  s.addText([
    { text: "A read-only MCP server over any SDMX 2.1 registry", options: { bold: true, breakLine: true, fontSize: 17 } },
    { text: "31 tools · no data migration · institutional rules in the server, not in prompts", options: { fontSize: 14, color: THEME.colors.accent6 } },
  ], { x: 0.6, y: 1.3, w: 12.1, h: 0.9, isTextBox: true, margin: 0 });

  s.addShape(pres.ShapeType.roundRect, { x: 0.6, y: 2.4, w: 5.9, h: 3.6,
    fill: { color: THEME.colors.lt2 }, rectRadius: 0.08, objectName: "expA" });
  s.addText("Experiment A — is the server faithful?", { x: 0.95, y: 2.65, w: 5.2, h: 0.4,
    fontSize: 15, bold: true, color: THEME.colors.dk2, isTextBox: true, margin: 0 });
  statBlock(s, { x: 0.95, y: 3.15, w: 5.2, value: S.fidelity.pct + "%", label: "OF 466,360 OBSERVATIONS MATCHED THE REGISTRY" });
  s.addText("No model involved. Deterministic, repeatable, 2.9 minutes.",
    { x: 0.95, y: 5.25, w: 5.2, h: 0.6, fontSize: 13, color: THEME.colors.dk2, isTextBox: true, margin: 0 });

  s.addShape(pres.ShapeType.roundRect, { x: 6.8, y: 2.4, w: 5.9, h: 3.6,
    fill: { color: THEME.colors.lt2 }, rectRadius: 0.08, objectName: "expB" });
  s.addText("Experiment B — can a model drive it?", { x: 7.15, y: 2.65, w: 5.2, h: 0.4,
    fontSize: 15, bold: true, color: THEME.colors.dk2, isTextBox: true, margin: 0 });
  statBlock(s, { x: 7.15, y: 3.15, w: 5.2, value: "500", label: "NATURAL-LANGUAGE QUESTIONS, FIVE PROMPT STYLES" });
  s.addText("Graded against the registry's own value, refreshed at grading time.",
    { x: 7.15, y: 5.25, w: 5.2, h: 0.6, fontSize: 13, color: THEME.colors.dk2, isTextBox: true, margin: 0 });
  s.addNotes(slideFor("two-experiments").notes);
}


pres.addSection({ title: "3 — Generalising" });

// --- 3. three-columns -----------------------------------------------
{
  const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: slideFor("three-columns").opts.section });
  s.addText(slideFor("three-columns").title, { placeholder: "title" });
  s.addText("A fair objection. The answer is not model capability — it is cost, containment and provenance.",
    { x: 0.6, y: 1.3, w: 12.1, h: 0.4, fontSize: 15, color: THEME.colors.accent6, isTextBox: true, margin: 0 });

  const cols = [
    ["Cost", THEME.colors.accent1,
      "A frontier model re-derives SDMX structure on every request. The MCP spends that once, in the schema.",
      CTX.cost_per_question + " per question"],
    ["Containment", THEME.colors.accent3,
      "Scope enforcement rejected every fabricated dataflow a weak model invented. Institutional rules live in the server, not in a prompt.",
      "0 invented flows served"],
    ["Provenance", THEME.colors.accent5,
      "Every answer carries its tool calls, query URL and filters — which is how we proved a model right and our own ground truth stale.",
      "Query log, not prose"],
  ];
  cols.forEach((c, i) => {
    const x = 0.6 + i * 4.12;
    s.addShape(pres.ShapeType.ellipse, { x, y: 2.0, w: 0.46, h: 0.46,
      fill: { color: c[1] }, objectName: `dot${i}` });
    s.addText(c[0], { x: x + 0.62, y: 2.0, w: 3.0, h: 0.46, fontSize: 20, bold: true,
      color: THEME.colors.dk1, isTextBox: true, margin: 0, valign: "middle" });
    s.addText(c[2], { x, y: 2.7, w: 3.78, h: 2.2, fontSize: 14,
      color: THEME.colors.dk2, isTextBox: true, margin: 0 });
    s.addText(c[3], { x, y: 4.95, w: 3.78, h: 0.5, fontSize: 16, bold: true,
      color: c[1], isTextBox: true, margin: 0 });
  });
  s.addText("The MCP does not add capability. It converts capability into something affordable, governable and checkable.",
    { x: 0.6, y: 5.85, w: 12.1, h: 0.6, fontSize: 16, bold: true, color: THEME.colors.dk1,
      isTextBox: true, margin: 0 });
  s.addNotes(slideFor("three-columns").notes);
}


pres.addSection({ title: "4 — Measuring it" });

// --- 4. chart-spread ------------------------------------------------
{
  const s = pres.addSlide({ masterName: "CHART", sectionTitle: slideFor("chart-spread").opts.section });
  s.addText(slideFor("chart-spread").title, { placeholder: "title" });

  const bySpread = [...POOLED.strata].sort(
    (a, b) => POOLED.spread[a.key].range - POOLED.spread[b.key].range);
  s.addChart(pres.ChartType.bar, [{
    name: "Spread between models (percentage points)",
    labels: bySpread.map(r => r.label),
    values: bySpread.map(r => POOLED.spread[r.key].range),
  }], Object.assign(chartBase(), {
    placeholder: "chart",
    barDir: "col",
    chartColors: bySpread.map(r => (POOLED.spread[r.key].range <= 4
      ? THEME.colors.accent5 : POOLED.spread[r.key].range >= 12
      ? THEME.colors.accent4 : THEME.colors.accent1)),
    showValue: true,
    dataLabelPosition: "outEnd",
    dataLabelFormatCode: '0.0" pts"',
    valAxisMinVal: 0,
    barGapWidthPct: 45,
  }));

  const sp = POOLED.spread;
  s.addText([
    { text: POOLED.models + " models, " + POOLED.cases.toLocaleString() + " graded cases, one manifest.", options: { fontSize: 13, color: THEME.colors.accent6, breakLine: true, paraSpaceAfter: 12 } },
    { text: "Pass rates are similar across strata — 93% to 97%. What changes is how far the models spread apart.", options: { fontSize: 14, breakLine: true, paraSpaceAfter: 12 } },
    { text: "prescriptive: " + sp.prescriptive.min + "%–" + sp.prescriptive.max + "%", options: { fontSize: 13, bullet: true, breakLine: true } },
    { text: "ambiguous: " + sp.ambiguous.min + "%–" + sp.ambiguous.max + "%", options: { fontSize: 13, bullet: true, breakLine: true, paraSpaceAfter: 12 } },
    { text: "Nearly five times the spread, on the same six models.", options: { fontSize: 14, bold: true, breakLine: true, paraSpaceAfter: 12 } },
    { text: "A prescriptive-only benchmark cannot tell these models apart.", options: { fontSize: 14, bold: true, color: THEME.colors.accent4 } },
  ], { placeholder: "body" });
  s.addNotes(slideFor("chart-spread").notes);
}


pres.addSection({ title: "5 — Cost" });

// --- 5. chart-cost --------------------------------------------------
{
  const s = pres.addSlide({ masterName: "CHART", sectionTitle: slideFor("chart-cost").opts.section });
  s.addText(slideFor("chart-cost").title, { placeholder: "title" });

  const costRows = S.models
    .map(m => ({ name: m.model.split("/")[1], cost: m.cost_per_500, acc: m.accuracy }))
    .sort((a, b) => a.cost - b.cost);

  // The outlier is 21x the cheapest, so one linear axis renders nine models as
  // identical stubs and a log axis did not survive rendering. Chart the models
  // that are comparable with each other; the outlier gets its own callout,
  // which makes the ratio the point rather than hiding it.
  // Threshold from the data, not a constant: anything past this gets its own
  // callout so the rest do not render as identical stubs. A fixed $10 silently
  // stopped separating anything once the dearest model measured $6.
  const OUTLIER = 3;
  const cheap = costRows.filter(r => r.cost < OUTLIER);
  const dear = costRows.filter(r => r.cost >= OUTLIER);

  s.addChart(pres.ChartType.bar, [{
    name: "USD per 500 questions",
    labels: cheap.map(r => r.name),
    values: cheap.map(r => r.cost),
  }], Object.assign(chartBase(), {
    x: 0.6, y: 1.35, w: 8.1, h: 4.9,
    barDir: "bar",
    chartColors: cheap.map(() => THEME.colors.accent1),
    showValue: true,
    dataLabelPosition: "outEnd",
    dataLabelFormatCode: '$#,##0.00',
    valAxisMinVal: 0,
    valAxisMaxVal: Math.ceil(Math.max(...cheap.map(r => r.cost)) * 4) / 4,
    valAxisTitle: "USD per 500 questions",
    showValAxisTitle: true,
    valAxisTitleFontSize: 10,
    valAxisTitleColor: THEME.colors.accent6,
    barGapWidthPct: 35,
  }));

  // Reference price for the callouts: cheapest model at the best measured
  // accuracy. Using the cheapest model outright invited a comparison against
  // one that returned almost no answers, so "60x the cheapest" meant nothing.
  const bestAcc = Math.max(...S.models.map(m => m.accuracy));
  const refCost = Math.min(...S.models.filter(m => m.accuracy === bestAcc)
                                      .map(m => m.cost_per_500));

  dear.forEach((r, i) => {
    const y = 6.42 + i * 0.6;
    s.addShape(pres.ShapeType.roundRect, { x: 0.6, y, w: 8.1, h: 0.52,
      fill: { color: "FBEAEC" }, rectRadius: 0.06, objectName: `outlier${i}`, h: 0.52 });
    s.addText([
      { text: r.name + "   ", options: { fontSize: 14, bold: true, color: THEME.colors.accent4 } },
      { text: "$" + r.cost.toFixed(2) + "  —  off this scale, " +
              (r.cost / refCost).toFixed(1) + "× the cheapest model at " +
              bestAcc + "%" + (r.acc < bestAcc ? ", and " + (bestAcc - r.acc).toFixed(1) + " points behind it" : ""),
        options: { fontSize: 13, color: THEME.colors.dk2 } },
    ], { x: 0.85, y, w: 7.6, h: 0.52, isTextBox: true, margin: 0, valign: "middle" });
  });

  // Derived, so the panel cannot keep asserting a comparison the data no longer
  // supports: this previously quoted a model that has since been dropped.
  const best = Math.max(...S.models.map(m => m.accuracy));
  const tier = S.models.filter(m => m.accuracy === best)
                       .sort((a, b) => a.cost_per_500 - b.cost_per_500);
  const cheapBest = tier[0];
  const dearest = S.models.reduce((a, b) => (b.cost_per_500 > a.cost_per_500 ? b : a));
  const ratio = (dearest.cost_per_500 / cheapBest.cost_per_500).toFixed(1);
  const gap = (cheapBest.accuracy - dearest.accuracy).toFixed(1);

  s.addText([
    { text: "Price predicts nothing", options: { fontSize: 12, bold: true, color: THEME.colors.accent6, breakLine: true, charSpacing: 0.8, paraSpaceAfter: 8 } },
    { text: `$${cheapBest.cost_per_500.toFixed(2)} → ${cheapBest.accuracy}%`, options: { fontSize: 19, bold: true, color: THEME.colors.accent1, breakLine: true } },
    { text: `$${dearest.cost_per_500.toFixed(2)} → ${dearest.accuracy}%`, options: { fontSize: 19, bold: true, color: THEME.colors.accent4, breakLine: true, paraSpaceAfter: 10 } },
    { text: `${ratio}× the cost, ${gap} points worse.`, options: { fontSize: 15, bold: true, breakLine: true, paraSpaceAfter: 14 } },
    { text: `${tier.length} of ${S.totals.models} models reach ${best}%, from $${tier[0].cost_per_500.toFixed(2)} to $${tier[tier.length - 1].cost_per_500.toFixed(2)}.`, options: { fontSize: 13, breakLine: true, paraSpaceAfter: 18 } },
    { text: "100 analyst questions a day, for a year:", options: { fontSize: 14, breakLine: true } },
    { text: `≈ ${CTX.annual_cost} in model spend`, options: { fontSize: 20, bold: true, color: THEME.colors.accent5 } },
  ], { placeholder: "body" });
  s.addNotes(slideFor("chart-cost").notes);
}


pres.addSection({ title: "6 — A moving target" });

// --- 6. faults-table ------------------------------------------------
{
  const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: slideFor("faults-table").opts.section });
  s.addText(slideFor("faults-table").title, { placeholder: "title" });

  const rows = [
    [{ text: "Model", options: { bold: true } },
     { text: "What it exposed", options: { bold: true } },
     { text: "Owner", options: { bold: true } }],
    ["Claude Haiku 4.5", "Throttling reported as “no data”; bare decimals inviting rescaling", "MCP"],
    ["Claude Haiku 5.5", "93 right answers scored as unsettled — omitted a structured field", "Harness"],
    ["GPT-5-nano", "Needs ~3,400 output tokens; our 1,200 ceiling failed 46% of cases", "Harness"],
    ["GPT-5-mini", "Dropped a dimension, then abstained instead of acting on the refusal", "Model"],
    ["Gemini 2.5 Flash Lite", "Invented dataflow names — scope enforcement caught every one", "Model"],
    ["DeepSeek, GLM, Qwen-235b", "Our JSON mode silently suppressed tool calling in all three", "Harness"],
    ["GLM-4.7-flashx", "Passed filters as a string; invented a GENDER dimension", "Model"],
    ["Qwen-3-235b", "Emitted tool calls as plain text; obeyed a contradictory instruction", "Both"],
    ["Qwen at high concurrency", "Our server returning tool errors with empty messages", "MCP"],
  ];

  s.addTable(rows, {
    x: 0.6, y: 1.3, w: 12.1, colW: [2.6, 7.4, 2.1],
    fontSize: 12, fontFace: "Calibri", color: THEME.colors.dk1,
    border: { type: "solid", color: "DCE6EC", pt: 1 },
    fill: { color: "FFFFFF" },
    rowH: 0.42, valign: "middle",
    margin: 0.06,
  });
  s.addText("Most of what multi-model testing found was wrong with our code, not the models.",
    { x: 0.6, y: 6.25, w: 12.1, h: 0.5, fontSize: 16, bold: true, color: THEME.colors.accent4,
      isTextBox: true, margin: 0 });
  s.addNotes(slideFor("faults-table").notes);
}


// --- 7. four-cards --------------------------------------------------
{
  const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: slideFor("four-cards").opts.section });
  s.addText(slideFor("four-cards").title, { placeholder: "title" });
  s.addText("Five times a setting of ours produced a plausible low score we nearly published as a model limitation.",
    { x: 0.6, y: 1.3, w: 12.1, h: 0.5, fontSize: 16, isTextBox: true, margin: 0 });

  const cards = [
    ["77.6%", "was really 96.2%", "Omitted a structured field; all 93 flagged answers were exactly right"],
    ["46%", "of cases “failed”", "Token ceiling set below what a reasoning model needs to answer at all"],
    ["0", "tool calls", "Our JSON mode suppressed tool use in three of seven models"],
    ["94%", "ceiling on the easiest stratum", "Ground truth fifteen days stale; the registry had restated the data"],
  ];
  cards.forEach((c, i) => {
    const x = 0.6 + i * 3.08;
    s.addShape(pres.ShapeType.roundRect, { x, y: 2.1, w: 2.88, h: 3.5,
      fill: { color: THEME.colors.lt2 }, rectRadius: 0.08, objectName: `card${i}` });
    s.addText(c[0], { x: x + 0.22, y: 2.35, w: 2.44, h: 0.8, fontSize: 36, bold: true,
      color: THEME.colors.accent4, isTextBox: true, margin: 0 });
    s.addText(c[1], { x: x + 0.22, y: 3.12, w: 2.44, h: 0.4, fontSize: 12, bold: true,
      color: THEME.colors.dk2, isTextBox: true, margin: 0 });
    s.addText(c[2], { x: x + 0.22, y: 3.6, w: 2.44, h: 1.8, fontSize: 12,
      color: THEME.colors.dk2, isTextBox: true, margin: 0 });
  });
  s.addText("Every figure in this talk survived that attribution.",
    { x: 0.6, y: 5.95, w: 12.1, h: 0.5, fontSize: 16, bold: true, color: THEME.colors.dk1,
      isTextBox: true, margin: 0 });
  s.addNotes(slideFor("four-cards").notes);
}


pres.addSection({ title: "7 — Why MCP endures" });

// --- 8. office ------------------------------------------------------
{
  const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: slideFor("office").opts.section });
  s.addText(slideFor("office").title, { placeholder: "title" });

  s.addText([
    { text: "Deterministic retrieval, probabilistic interpretation", options: { fontSize: 18, bold: true, breakLine: true, paraSpaceAfter: 10 } },
    { text: "Because the deterministic layer carries the correctness guarantee, the probabilistic layer need not be excellent — only competent at code resolution and honest about failure. That competence is now cheap and widely available.", options: { fontSize: 14 } },
  ], { x: 0.6, y: 1.3, w: 7.9, h: 1.9, isTextBox: true, margin: 0 });

  s.addText(bullets([
    "One MCP server over an existing SDMX endpoint — no data migration, no warehouse change",
    "A cheap model, swappable by one environment variable",
    "Institutional rules in the server: which flows are public, which are draft, what a refusal means",
  ]), { x: 0.6, y: 3.35, w: 7.9, h: 1.8, fontSize: 14, color: C.text1, isTextBox: true, margin: 0 });

  s.addText([
    { text: "Still needs local work", options: { fontSize: 15, bold: true, color: THEME.colors.accent4, breakLine: true, paraSpaceAfter: 8 } },
    { text: "Codelists scoped per dataflow — a warehouse-side fix, and real work", options: { fontSize: 13, bullet: true, breakLine: true } },
    { text: "Model screening: capability at tool use varies far more than price", options: { fontSize: 13, bullet: true, breakLine: true } },
    { text: "Language — everything here was English against English labels", options: { fontSize: 13, bullet: true } },
  ], { x: 0.6, y: 5.25, w: 7.9, h: 1.6, isTextBox: true, margin: 0 });

  s.addShape(pres.ShapeType.roundRect, { x: 8.9, y: 1.3, w: 3.8, h: 3.5,
    fill: { color: THEME.colors.dk1 }, rectRadius: 0.08, objectName: "costcard" });
  s.addText("≈ $65", { x: 9.2, y: 1.95, w: 3.2, h: 1.0, fontSize: 48, bold: true,
    color: THEME.colors.accent1, isTextBox: true, margin: 0 });
  s.addText("a year in model spend, for 100 analyst questions a day",
    { x: 9.2, y: 3.0, w: 3.2, h: 1.2, fontSize: 14, color: "C9DCE8", isTextBox: true, margin: 0 });
  s.addText("No per-seat licence. No data egress to a vendor. No lock-in — ten models from five vendors ran through one server.",
    { x: 8.9, y: 5.0, w: 3.8, h: 1.6, fontSize: 13, color: THEME.colors.dk2, isTextBox: true, margin: 0 });
  s.addNotes(slideFor("office").notes);
}


pres.addSection({ title: "8 — Close" });

// --- 9. close -------------------------------------------------------
{
  const s = pres.addSlide({ masterName: "SECTION", sectionTitle: slideFor("close").opts.section });
  s.addText(slideFor("close").title, { placeholder: "title" });
  s.addText("The method should outlast the model names in it.", { placeholder: "body" });

  s.addText([
    { text: "PROVEN", options: { fontSize: 13, bold: true, color: THEME.colors.accent1, breakLine: true, charSpacing: 1 } },
    { text: "Server fidelity at " + CTX.fidelity_pct + " of " + CTX.fidelity_total + " observations", options: { fontSize: 14, color: "FFFFFF", bullet: true, breakLine: true } },
    { text: CTX.accuracy_band + " accuracy for about a dollar per 500 questions", options: { fontSize: 14, color: "FFFFFF", bullet: true, breakLine: true } },
    { text: "Models abstain rather than fabricate — " + CTX.negative_rate + " on absent data", options: { fontSize: 14, color: "FFFFFF", bullet: true } },
  ], { x: 0.9, y: 4.95, w: 5.6, h: 2.1, isTextBox: true, margin: 0, valign: "top" });

  s.addText([
    { text: "NOT PROVEN", options: { fontSize: 13, bold: true, color: THEME.colors.accent3, breakLine: true, charSpacing: 1 } },
    { text: "Non-English registries and non-English users", options: { fontSize: 14, color: "FFFFFF", bullet: true, breakLine: true } },
    { text: "Multi-step analysis across flows or computed rates", options: { fontSize: 14, color: "FFFFFF", bullet: true, breakLine: true } },
    { text: "Durability — the cheapest competent model changed twice this year", options: { fontSize: 14, color: "FFFFFF", bullet: true } },
  ], { x: 6.9, y: 4.95, w: 5.5, h: 2.1, isTextBox: true, margin: 0, valign: "top" });
  s.addNotes(slideFor("close").notes);
}



(async () => {
  const out = path.join(DIR, "SDMX-MCP-Ankara.pptx");
  await pres.writeFile({ fileName: out });
  await applyTheme(out, THEME);
  console.log("wrote " + out);
})();
