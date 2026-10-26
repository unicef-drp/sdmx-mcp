# The Ankara paper

Source for *A Year of SDMX MCP*, built from one file so the document and the
deck cannot disagree.

```
master.md        the single source: the document, then the slide outline
context.py       turns the analysis layer's JSON into {{placeholder}} values
build_pdf.py     master.md -> SDMX-MCP-Report.pdf   (via HTML and headless Chrome)
build_deck.js    master.md -> SDMX-MCP-Ankara.pptx  (via pptxgenjs)
```

Everything above the presentation-outline heading in `master.md` is the
document; everything below it supplies each slide's words. The visual treatment
for each slide layout lives in `build_deck.js`.

## Building it

The figures come from the analysis layer, so run that first and point the build
at its output:

```bash
python3 scripts/sdmx_eval_consolidate.py tmp/final tmp/sweep
python3 scripts/sdmx_eval_stats.py       tmp/final
python3 scripts/sdmx_eval_pooled.py      tmp/final
python3 scripts/sdmx_eval_variance.py    tmp/repeats

cd paper
export SDMX_EVAL_DATA=../tmp/final        # where stats.json and pooled.json are
python3 context.py                        # -> context.json
python3 build_pdf.py                      # needs the `markdown` package
node build_deck.js                        # needs `npm install pptxgenjs`
```

`variance.json` is read from `SDMX_EVAL_DATA` too; `SDMX_EVAL_ABLATION` and
`SDMX_EVAL_ROUTED` point at the ablation and routed-repeat runs if they are
somewhere other than `tmp/ablation` and `tmp/variance`.

## Why no figure is typed into the markdown

`master.md` carries `{{placeholders}}`, never literal numbers, and
`context.py` refuses to build on an unknown one — a slide reading
`{{top_accuracy}}` on a conference screen is worse than a build that stops.

This is not fastidiousness. Over one month of measurement the headline model
changed three times, a vendor dropped its price 8.7-fold for identical
accuracy, and six cases of ground truth were withdrawn by the registry
mid-study. Every hand-typed figure went stale, and several survived into drafts
after the number behind them had moved. The deck's cost slide asserted a
comparison against a model that had been dropped from the study entirely.

So the rule: if a figure is not yet a placeholder, add it to `context.py`
rather than inlining it.
