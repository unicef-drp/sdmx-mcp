# A Year of SDMX MCP

> subtitle: What it takes for a statistical office to stay the source of record
> author: Yves Jaques, Chief, Geospatial and Computational Analytics Unit, UNICEF Innocenti
> with: Claude (Anthropic)
> repo: github.com/unicef-drp/sdmx-mcp
> endpoint: https://sdmx-mcp.fly.dev/mcp
> status: A working tool, publicly reachable over UNICEF's public data. Not an official UNICEF product, and not yet carrying institutional endorsement.

<!--
SINGLE SOURCE. The PDF and the PPTX are both generated from this file.

  python3 build_pdf.py     -> SDMX-MCP-Report.pdf   (the document, for reading)
  node build_deck.js       -> SDMX-MCP-Ankara.pptx  (the talk, for presenting)

Figures in {{double braces}} are substituted from stats.json, which is computed
from the graded eval output by stats.py. Never type a number here by hand: if a
figure is not yet a placeholder, add it to stats.py rather than inlining it, or
the two deliverables will drift apart.

Everything above the presentation-outline heading is the document. Everything
below it is the talk. The outline supplies each slide's words; the visual
treatment for each layout lives in build_deck.js.

Structure follows the arc, not a list of findings: the problem, the server, the
decision to generalise it, what measuring it properly took, what it costs,
what kept moving, why the deterministic layer endures, and why the work does
not finish. The analysis sits where the story reaches it.
-->

A language model asked for a country's under-five mortality rate can return a figure with nothing
behind it, and nothing in the answer will say whether it was retrieved, remembered or invented. This
is a report on a year spent building and measuring one answer to that problem: a generic MCP server
over an SDMX 2.1 registry, which lets a cheap model answer analyst questions at **{{top_accuracy}}
agreement with the registry for {{top_cost}} per five hundred questions**. {{tier_note}}

## Why we started

The first version of this problem was not a benchmark. It was watching a model answer a question
about child mortality with a figure that did not come from anywhere.

That matters more for official statistics than for most domains. A wrong answer about a film's
release date is a nuisance. A wrong answer about stunting prevalence in a specific country-year
enters a brief, then a slide, then a decision — and it arrives with exactly the same fluency as the
correct figure. The institutions that spend decades establishing what the number *is* have no
mechanism for contesting a number that was never sourced.

The risk is not that models are useless on this material. It is that they are useful enough to be
trusted, and a statistical office that cannot put its own data in front of them does not get to opt
out of the conversation. It simply loses authorship of it.

That is no longer hypothetical. João Pedro Azevedo's stocktake counted **92 MCP servers giving AI
agents access to official statistics** as of 27 October 2026, built in about two years: 9 official,
3 independent, 1 commercial and 79 community-built. Most are not run by the offices that produce the
statistics. Peers have started, among them India's MoSPI (in beta since February 2026), Singapore's
SingStat, the US Census Bureau, and France's data.gouv.fr, whose experimental server comes with its
own warning about unofficial servers presenting themselves as linked to the platform. Grounding has
also been shown to work elsewhere: a US federal pilot reported accuracy on two public datasets
reaching 95% with MCP servers, up from 18% in the summary of its own report, and from 0% and 2.1%
by dataset in FedScoop's account.

Azevedo's benchmark of three MCP servers sharpens the point about authorship. Within the same
questions, the UN System Data Commons delivered 37.9 percentage points less of a fifteen-element
metadata standard than this server, and World Bank Data360 31.6 points less; the Data Commons
credits "United Nations" on every UNICEF facet. An office that does not serve its own data loses
attribution, not only authorship.

We should be careful how much weight the fabrication story carries. The unicefstats-mcp benchmark,
as Azevedo reports it, found that one model without tools scored poorly overall but fabricated on
only 2.5% of questions about absent data. Our own test cannot settle the matter, for reasons the
section on measurement sets out. The firmer ground for this work is accuracy and provenance.

So the question was narrow and practical: can an agent be made to answer from the registry rather
than from memory, cheaply enough that an office could actually run the service?

## A server over the data warehouse

The answer took the form of an MCP server: a read-only client of UNICEF's SDMX 2.1 warehouse,
exposing the registry's own structures — dataflows, dimensions, codelists, observations — as tools
an agent can call. Nothing is copied, nothing is migrated, and no figure is ever synthesised. The
server either returns what the registry holds or reports that it could not.

That property is testable without involving a model at all, and it is the foundation everything
else rests on. Every observation reachable in the global dataflow, compared against values read
directly from the SDMX REST API: {{fidelity_matched}} of {{fidelity_total}} matched,
**{{fidelity_pct}}**, in {{fidelity_minutes}} minutes.

What that proves has a limit. The warehouse is the source of record for UNICEF-custodian series. It
also republishes inter-agency estimates, such as those of IGME, the JMP and WUENIC, whose release of
record lies with the inter-agency group. Every figure in this report measures agreement with the
warehouse. Agreement with each custodian's own release is a separate question that we have not
tested.

This separation is the architectural claim of the whole project. **Retrieval is deterministic.**
Given a flow, a key and a period, the server returns exactly what the registry holds, and that can
be proven. **Interpretation is probabilistic**, and that is the only part a model is needed for.
Because the deterministic layer carries the correctness guarantee, the probabilistic layer does not
have to be excellent — only competent at resolving codes and honest about failure. That is a much
cheaper thing to buy.

## Making it generic, and what that forced us to learn

The server was built for UNICEF's registry. It was made generic on purpose, because an office that
cannot run it on its own warehouse has been handed a case study rather than a tool, and because the
problem is obviously not ours alone.

Generalising it turned out to be the most instructive engineering of the year, and not for the
reason we expected. The hard part was not supporting arbitrary SDMX. It was discovering how much of
what we had written down as *code* was really *configuration*.

**Dimensions carry roles, and the roles are what agents reason about.** Our code knew about
`INDICATOR`, `REF_AREA` and `TIME_PERIOD` because those are what our registry calls them. Another
agency's registry names them differently while meaning the same thing. The server now reasons in
terms of a **subject** role, a **geography** role and a **time** role, and which dimension plays
each is declared rather than assumed. Nothing else in the server has to know the local vocabulary.

**An agent needs a path from a topic to a code family.** A registry holds thousands of indicator
codes, and a question arrives as "child mortality", not as `CME_MRM0`. We ended up with a
configurable prefix map — {{theme_prefix_rows}} rows associating indicator-code prefixes with
subject domains — that gives an agent somewhere to start. It ships disabled by default, because a
map tuned to our coding conventions would be actively misleading on a registry that uses different
ones.

**Scope has to be declared, and then enforced.** An unrestricted agent considers every dataflow in
the registry, including drafts and internal flows it has no business reading. Scope is configured by
agency allowlist, dataflow prefix and regular expression, and enforcement switches on automatically
once any of it is set. This is not a tidiness feature. {{scope_rejections}} of the tool errors
recorded across the evaluation were scope rejections, and they include every dataflow name a weak
model invented outright.

**Configuring this is a deployment step, not an optimisation.** The dimension policy — which
dimension carries the subject role, which the geography, which the time; which tokens to ignore when
scoring a query; how candidate codes are ranked — cannot ship with useful universal defaults,
because the right answer is a property of your registry and of the questions your office actually
receives. Any office standing this up should expect to write it as part of deployment, in the same
breath as pointing the server at its own endpoint and declaring which dataflows are in scope.

What makes this worth a paragraph rather than a footnote is how it fails. An unconfigured server
does not refuse to start, and nothing in its output says anything is missing. It falls back to a
default that declares the three roles under generic names, with no codelist or hierarchy named as a
source for any of them, and no topic hints at all — so it runs without the resolution layer, which the ablation above shows is the
single most valuable thing it has. The deployment works. The answers are quietly much worse than the
software is capable of, and an office evaluating it would reasonably conclude the approach does not
work rather than that it was never configured.

It is a small piece of work — one JSON file, an afternoon with somebody who knows the registry's
conventions. It is also the difference between the numbers in this report and the numbers in the
ablation.

The general lesson, which we would offer to anyone building something similar: the registry-specific
parts of your server are not the parts that look registry-specific. They are the constants you never
thought to question, because your own warehouse made them feel like facts about SDMX.

## What it took to measure this properly

Measuring whether a model can drive the server is a different claim from whether the server is
faithful, and it fails in different ways. It needs questions whose correct answers are known, enough
of them to distinguish models, and a spread of question types — because a benchmark made only of
easy questions cannot tell models apart at all.

The manifest is 500 questions in plain language, stratified across five prompt styles, each graded
against the registry's own value:

| Stratum | n | What it tests |
| --- | --- | --- |
| prescriptive | 100 | The prompt names the tool and the exact filter codes. Pure execution |
| named-indicator | 175 | The registry's indicator label and a country name; the agent resolves the codes |
| ambiguous | 75 | A phrase with one defensible answer among sibling indicators |
| disaggregated | 75 | A specific breakdown (one sex, not the total) must be honoured |
| negative | 75 | The registry holds no value. The correct answer is to abstain |

Six further cases, whose series the registry withdrew during the study, also expect an abstention
and stay in their original strata.

The negative stratum carries disproportionate weight. A model that invents a plausible figure for a
country-year absent from the registry is worse than useless to a statistical office, and only
questions built on absent data catch it. Across the evaluation, models abstained correctly at
{{negative_rate}}.

These prompts test less than their names suggest, and Azevedo's review was right to say so. Every
prompt begins "Use the SDMX MCP only." The named-indicator prompts quote the registry's own label,
so they test resolving a label to a code rather than understanding how people ask. The negative
cases are missing years of series that exist, worded like the named-indicator prompts, so they show
whether a model reports absence when the registry shows it. They do not measure whether a model left
to itself would invent a figure, and "absent from the registry" is not the same as never collected:
a national survey may exist that the warehouse does not hold. A no-tools arm and a sample of real
users' questions are the tests that would answer those questions, and we have not yet run them.

### What counts as a correct answer

Every accuracy figure here means **agreement with the registry on the grading date**: fidelity to
the registry, in the phrase Azevedo suggests, and not truth about the world. The grading rule is
worth stating exactly, because a headline rests on it.

A value passes if it equals the expected value, or equals it rounded to the decimals the model
reported. That rule has no floor, so in principle "0" would pass for 0.4. In practice it did almost
no work: of {{review_passes}} passes on answerable questions, {{review_rounding}} relied on the
rounding rule, the largest gap was {{review_max_gap}}, and {{review_to_zero}} were rounded to zero.

An answer that omits its year or its dataflow can still pass, provided its tool calls reached the
expected series. In official statistics a value without its reference period is a different answer,
so we will tighten this. It changes nothing reported here: {{review_no_year}} of those passes omitted
the year. {{review_no_flow}} ({{review_no_flow_pct}}) omitted the dataflow, and every one of them
queried the right series.

Unit, source and uncertainty bounds are not graded at all. A second outcome for a complete answer,
with value, reference period, unit, source and bounds where the custodian publishes them, is work
we would like to do with Azevedo's metadata scorer, which can be applied to the saved responses.

### Where models actually struggle

Pooled across the {{pooled_models}} models measured on the clean manifest ({{pooled_cases}} graded
cases):

{{table_strata}}

**Pass rates are broadly similar across strata.** What changes is how far the models spread apart.
On prescriptive prompts they sit within {{spread_prescriptive}} percentage points of each other; on
questions where the agent must work out the codes itself they fan out several times wider.
That spread is descriptive rather than a test: it grows with the number of models and as strata get
smaller. Comparing models within the same case, with case fixed effects and clustered errors, is the
stronger analysis, and the saved responses already support it.

Execution, in other words, is solved. Given the tool name and the exact codes, essentially every
model succeeds. **But a prescriptive-only benchmark cannot tell these models apart** — which is
precisely the benchmark a procurement exercise would run, and precisely the one that would choose
wrongly.

### What actually separates models is code resolution

It is tempting to conclude that ambiguous phrasing is the hard part. We drew that conclusion
ourselves, from earlier figures, and a clean manifest did not support it. Ordering the strata by how
far the models spread apart puts the real boundary somewhere else:

| Stratum | What the agent must do | Spread |
| --- | --- | --- |
| prescriptive | Nothing. The tool and the exact codes are given | {{spread_prescriptive}} pts |
| negative | Recognise that no data exists and decline | {{spread_negative}} pts |
| disaggregated | Map a breakdown — "girls" — to a dimension code | {{spread_disaggregated}} pts |
| named-indicator | Map an indicator label and a country name to codes | {{spread_natural}} pts |
| ambiguous | Do that, and choose between sibling indicators | {{spread_ambiguous}} pts |

The jump is not at ambiguity. It is between *codes supplied* and *codes to be resolved*. Adding
ambiguity on top of plain name resolution widens the spread barely at all, and the increment is
inside the noise: the ambiguous stratum is measured on 75 cases per model against 175 for named indicators.
Pooled across every model, the two strata score *identically*.

The ordering survived a hard test. We first measured it across six models within three points of
one another, where it could have been an artefact of that narrow band. Repeating it across
{{pooled_models}} models spanning {{accuracy_range}} reproduced the same ranking of strata, with the
gap between fully specified and unspecified queries widening rather than closing.

One exclusion, decided on tool use rather than on score: a model that never gets the server to
answer records zero on every stratum, which makes every range the full 100 points and erases the
gradient. {{spread_excluded}} averaged {{spread_excluded_calls}} tool calls per case and is left out
of the spread for that reason. Pooled pass rates include every model.

So the discriminating capability is not judgement about vague language. It is the ability to turn
human words into registry identifiers. That has three consequences.

**It is the architectural argument, made concrete.** Everything described in the section above —
declared dimension roles, the theme prefix map, the candidate-finding tools — is tooling for exactly
the task that separates a competent model from a weak one. The discovery layer is not a convenience
wrapped around the registry; it is the part that does the work models are worst at.

That is measurable rather than rhetorical. We withheld the twenty-one discovery and resolution tools
and left the ten that correspond to native SDMX REST operations — read a structure, list a codelist,
request an observation — then re-ran the resolution strata:

{{table_ablation}}

The best open-weight model in this study loses {{ablation_drop_best}} percentage points. The next
one down loses {{ablation_drop_worst}}. The effect decomposes into two distinct failures: between a
third and a half of cases no longer complete at all, because tool calls per question rise from
{{ablation_tools_before}} to {{ablation_tools_after}} and exhaust a step budget that was previously
ample; and among the cases that *do* produce an answer, accuracy still falls sharply. The models are
not failing to reason. They are drowning in codelist enumeration.

**This is a lower bound, and a generous one.** The ablated arm is a proxy for working against a raw
REST API, not the thing itself: it still receives structured tool schemas, our validation layer
explaining each malformed request, scope enforcement, caching and transport retry, and it never has
to build a URL or parse SDMX-JSON. A model genuinely working against the registry's REST interface
has none of that. The honest reading is that the discovery layer is worth *at least* this much.

**A benchmark must include name-based resolution.** Elaborate ambiguity cases are a refinement;
without resolving names to codes a suite cannot choose a model at all.

**And stratum sensitivity is a property of weak models, not of the task.** The strongest models here
sit within a couple of points of their own average on every stratum. Choose one of them and question
type stops mattering — which is a reassuring thing to be able to tell an office, and is only visible
because the range was tested rather than ranked.

### Ground truth has a shelf life

A manifest built on one date and graded two weeks later capped every model at 94% on the easiest
stratum. The cause was not the models. UNICEF had restated the `PV_CHLD_*` child-poverty family in
the interval — one series moving from 18.5535945892334 to 23.67173993 as its source changed from DHS
VII to DHS 2018 (IPUMS) — and withdrawn several series entirely. Models were penalised twice: for
reporting the current value, and for correctly answering *no data* where data no longer existed.

Re-reading expected values from the live registry costs nothing, needs no model, and applies to
responses already collected. It belongs immediately before grading, every time. It also needs to be
more robust than it first appears: a single pass under registry load confirmed a fraction of the
affected cases, and the registry throttles clients hard without sending a `Retry-After` header, so
a refresh that silently checked almost nothing reported itself as a clean confirmation.

## What it costs

The same 500 questions, every model on the same manifest, graded the same way:

{{table_all}}

Cost here is measured per *task*, not per token, and the two tell different stories. A single
question costs roughly {{input_tokens_per_case}} input tokens against {{output_tokens_per_case}}
output tokens, because every tool result is resent on the following turn. Input price therefore
dominates by a factor of about {{io_ratio}}, and output price is nearly irrelevant.

That has a consequence worth stating plainly, because it is not visible on any price list: **a
model's name tells you nothing about what it costs to do this work.** Several models marketed on
speed and economy are among the most expensive here, because their input pricing is what matters and
theirs is high. One of the cheapest models in a catalogue of several hundred scored zero, having
declined to call the tools at all.

At the measured rate, **100 analyst questions a day for a year is about {{annual_cost}}** in model
spend. No per-seat licence, no data egress to a vendor, no lock-in.

### Screening is most of the economy

Capability at tool use varies far more than price does, and the gap does not appear on easy
questions. A short screening manifest resolves this cheaply: screening {{screen_models}} models on
74 cases cost {{screen_cost}}, and separated models that drive the server competently from models
that cannot, before any money went into a full run.

### What this server costs against others

Everything above compares models on one server. Azevedo (2026) did the reverse: three servers and a
small number of Claude models, on the same questions. He reads the result as this server being the
costly one to use. Our study cannot test that, because we have never run another server. His figures
show that the answer depends on what is measured:

| Azevedo (2026), per question | This server | UN Data Commons | Data360 |
| --- | --- | --- | --- |
| Scripted path, median characters | **13.4k** | 8.9k | 4.2k |
| Scripted path, mean characters | 12.7k | **13.3k** | 4.1k |
| Agent sessions, median characters | 16.6k | 8.1k, plus a 12k instruction file | **27.3k** |
| Agent sessions, tokens sent (mean) | 633k | **749k** | 519k |

The purposes differ as well. His scripted path appears to set aside the Data Commons instruction
file and requires a structure call of this server, and Data360 is cheap partly because its search
found only 4 of the 14 series it carries. His agent sessions use four well-specified series on
capable models, where every model finds the value, so they cannot show what discovery tools buy. Our
withholding experiment can: a cheaper model that loses those tools loses {{ablation_drop_best}} to
{{ablation_drop_worst}} points. Most of his tokens are also the client's own baseline, re-sent and
largely cached, so tokens sent are not dollars.

He is nevertheless right about where the weight is. In our own traces `plan_topic_query` returns a
median of {{review_plan_median}} characters and `guided_discover` {{review_guided_median}}, against
{{review_single_median}} for the observation itself. A question with no data costs this server a
median of 12.6k characters in his runs and the Data Commons 0.6k. Tool definitions matter too: a
request that has received no tool results already costs {{review_zero_low}} to {{review_zero_high}}
input tokens on the models where we can observe one, and that block is re-sent on every turn. His
estimate of about 20,000 tokens for the same tool list comes from a different client and a different
measure, and we have not reconciled the two.

Our own harness hid part of this. It clipped each tool result at 16,000 characters before the model
saw it, which {{review_over_clip}} of {{review_tool_results}} results exceeded. One tool,
`get_flow_structure`, returned {{review_structure_chars}} characters on {{review_structure_big}} of
its {{review_structure_calls}} calls. His agent met the same response, about 1.3 million characters,
and his client refused it. Our costs are understated wherever a response overflowed, and a client
without that clip would have paid in full.

## A moving target

The single most useful practice of the year was running many models against the same questions — not
to rank them, but because each one exposed a different fault, and most of the faults were ours.

{{table_faults}}

Read the owner column. **Most of what multi-model testing found was wrong with our code, not the
models.** A single-model benchmark produces a server tuned to one model's habits and a score that
says more about the harness than about the registry.

The JSON-mode episode is the clearest case. It was added to accommodate one model, failed to fix
that model, and silently disabled tool calling in three others — two of which are among the best
performers measured here. Tested against one model, we would have shipped it and published the
conclusion that open-weight models cannot drive an SDMX MCP.

### A low score is a hypothesis, not a measurement

Repeatedly, a setting of ours produced a plausible low score that we nearly published as a model
limitation. A model that appeared to score 77.6% was really at 96.2%, having omitted a structured
field while answering correctly. One that "failed" 46% of cases had been given an output budget
below what it needs to answer at all. Three models made zero tool calls because we had asked for a
JSON object. Every model was capped at 94% by stale ground truth. A re-run that printed "running"
and wrote nothing scored stale failures as 16.8%. Correct abstentions were filed as unreviewable
rather than counted. Transient transport errors were scored as wrong answers.

Not one of these announced itself as an error. Each produced a believable number, and every single
correction moved the figures **upward**, because each fault made the models look worse than they
were. A benchmark run once, carefully, by people acting in good faith would have published
understated results for every model it measured and had no way to know.

**On a tool-using benchmark, a low score has to be attributed before it is reported.** Expect to
spend more effort on the harness than on the models.

### How much does a published figure move if you re-run it?

Every number in this report is a single run, which is the convention and is worth interrogating. We
re-ran one case set three times, unchanged, at the settings the tables use.

{{table_variance}}

Two things come out of it. **For competent models the figure is stable to about a point**, which is
a reasonable error bar to carry mentally against any single number here. And **the headline can be
stable while the individual answers are not**: one model varied by {{variance_tight_spread}} points
across repeats while {{variance_tight_flip}} of its individual cases returned a different verdict
each time. An office defending a specific published figure cares about the second number, and only
case-level comparison reveals it.

The exception is stark. {{variance_worst_model}} varied by {{variance_worst_spread}} points, and
**{{variance_worst_flip}} of its cases changed verdict between identical runs**. For that model any
single answer is close to a coin toss — a more disqualifying property than its headline score, and
one no sample size can expose in a single run.

### You are measuring a model *and* a serving route

For a commercial model, "the model" is one service: one vendor, one version, named and dated. For an
open-weight model this is not so. The weights are published, many companies host them, and a gateway
routes each request to one of several — our metadata shows one model resolving to a host with three
others standing by, another with seven alternates. Identical requests, minutes apart, can be served
by different infrastructure.

What varies between hosts is not cosmetic. Numerical precision varies, sampling defaults vary where
the caller does not pin them, and — most consequentially here — **tool calling for an open-weight
model is usually implemented by the serving stack rather than by the model**. The template that
advertises the tools and the parser that turns output back into a structured call belong to the
host. Whether a tool call arrives structured or as plain text in the message body can therefore be a
property of who served it.

**We expected this to show up as instability, and it did not.** Two measurements say so. The
commercial control and the best open-weight model returned identical run-to-run spread, so among
competent models variance tracked capability rather than licensing. And when we recorded the host
for every request and re-ran the least stable model three times, **all {{routed_requests}} requests
went to the same host** — the gateway routed stickily, despite naming three fallbacks as available.
That model still varied {{routed_spread}} points across those repeats. Its instability is therefore
within a single serving host, not an artefact of switching between them.

So the mechanism is real but, in our measurements, latent. We report it because it bears on
reproducibility and because a reader hosting elsewhere inherits it, not because we caught it moving
a number.

It does, however, leave one attribution in the table above undefendable. We recorded that certain
models emit tool calls as plain text and assigned that fault to the model; the component responsible
sits in the serving stack, and we did not record which host answered each request. The honest entry
is *undetermined*, and we have changed it to say so.

Three consequences for anyone reproducing this:

- **Record the routing metadata per request.** Gateways return it. Without it, a cluster of failures
  cannot be separated from a bad route. We added that capture only after it mattered, and then
  mis-wired it and collected nothing for a further 2,664 cases.
- **If you self-host, test your own deployment.** This is the strongest reason not to trust any
  published table, this one included. Your serving stack is yours.
- **A first-party API buys reproducibility.** One route, one behaviour, a version you can name. That
  cuts against the cost argument made above, and it is part of what the price difference purchases.

## Why the deterministic layer endures

A fair objection has grown louder over the year: frontier agentic models can read documentation,
call a REST API and write their own queries, so why maintain a server in between? The measurements
answer on three grounds, none of which is model capability.

**Cost.** A frontier model re-derives registry structure on every request, and repeats that work for
the next user. The server spends it once, in the schema — {{cost_per_question}} per question against
a cheap model over a narrow toolset.

**Containment.** The server rejected {{validation_errors}} malformed tool calls over the evaluation
and explained each one back to the model: labels supplied where code identifiers are required,
invented dimensions, two-letter country codes where the registry uses three, keys with the wrong
number of segments. Models recovered from most of them and went on to answer correctly. A model
writing its own REST calls against an open endpoint has nothing to refuse it and nothing to correct
it. The server is where an institution's rules live — which flows are public, which are draft, what
a refusal means — and those should not depend on model quality.

**Provenance.** Every answer carries the tool calls that produced it, the query URL the registry
received, and the filters applied. That is how we repeatedly established that a model was right and
our own ground truth stale. A free-form agent leaves a reasoning trace, not a query log; an office
answering for a published figure needs the log. That is provenance at the level of the query.
Provenance at the level of the custodian asks for more, as the next section describes.

There is a fourth ground we did not anticipate, and it arrived unsolicited. During this evaluation
the registry became entirely unavailable for a period. Our discovery layer, asked for an indicator,
returned an empty list rather than an error — and a well-behaved model concluded, honestly, that no
such indicator existed. The bug was ours and is fixed: a transport failure now surfaces as a
failure. But the episode is the argument in miniature. **A deterministic layer is where the handling
for a bad day belongs.** A model talking directly to a registry that has stopped answering has
nowhere to put that logic, and every incentive to fill the silence.

The server does not add capability. **It converts capability into something affordable, governable
and checkable.**

## What an independent review changed

A draft of this report went to João Pedro Azevedo, UNICEF's Chief Statistician, who had been
benchmarking three MCP servers, this one among them. His starting point was that an audience of
statisticians will hold anything like this to the standard of an official release: every figure
traceable, every method stated, every value attributed to its custodian. The two studies are
complementary, since we varied the model on one server and he varied the server. Where they overlap
they agree: 1,952 of the 1,953 values his agents reported matched the warehouse, and the one
exception was a year the warehouse does not hold. This server also scored highest of the three
against his metadata standard, meeting 5 of 15 elements and partly meeting 8, and delivering 46–70%
of it on every question answered.

His review also found faults that our own testing had not, and it changed the software:

| What the review found | What changed in the server |
| --- | --- |
| One `get_flow_structure` call returned 1.3 million characters, and his client refused it | Every tool result now has a ceiling, 50,000 characters by default. That call now returns about 26,000, with codelists summarized and a pointer to `list_codes`, which pages. `query_data` cuts at whole rows and states the offset of the next page. Anything else over the ceiling is an error rather than a silent clip |
| No response carried a retrieval time, a server version or a licence | Data responses now state when they were read and by which server version, beside the flow reference that names the agency and structure version. A licence is stated when the deployment configures one |
| Search, not metadata, is what costs | Search returns what the next call needs. On one test question `plan_topic_query` fell from about 10,700 characters to 4,400, and `guided_discover` from about 21,000 to 6,700 |
| A question with no data cost 12.6k characters, against 0.6k | A caller can name one country. If none of the candidates has data there, the search says so in about 700 characters. If the check cannot be completed, the search proceeds normally, because "could not check" must never read as "no data" |

Two of his points are not closed by these changes. Custodian-level attribution needs the source of
each series, and `DATA_SOURCE` appears on only 59% of observations in his panel, which no server
change can repair. And the new options lengthened the tool list by about 2%, against his finding
that tool definitions are the largest thing a server adds to each request. Shortening that list is
the next test, but it has to be a test: the withholding experiment above shows that discovery tools
cannot simply be removed.

None of the figures in this report reflect these changes. They were measured in October 2026 on the
server as it stood before them, and a response that is shorter or differently shaped can move a
model's accuracy in either direction. We will re-run the full manifest before claiming anything
about their effect.

The episode has a lesson of its own. We ran 8,000 cases across sixteen models and missed a
response of more than a million characters, because our harness trimmed it before anything could
trip over it. Azevedo found it with 96 sessions and a different client. Multi-model testing exposes
one class of fault and multi-server testing another, and neither replaces a critical reader. His
remark that a deterministic layer is where the handling for a bad day belongs, applied to size as
much as to outages, was our own sentence returned with a defect attached.

## The work does not finish

The cheapest competent model changed more than once inside a single year. Harness conventions moved
under us. The registry restated a series mid-evaluation, throttled us without saying so, and fell
over. Every one of those events changed a number in this report.

The conclusion we would most like to leave with this audience is therefore not a model
recommendation, which would date within months. It is that **a tool like this has to be retested as
a matter of routine, and the retesting has to be cheap enough that you actually do it.** That is a
design constraint on the harness, not an afterthought: reproducible case manifests, free ground-truth
refresh, responses kept so they can be re-graded without re-running, and screening cheap enough to
precede every commitment.

The architecture is built so that model choice is a single configuration line. That is the point. A
result about specific models ages quickly; the method should not.

But a configuration line is only useful to an office that knows when to change it, and that is not a
technical capability. It is a business process, and most statistical offices do not yet have it.

## What an office would need to put in place

None of what follows is difficult. All of it is unfamiliar, and none of it exists in a statistical
office's current operating model. Every item is something this year taught us by going wrong.

1. **Write the dimension policy before anything else.** Which dimensions carry the subject,
   geography and time roles, and which tokens to ignore when scoring a query. It is registry-
   specific, it has no useful default, and discovery does not work without it. Revisit it when your
   registry's conventions change.
2. **Own your question set.** A manifest of questions your office actually receives, answered from
   your own registry, reproducible from a seed. A generic benchmark measures somebody else's
   registry and will mislead you about your own.
3. **Refresh ground truth before every grading run.** It is free, needs no model, and your own data
   moves: ours was restated inside fifteen days and two series were withdrawn outright, which cost
   every model measured a point until we noticed.
4. **Verify that the refresh actually reached the registry.** A throttled case is unchecked, not
   confirmed. Ours reported a clean no-change refresh while 494 of 500 cases had been rejected.
5. **Screen before you commit.** {{screen_models}} models on 74 cases cost {{screen_cost}} and
   separated the competent from the unusable. Only spend a full run on what survives.
6. **Measure cost per task, not per token.** Input dominates by roughly {{io_ratio}} to one here, so
   per-token price lists actively mislead. Several models sold on economy are the dearest.
7. **Retest on model releases, not on the calendar.** The cheapest competent model changed more than
   once in a year, and one vendor cut price {{vendor_price_drop}} for identical accuracy.
8. **Re-run before you publish.** A single run is an estimate. Competent models move about a point
   between identical runs; a weak one moved {{variance_worst_spread}}.
9. **Test abstention and fabrication on purpose.** Questions the registry cannot answer show
   whether a model reports absence; a no-tools arm shows what it invents on its own. Accuracy on
   answerable questions tells you neither, and inventing figures is the one failure a statistical
   office cannot absorb.
10. **Record which serving route answered.** Gateways return it. Without it you cannot separate a bad
   model from a bad host.
11. **Attribute every low score before acting on it.** Most of ours were our own harness, and every
    correction moved the numbers up.

Taken together that is perhaps a week of setup and a day per retest, against a capability an office
otherwise cedes entirely. The barrier is not cost or difficulty. It is that nobody's job description
currently contains it.

## For practitioners

Everything described here is in `github.com/unicef-drp/sdmx-mcp`, and none of it is specific to
UNICEF. The server takes any SDMX 2.1 endpoint through `SDMX_BASE_URL`; registry-specific behaviour
lives in policy files and environment variables rather than in code. A live instance runs at
`https://sdmx-mcp.fly.dev/mcp` over Streamable HTTP.

**Three things a deployment must supply**, in order: `SDMX_BASE_URL` pointing at your endpoint; a
scope declaration, so an agent considers your published flows and not your drafts; and a dimension
policy derived from `query_dimension_policy.example.json`, naming which dimensions carry the
subject, geography and time roles. The first two have defaults you can live with briefly. The third
does not, and discovery scoring does not work without it.

| Script | What it does |
| --- | --- |
| `server.py` | The MCP server — 31 tools over an SDMX 2.1 registry |
| `mcp_fidelity_sweep.py` | **Experiment A.** `--mode bulk` does it in ~350 requests instead of ~79,000 |
| `sdmx_eval_build_cases_from_sweep.py` | Builds a stratified question manifest from the sweep's ground truth |
| `sdmx_eval_refresh_ground_truth.py` | Re-reads expected values from the live registry. Run before grading |
| `sdmx_eval_runner.py` | **Experiment B.** `run-provider`, then `grade-results` |
| `sdmx_eval_model_sweep.py` | Runs one manifest across many models and tabulates accuracy against cost |
| `sdmx_eval_provider_gateway.py` | Drives any OpenAI-compatible gateway model, running the MCP loop locally |
| `sdmx_eval_provider_anthropic.py` | Drives Claude through Anthropic's hosted MCP connector |

### Experiment A — is your server faithful?

Start here. It needs no model, no API key and no money, and it is the only part that can give a hard
correctness guarantee.

```
python3 scripts/mcp_fidelity_sweep.py --mode bulk \
    --flow AGENCY,FLOW,1.0 --endpoint https://your-mcp/mcp --out tmp/fidelity
```

For another registry, point `--area-codelist` and `--area-dim` at your own dimensions.
`--area-pattern` filters area codes by shape — an ISO3 regex suits registries that use ISO3, and an
empty string disables the check for those using M49. Set `--min-area-codes` as a floor: a codelist
request that silently returned a partial page once filtered a whole sweep to zero rows.

### Experiment B — can a model drive it?

Four steps, of which only the third costs anything.

1. `sdmx_eval_build_cases_from_sweep.py --ground-truth tmp/fidelity/ground_truth.csv --manifest tmp/manifest.jsonl --count 500 --seed 1` — the seed makes the case set reproducible.
2. `sdmx_eval_refresh_ground_truth.py --manifest tmp/manifest.jsonl --out tmp/manifest.fresh.jsonl` — free, and it stops a restated series from scoring against the model.
3. `sdmx_eval_model_sweep.py --config cfg.json --manifest tmp/manifest.fresh.jsonl --out-dir tmp/sweep --models vendor/model-a vendor/model-b` — runs and grades each model, writing its own responses and grades so any number traces back to a transcript.
4. Re-grade at any time with `sdmx_eval_runner.py grade-results`: responses already collected can be scored against a refreshed manifest without spending again.

### If you reproduce this, watch for these

Each of these cost us a published-looking number that was wrong.

- **Screen cheaply, then commit.** Capability at tool use varies far more than price. Only spend a
  full run on what survives a short screen.
- **Refresh ground truth immediately before grading**, verify that the refresh actually reached the
  registry, and treat a throttled case as unchecked rather than as confirmed.
- **Retry transient failures before grading.** A transport error scored as a wrong answer is
  indistinguishable from incompetence in the final table.
- **Never let an empty result stand in for an error.** A discovery call that returns nothing because
  the registry is down reads to a model as "this indicator does not exist", and a well-behaved model
  will tell your user exactly that.
- **Build the negative stratum first.** Questions the registry cannot answer are what show whether
  a model reports absence, and no amount of accuracy on answerable questions substitutes for it.
- **Check that your harness is not the thing you are measuring.** Output-token ceilings, step
  ceilings and structured-output modes all produced convincing model-shaped failures.
- **Attribute every low score before reporting it.**

## Where this goes next

SDMX is not one organisation's format. It is the exchange standard the BIS, the ECB, Eurostat, the
IMF, the OECD, the United Nations and the World Bank converged on, precisely so that structure and
meaning would not have to be renegotiated for every transfer. And this community has already shown
what pooling effort on tooling looks like: the SIS-CC, led by the OECD, has put years of sustained
work into shared open-source SDMX infrastructure that many of us now depend on.

Which makes the situation in front of us slightly absurd. If every organisation serving SDMX data
builds its own MCP layer, each will independently rediscover that discovery tooling is what
separates competent models from weak ones, that ground truth goes stale inside a fortnight, that a
structured-output setting can silently disable tool calling, and that an empty result is not an
error. We rediscovered all of it this year, at a cost measured in weeks.

The alternative is a single generic, open-source MCP layer for SDMX — configured per registry rather
than forked per organisation, which is only possible because the registry-specific parts turned out
to be configuration rather than code. This codebase is offered as a starting point for that, not as
a finished answer to it, and the SIS-CC's example is the model worth following.

It is a working tool, not a product. It runs continuously over UNICEF's public SDMX data and anyone
can point a client at it today. It does not carry an official UNICEF imprimatur, and saying so
matters: what exists is a demonstrated capability and a public codebase, not an institutional
commitment. Turning the first into the second is exactly the work a community does, and it is why
this is being presented to this room rather than announced.

### The gaps are the agenda

Everything below is something we cannot settle alone and someone here can.

- **Non-English registries and labels.** Untested, and the most important gap by a distance. For
  much of the SDMX community this is the first question, not the last, and we have no way to answer
  it.
- **Other registries.** The server is generic by construction and the harness takes any SDMX 2.1
  endpoint, but every figure here comes from one registry. Experiment A costs nothing but time and
  needs no model: run it against yours and tell us what broke.
- **Multi-step analysis.** Every case here resolves to a single observation, series or table.
  Combining flows, deriving a rate from a numerator and a denominator, and reasoning across time are
  untested.
- **Whether exposing units helps.** Our own A/B was underpowered at n=75 and inconclusive (p=0.36),
  and the weaker model got *worse* with units present.
- **Fabrication without tools, and real questions.** Our prompts name the server and quote registry
  labels. A no-tools arm and a sample of real users' questions would show how far these results
  travel.
- **Comparing servers.** Azevedo's benchmark varies the server and ours varies the model. One
  question set, one grader and one metadata scorer would let either study answer the other's
  question, and we hope to build them together.
- **Reproducibility from a clean checkout.** The figures here rebuild from run files that are not yet
  public. A replication package with the manifest, a dated registry snapshot and every response and
  grade should follow this draft.
- **A shared model-evolution watch.** The business processes above are real work, and each office
  doing them independently is waste. A shared, current answer to "which models drive an SDMX MCP
  competently, and at what cost per query" would serve everyone and cost any single participant very
  little.

## Sources

Azevedo, J.P. (2026). *Metadata, cost and accuracy of official statistics served to AI agents:
evidence from three MCP servers*. UNICEF working draft, 27 October 2026.

Azevedo, J.P. (2026). *92 AI gateways to official statistics in two years*. LinkedIn article,
figures as of 27 October 2026.
<https://www.linkedin.com/pulse/92-ai-gateways-official-statistics-two-years-six-we-should-azevedo-xm0ne/>

FedScoop (4 February 2026), on MCP servers improving AI access to public data.
<https://fedscoop.com/federal-goverment-mcp-improve-ai-access-public-data/>

U.S. Digital Corps, *Improving LLM Access to Federal Data*. <https://digitalcorps.gsa.gov/pdfs/MCP_Report.pdf>

data.gouv.fr, *Expérimentation autour d'un serveur MCP pour datagouv*.
<https://www.data.gouv.fr/posts/experimentation-autour-dun-serveur-mcp-pour-datagouv>

We thank João Pedro Azevedo for his review of an earlier draft. Remaining errors are ours.

---
# Presentation outline

### A Year of SDMX MCP
- layout: title
- section: Opening
- subtitle: Keeping decisions about children anchored to the data we actually hold

> Open with the problem, not the project. A model asked for under-five mortality can hand you a number with nothing behind it, and it reads exactly like the right one. A wrong film release date is a nuisance; a wrong stunting figure enters a brief, then a slide, then a decision. The institutions that spend decades establishing what the number is have no mechanism for contesting a number that was never sourced. And this is already happening at scale: João Pedro Azevedo counted 92 MCP servers for official statistics as of late October, built in about two years, and only 9 of them are official. An office that does not serve its own data loses attribution, not only authorship. That is why we started a year ago. Today: what we built, why we made it generic, what measuring it honestly took, what it costs, and why this work does not finish.

### What was built, and the one claim we can prove
- layout: two-experiments
- section: 2 — The server

> Two experiments, because 'the server is faithful' and 'the model is competent' are different claims that fail differently. Experiment A involves no model at all — that is the point, and it is the only part that yields a hard guarantee. Retrieval is deterministic and provable; interpretation is probabilistic, and that is the only part you need a model for. Because the deterministic layer carries the correctness guarantee, the probabilistic layer does not have to be excellent — only competent at resolving codes and honest about failure. That is a far cheaper thing to buy. One caution about what the right-hand number means: it is agreement with the registry on the grading date, not truth about the world, and the warehouse also republishes inter-agency estimates whose release of record lies elsewhere.

### Making it generic taught us what was really configuration
- layout: three-columns
- section: 3 — Generalising

> We built it for our registry and made it generic on purpose — an office that cannot run it on its own warehouse has a case study, not a tool. The hard part was not supporting arbitrary SDMX. It was discovering how much of what we had written as code was actually configuration. Dimensions carry roles: subject, geography, time — and which dimension plays each is now declared, not assumed. Agents need a path from a topic to a code family, so there is a configurable prefix map, shipped disabled because a map tuned to our conventions misleads on anyone else's. And scope has to be declared and then enforced. The lesson to take home: the registry-specific parts of your server are not the parts that look registry-specific — they are the constants you never thought to question.

### Models agree on precise queries, diverge on vague ones
- layout: chart-spread
- section: 4 — Measuring it

> A note on the labels first: these prompts name the server and quote the registry's indicator labels, so they test resolving codes, not how real people ask. Levels are fairly flat across strata and high everywhere. What matters is the spread. On prescriptive prompts — tool named, codes given — the models sit a couple of points apart. On anything the user has not fully specified they fan out six times wider. Execution is solved; interpretation is where models differ. The practical consequence is uncomfortable: an easy benchmark cannot choose a model for you, and an easy benchmark is exactly what a procurement exercise runs. Note also that difficulty looks like a property of the question, not the model — two unrelated models at similar capability produced identical stratum profiles and failed the same individual cases.

### {{models_tested}} models, ten producers, one manifest
- layout: models-table
- section: 5 — Cost

> This is the whole study on one slide. Sixteen models, every major commercial and open-weight producer, the same five hundred questions, the same grader, the same server. Four of them tie at the top. The spread underneath is what matters: from ninety-nine point six percent down to two point six, at prices from ten cents to six dollars per five hundred questions — and the ordering of price tells you almost nothing about the ordering of accuracy. The model at the bottom is the cheapest in the catalogue; it never successfully called a tool and answered from memory instead. Cost here is per task, not per token: a question costs about {{input_tokens_per_case}} input tokens against {{output_tokens_per_case}} output, so input price dominates by roughly {{io_ratio}} to one, and a model's name tells you nothing about what this work costs. A hundred analyst questions a day for a year is about {{annual_cost}}. Accuracy throughout means agreement with the registry.

### An independent review changed the server
- layout: review
- section: 5 — Cost

> We sent a draft to João Pedro Azevedo, UNICEF's Chief Statistician, who had been benchmarking three MCP servers including this one. We varied the model on one server; he varied the server. Where the studies overlap they agree: 1,952 of the 1,953 values his agents reported matched the warehouse, and this server scored highest of the three on his metadata standard. He also found what we had missed. One response ran past a million characters, and our own harness had hidden it by trimming results before the model saw them. Responses carried no retrieval time, version or licence. Search was the heavy part of the bill. On cost, which server is dearest depends on what you measure: first on one of his measures, in the middle or cheapest on others, and his purpose and ours differ. Every change on the right came from that review. None of it is in the numbers you have just seen, because a shorter response can move accuracy either way, so we will re-measure before claiming anything.

### Many models, many different faults — mostly ours
- layout: faults-table
- section: 6 — A moving target

> Read the owner column. Most of what multi-model testing found was wrong with our code, not the models. The JSON-mode line is the one to dwell on: added to accommodate one model, it failed to fix that model and silently disabled tool calling in three others — two of which are among the best performers we measured. Tested against a single model we would have shipped it and published the conclusion that open-weight models cannot drive an SDMX MCP. Screening {{screen_models}} models on 74 cases cost {{screen_cost}}.

### A low score is a hypothesis, not a measurement
- layout: four-cards
- section: 6 — A moving target

> This is the methodological point, and for this audience it is the one that earns trust. Each of these looked exactly like a model being bad at SDMX, and none of them was. Here is the part I would underline: every correction we made moved the numbers upward, because each fault made the models look worse than they were. A benchmark run once, carefully, by people acting in good faith would have published understated results for every model it measured and had no way of knowing. If you evaluate a model over your own registry, expect to spend more effort on your harness than on the models, and attribute every low score before you report it.

### Why keep a server when models can read your API?
- layout: office
- section: 7 — Why MCP endures

> Cost, containment, provenance — none of which is about model capability. The server rejected {{validation_errors}} malformed tool calls and explained each one back to the model, which then recovered; a model writing its own REST calls has nothing to refuse it and nothing to correct it. Every answer carries its query log, which is how we repeatedly established that a model was right and our own ground truth stale. And the review we just discussed pushed that further, from provenance of the query to provenance of the custodian. And a fourth reason we did not plan: mid-evaluation the registry became entirely unavailable, our discovery layer returned an empty list, and a well-behaved model concluded honestly that the indicator did not exist. Our bug, now fixed — but that is the argument in miniature. A deterministic layer is where the handling for a bad day belongs. The expensive part of this architecture is not the part you have to buy.

### Deploying it: one container, one command
- layout: deploy
- section: 8 — Running it yourself

> I want to be concrete about the barrier here, because "build an MCP server" sounds like a project and it is not. The repository has a Dockerfile. The image is two hundred and fifty megabytes, it runs on one shared CPU, and it goes to any platform that takes a container — we use Fly, but Cloud Run, App Service or ECS are the same two commands. What you get is one HTTPS endpoint speaking Streamable HTTP that any MCP client can point at. The deploy that put our current version live took seventy-four seconds, and it runs on every push.

### Configuring it: two files and some environment
- layout: config
- section: 8 — Running it yourself

> And this is the part that is actually yours to write, so it is worth seeing how small it is. Two JSON files and a handful of environment variables. The dimension policy says which of your dimensions carries the subject, which the geography, which the time — that is the one with no sensible default, and the one that quietly costs you the resolution layer if you skip it. It names each dimension, gives it a role and a priority, and points at the codelist to resolve against. The discovery policy holds stopwords and topic hints, mapping words your users say to families of flows in your registry. Everything else is environment: your endpoint, which agencies you expose, whether drafts are visible. That is the whole adaptation surface. No fork, no code.

### The work does not finish
- layout: close
- section: 9 — Close

> The cheapest competent model changed more than once inside one year. Harness conventions moved under us. The registry restated a series mid-evaluation, throttled us without saying so, and fell over entirely. Every one of those changed a number in this report, and an outside reader found a defect that eight thousand test cases had not. So our closing recommendation is deliberately not a model — that would date within months. It is that retesting has to be routine, and cheap enough that you actually do it: reproducible manifests, free ground-truth refresh, responses kept so they can be re-graded without spending again. Model choice is one configuration line. The code is public, and running Experiment A against your own registry costs nothing but time.
