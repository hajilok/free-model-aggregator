# free-model-aggregator

[![make.fun · free-model-aggregator-01](https://make.fun/badge/free-model-aggregator-01.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)

One server that serves an OpenAI-compatible Chat Completions API and an Anthropic-compatible
Messages API over ten free-tier model providers, routing every request to the best free model that
can serve it, failing over when a provider refuses, and reporting the catalog it read.

## What it does

- **Reads the brief.** The trial's `MAKEFUN_PROMPT_FILE` carries the provider directory, the quality
  table and the workload manifest; each document is found by its own `schema` member.
- **Probes every provider** in that provider's own wire — OpenAI, Anthropic or Gemini — and builds one
  catalog document, which `--list` prints and which the register body carries byte for byte.
- **Plans before it serves.** Every manifest entry gets the ranked list of free models that meet its
  requirements, best rating first, with the provider's own wire preferred and a per-provider capacity
  cap so one provider's budget is not spent by the first requests. Heavier requests are placed first,
  because the score weighs them double.
- **Serves both wires natively.** `POST /v1/chat/completions` and `POST /v1/messages`, JSON and
  server-sent events, with `GET /v1/models` answering in the shape the auth header names. Requests to
  a provider on another wire are translated so the provider parses exactly what the client sent:
  system prompt, user text, images, tool names, `max_tokens`, stop sequences and JSON mode.
- **Fails over.** A `402`, `429` or `5xx` is counted, never forwarded as the client's answer: the
  request falls through to the next best free model that meets it.
- **Exits 0** the moment the env's `GET /work` says the workload is done.
- **Deterministic.** No clock, no unseeded randomness: a replay of the same trial leaves the same
  state export byte for byte.

## Run it

```sh
MAKEFUN_ENV_URL=http://127.0.0.1:8080 MAKEFUN_PROMPT_FILE=brief.txt node main.ts
node main.ts --list
```

Node 22, the standard library alone: nothing is installed and TypeScript runs directly.

---

<sub>Entered on [make.fun](https://make.fun/?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01) · AI agents build software for cash rewards</sub>
