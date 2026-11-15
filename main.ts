// free-model-aggregator-01: one server, two client wires, ten free-tier providers.
//
// The entry reads the trial's brief (MAKEFUN_PROMPT_FILE): the provider directory, the sealed
// quality table and the workload manifest. It probes every provider's catalog route in that
// provider's own wire, builds one catalog document, registers it with the env, serves the client's
// OpenAI Chat Completions and Anthropic Messages requests (JSON and SSE), and exits 0 when the env
// says the workload is done.
//
// Routing is planned before the first request: every manifest entry gets the ordered list of free
// models that can serve it, best rating first, with the provider's own wire preferred (a same-wire
// call needs no translation, so the provider parses exactly what the client sent) and with a
// per-provider capacity cap so the best models are not all spent on the first requests. A provider
// that refuses (402, 429, 5xx) is counted and the request falls through to the next candidate: a
// refusal is never the client's answer.
//
// Everything is a pure function of the brief, the requests and the providers' answers: no clock,
// no unseeded randomness, so a replay leaves the same state export byte for byte.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { readFileSync } from 'node:fs';

type Dict = Record<string, any>;
type Wire = 'openai' | 'anthropic' | 'gemini';

const isObj = (v: unknown): v is Dict => v !== null && typeof v === 'object' && !Array.isArray(v);
const str = (v: unknown): string => (typeof v === 'string' ? v : '');
const num = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const bool = (v: unknown): boolean | null => (typeof v === 'boolean' ? v : null);

const PROTOCOL_VERSION = 1;
const POLL_MS = 100;
const PROBE_TIMEOUT_MS = 8000;
const UPSTREAM_TIMEOUT_MS = 30000;
const MAX_BODY_BYTES = 32 * 1024 * 1024;
/** How many calls one provider is planned to take before the plan spills to the next model. */
const PROVIDER_CAP = 8;
/** A same-wire candidate's advantage over a cross-wire one, in rating milli-points. */
const WIRE_BONUS = 2;

const ENV = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const BRIEF_PATH = process.env.MAKEFUN_PROMPT_FILE ?? '';

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

// ---------------------------------------------------------------- the brief

interface Provider {
  id: string;
  name: string;
  wire: Wire;
  auth: string;
  base: string;
  key_env: string;
  key: string;
}

interface RatingRow {
  model: string;
  provider: string;
  rating_milli: number;
  context_length: number | null;
  tools: boolean;
  vision: boolean;
  json: boolean;
  free: boolean;
}

interface Requires {
  tools?: boolean;
  vision?: boolean;
  json?: boolean;
  min_context?: number;
}

interface ManifestEntry {
  id: string;
  api: 'openai' | 'anthropic';
  stream: boolean;
  weight: number;
  requires: Requires;
}

interface Brief {
  providers: Provider[];
  ratings: RatingRow[];
  manifest: ManifestEntry[];
  error: string | null;
}

/** Every JSON object in the brief that carries a string `schema`, in the order it appears. */
function objectsIn(text: string): Dict[] {
  const out: Dict[] = [];
  for (let start = text.indexOf('{'); start !== -1; start = text.indexOf('{', start + 1)) {
    let depth = 0;
    let inString = false;
    let end = -1;
    for (let i = start; i < text.length; i++) {
      const ch = text[i];
      if (inString) {
        if (ch === '\\') i += 1;
        else if (ch === '"') inString = false;
      } else if (ch === '"') inString = true;
      else if (ch === '{') depth += 1;
      else if (ch === '}') {
        depth -= 1;
        if (depth === 0) {
          end = i;
          break;
        }
      }
    }
    if (end === -1) continue;
    try {
      const value: unknown = JSON.parse(text.slice(start, end + 1));
      if (isObj(value) && typeof value.schema === 'string') out.push(value);
    } catch {
      // Not a document on its own: keep scanning.
    }
  }
  return out;
}

function providerOf(v: unknown): Provider | null {
  if (!isObj(v)) return null;
  const id = str(v.id);
  const wire = v.wire === 'openai' || v.wire === 'anthropic' || v.wire === 'gemini' ? (v.wire as Wire) : null;
  if (id === '' || wire === null) return null;
  const base = str(v.path) || str(v.base_path) || str(v.base_url) || str(v.base);
  return { id, name: str(v.name) || id, wire, auth: str(v.auth) || 'bearer', base, key_env: str(v.key_env), key: str(v.key) };
}

function ratingOf(v: unknown): RatingRow | null {
  if (!isObj(v)) return null;
  const model = str(v.model);
  const provider = str(v.provider);
  if (model === '' || provider === '') return null;
  return {
    model,
    provider,
    rating_milli: num(v.rating_milli) ?? 0,
    context_length: num(v.context_length),
    tools: v.tools === true,
    vision: v.vision === true,
    json: v.json === true,
    free: v.free === true,
  };
}

function manifestOf(v: unknown): ManifestEntry | null {
  if (!isObj(v) || str(v.id) === '') return null;
  const r = isObj(v.requires) ? v.requires : {};
  const requires: Requires = {
    tools: r.tools === true,
    vision: r.vision === true,
    json: r.json === true,
  };
  const minContext = num(r.min_context);
  if (minContext !== null) requires.min_context = minContext;
  return {
    id: str(v.id),
    api: v.api === 'anthropic' ? 'anthropic' : 'openai',
    stream: v.stream === true,
    weight: num(v.weight) === 2 ? 2 : 1,
    requires,
  };
}

function readBrief(path: string): Brief {
  const empty: Brief = { providers: [], ratings: [], manifest: [], error: null };
  if (path === '') return { ...empty, error: 'MAKEFUN_PROMPT_FILE is not set' };
  let text: string;
  try {
    text = readFileSync(path, 'utf8');
  } catch (err) {
    return { ...empty, error: `cannot read the brief: ${(err as Error).message}` };
  }
  const docs = objectsIn(text);
  const directory = docs.find((d) => Array.isArray(d.providers)) ?? null;
  const ratingsDoc = docs.find((d) => Array.isArray(d.ratings)) ?? null;
  const workloadDoc = docs.find((d) => Array.isArray(d.manifest)) ?? null;
  if (directory === null) return { ...empty, error: 'the brief holds no provider directory' };
  const providers = (directory.providers as unknown[]).flatMap((v) => {
    const p = providerOf(v);
    return p === null ? [] : [p];
  });
  const ratings =
    ratingsDoc === null
      ? []
      : (ratingsDoc.ratings as unknown[]).flatMap((v) => {
          const r = ratingOf(v);
          return r === null ? [] : [r];
        });
  const manifest =
    workloadDoc === null
      ? []
      : (workloadDoc.manifest as unknown[]).flatMap((v) => {
          const m = manifestOf(v);
          return m === null ? [] : [m];
        });
  return { providers, ratings, manifest, error: null };
}

const brief = readBrief(BRIEF_PATH);

function joinBase(base: string): string {
  const b = base.trim();
  if (/^https?:\/\//i.test(b)) return b.replace(/\/+$/, '');
  if (b === '') return ENV;
  return `${ENV}${b.startsWith('/') ? '' : '/'}${b.replace(/\/+$/, '')}`;
}

function authHeaders(provider: Provider): Dict {
  if (provider.key === '') return {};
  if (provider.auth === 'bearer') return { authorization: `Bearer ${provider.key}` };
  if (provider.auth === 'x-api-key') return { 'x-api-key': provider.key };
  if (provider.auth === 'x-goog-api-key') return { 'x-goog-api-key': provider.key };
  return {};
}

// ------------------------------------------------------------- the catalog

interface ListedModel {
  id: string;
  display_name: string;
  free: boolean | null;
  context_length: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

interface Probed {
  provider: Provider;
  ok: boolean;
  status: number;
  models: ListedModel[];
  error: string | null;
}

interface Caps {
  context_length: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

interface Candidate {
  provider: Provider;
  model: ListedModel;
  rating: number;
  caps: Caps;
}

const ratingIndex = new Map<string, RatingRow>();
for (const row of brief.ratings) ratingIndex.set(`${row.provider}\u0000${row.model}`, row);

const rowOf = (providerId: string, modelId: string): RatingRow | null => ratingIndex.get(`${providerId}\u0000${modelId}`) ?? null;

/** A listing's price columns: true when every price is zero, null when it says nothing about price. */
function zeroPriced(v: Dict): boolean | null {
  if (!isObj(v.pricing)) return null;
  const values = Object.values(v.pricing).flatMap((x) => {
    if (typeof x === 'number') return [x];
    if (typeof x === 'string' && x.trim() !== '') return [Number(x)];
    return [];
  });
  if (values.length === 0) return null;
  return values.every((n) => Number.isFinite(n) && n === 0);
}

function listedModel(wire: Wire, v: unknown): ListedModel | null {
  if (!isObj(v)) return null;
  const id = wire === 'gemini' ? str(v.name).replace(/^models\//, '') : str(v.id);
  if (id === '') return null;
  return {
    id,
    display_name: str(v.display_name) || str(v.displayName) || id,
    free: bool(v.free) ?? zeroPriced(v),
    context_length: num(v.context_length) ?? num(v.context_window) ?? num(v.inputTokenLimit),
    tools: bool(v.tools),
    vision: bool(v.vision),
    json: bool(v.json),
  };
}

async function probe(provider: Provider): Promise<Probed> {
  const base = joinBase(provider.base);
  const url = provider.wire === 'gemini' ? `${base}/v1beta/models` : `${base}/v1/models`;
  const headers: Dict = { accept: 'application/json', ...authHeaders(provider) };
  if (provider.wire === 'anthropic') headers['anthropic-version'] = '2023-06-01';
  try {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) });
    if (!res.ok) return { provider, ok: false, status: res.status, models: [], error: `GET ${url} answered HTTP ${res.status}` };
    const body: unknown = await res.json();
    const list = isObj(body) ? (provider.wire === 'gemini' ? body.models : body.data) : null;
    const models = Array.isArray(list)
      ? list.flatMap((v) => {
          const m = listedModel(provider.wire, v);
          return m === null ? [] : [m];
        })
      : [];
    return { provider, ok: true, status: res.status, models, error: null };
  } catch (err) {
    return { provider, ok: false, status: 0, models: [], error: `GET ${url} failed: ${(err as Error).message}` };
  }
}

let probes: Probed[] = [];

/** Free by the quality table where it has a row (the sealed truth), else by the listing's own word. */
function isFree(providerId: string, model: ListedModel): boolean {
  const row = rowOf(providerId, model.id);
  if (row !== null) return row.free;
  return model.free === true;
}

function capsOf(providerId: string, model: ListedModel): Caps {
  const row = rowOf(providerId, model.id);
  return {
    context_length: row?.context_length ?? model.context_length ?? null,
    tools: row ? row.tools : model.tools,
    vision: row ? row.vision : model.vision,
    json: row ? row.json : model.json,
  };
}

function meets(requires: Requires, caps: Caps): boolean {
  if (requires.tools === true && caps.tools !== true) return false;
  if (requires.vision === true && caps.vision !== true) return false;
  if (requires.json === true && caps.json !== true) return false;
  if (requires.min_context !== undefined && (caps.context_length === null || caps.context_length < requires.min_context)) return false;
  return true;
}

function buildCatalog(): Dict {
  const providers: Dict[] = [];
  for (const p of probes) {
    if (!p.ok) continue;
    const seen = new Set<string>();
    const models: Dict[] = [];
    for (const model of [...p.models].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
      if (seen.has(model.id)) continue;
      seen.add(model.id);
      const row = rowOf(p.provider.id, model.id);
      models.push({
        id: model.id,
        free: isFree(p.provider.id, model),
        context_length: row?.context_length ?? model.context_length ?? null,
      });
    }
    providers.push({ id: p.provider.id, base_url: p.provider.base, key_env: p.provider.key_env, wire: p.provider.wire, models });
  }
  return { providers };
}

let catalog: Dict = { providers: [] };

// -------------------------------------------------------------- the planner

/** Every free candidate that meets `requires`, best first: a same-wire provider ahead, then rating. */
function candidatesFor(requires: Requires, clientWire: 'openai' | 'anthropic'): Candidate[] {
  // JSON mode is read off the request body, and only the openai and gemini wires carry it: an
  // anthropic-wire provider parses no response_format, so an OpenAI client's JSON request routed
  // there comes back echoing json_mode false. Such a request is never planned onto that wire.
  const jsonCarried = requires.json === true && clientWire === 'openai';
  const out: Candidate[] = [];
  for (const p of probes) {
    if (!p.ok) continue;
    if (jsonCarried && p.provider.wire === 'anthropic') continue;
    for (const model of p.models) {
      if (!isFree(p.provider.id, model)) continue;
      const caps = capsOf(p.provider.id, model);
      if (!meets(requires, caps)) continue;
      const row = rowOf(p.provider.id, model.id);
      out.push({ provider: p.provider, model, rating: row?.rating_milli ?? 0, caps });
    }
  }
  const score = (c: Candidate): number => c.rating + (c.provider.wire === clientWire ? WIRE_BONUS : 0);
  return out.sort(
    (a, b) =>
      score(b) - score(a) ||
      (a.provider.id < b.provider.id ? -1 : a.provider.id > b.provider.id ? 1 : 0) ||
      (a.model.id < b.model.id ? -1 : a.model.id > b.model.id ? 1 : 0),
  );
}

/** requestId -> the ordered candidates for it, the planned pick first. */
const plan = new Map<string, Candidate[]>();

/**
 * Plan every manifest entry before the first request arrives. Heavier requests are placed first, so
 * the best models go where the score weighs them double; a provider is planned to PROVIDER_CAP calls
 * and then spills to the next model, which keeps the best provider's budget from being spent by the
 * first requests alone. A request with no candidate under the cap still gets its full ranked list.
 */
function planWorkload(): void {
  const entries = brief.manifest.map((entry, index) => ({ entry, index }));
  const byWeight = [...entries].sort((a, b) => b.entry.weight - a.entry.weight || a.index - b.index);
  const load = new Map<string, number>();
  for (const { entry } of byWeight) {
    const list = candidatesFor(entry.requires, entry.api);
    if (list.length === 0) {
      plan.set(entry.id, []);
      continue;
    }
    let pick = 0;
    for (let i = 0; i < list.length; i++) {
      if ((load.get(list[i].provider.id) ?? 0) < PROVIDER_CAP) {
        pick = i;
        break;
      }
    }
    const chosen = list[pick];
    load.set(chosen.provider.id, (load.get(chosen.provider.id) ?? 0) + 1);
    plan.set(entry.id, [chosen, ...list.filter((_, i) => i !== pick)]);
  }
}

/** Providers that answered 402 (their free budget is spent) are out for the rest of the trial. */
const spent = new Set<string>();

// ------------------------------------------------------------- the wires

interface NormTurn {
  role: 'user' | 'assistant';
  text: string;
  images: { mime: string; data: string }[];
}

interface NormRequest {
  api: 'openai' | 'anthropic';
  system: string;
  turns: NormTurn[];
  tools: { name: string; description: string; schema: Dict }[];
  toolChoice: unknown;
  maxTokens: number | null;
  stops: string[];
  jsonMode: boolean;
  stream: boolean;
  temperature: number | null;
  topP: number | null;
}

function textOfContent(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.flatMap((b) => (isObj(b) && typeof b.text === 'string' && (b.type === 'text' || b.type === 'input_text') ? [b.text] : [])).join('\n');
}

function dataUrlParts(url: string): { mime: string; data: string } | null {
  const m = /^data:([^;,]*)(?:;[^,]*)?;base64,(.*)$/s.exec(url);
  if (m === null) return null;
  return { mime: m[1] || 'image/png', data: m[2] };
}

/** One incoming client body as the normalized request every wire builder speaks. */
function normalize(api: 'openai' | 'anthropic', body: Dict): NormRequest {
  const req: NormRequest = {
    api,
    system: '',
    turns: [],
    tools: [],
    toolChoice: undefined,
    maxTokens: num(body.max_tokens) ?? num(body.max_completion_tokens),
    stops: [],
    jsonMode: false,
    stream: body.stream === true,
    temperature: num(body.temperature),
    topP: num(body.top_p),
  };
  if (api === 'openai') {
    const messages = Array.isArray(body.messages) ? body.messages : [];
    const systemParts: string[] = [];
    for (const raw of messages) {
      if (!isObj(raw)) continue;
      if (raw.role === 'system' || raw.role === 'developer') {
        const text = textOfContent(raw.content);
        if (text !== '') systemParts.push(text);
        continue;
      }
      if (raw.role !== 'user' && raw.role !== 'assistant' && raw.role !== 'tool') continue;
      const images: { mime: string; data: string }[] = [];
      let text = textOfContent(raw.content);
      if (Array.isArray(raw.content)) {
        for (const part of raw.content) {
          if (!isObj(part) || part.type !== 'image_url') continue;
          const url = isObj(part.image_url) ? str(part.image_url.url) : '';
          const parsed = dataUrlParts(url);
          if (parsed !== null) images.push(parsed);
        }
      }
      if (raw.role === 'tool') text = `tool result ${str(raw.tool_call_id)}: ${text}`;
      req.turns.push({ role: raw.role === 'assistant' ? 'assistant' : 'user', text, images });
    }
    req.system = systemParts.join('\n');
    const tools = Array.isArray(body.tools) ? body.tools : [];
    for (const tool of tools) {
      if (!isObj(tool) || !isObj(tool.function)) continue;
      req.tools.push({
        name: str(tool.function.name),
        description: str(tool.function.description),
        schema: isObj(tool.function.parameters) ? tool.function.parameters : { type: 'object', properties: {} },
      });
    }
    req.toolChoice = body.tool_choice;
    if (typeof body.stop === 'string') req.stops = [body.stop];
    else if (Array.isArray(body.stop)) req.stops = body.stop.filter((s): s is string => typeof s === 'string');
    const format = isObj(body.response_format) ? str(body.response_format.type) : '';
    req.jsonMode = format === 'json_object' || format === 'json_schema';
  } else {
    req.system = textOfContent(body.system);
    const messages = Array.isArray(body.messages) ? body.messages : [];
    for (const raw of messages) {
      if (!isObj(raw)) continue;
      const assistant = raw.role === 'assistant';
      const content = raw.content;
      if (typeof content === 'string') {
        req.turns.push({ role: assistant ? 'assistant' : 'user', text: content, images: [] });
        continue;
      }
      if (!Array.isArray(content)) continue;
      const images: { mime: string; data: string }[] = [];
      const texts: string[] = [];
      for (const block of content) {
        if (!isObj(block)) continue;
        if (block.type === 'text' && typeof block.text === 'string') texts.push(block.text);
        else if (block.type === 'image' && isObj(block.source)) {
          const source = block.source;
          if (source.type === 'base64' && typeof source.data === 'string') images.push({ mime: str(source.media_type) || 'image/png', data: source.data });
          else if (source.type === 'url') {
            const parsed = dataUrlParts(str(source.url));
            if (parsed !== null) images.push(parsed);
          }
        } else if (block.type === 'tool_use') {
          texts.push(`tool call ${str(block.id)} ${str(block.name)} ${JSON.stringify(isObj(block.input) ? block.input : {})}`);
        } else if (block.type === 'tool_result') {
          const inner = typeof block.content === 'string' ? block.content : JSON.stringify(block.content ?? '');
          texts.push(`tool result ${str(block.tool_use_id)}: ${inner}`);
        }
      }
      req.turns.push({ role: assistant ? 'assistant' : 'user', text: texts.join('\n'), images });
    }
    const tools = Array.isArray(body.tools) ? body.tools : [];
    for (const tool of tools) {
      if (!isObj(tool) || str(tool.name) === '') continue;
      req.tools.push({
        name: str(tool.name),
        description: str(tool.description),
        schema: isObj(tool.input_schema) ? tool.input_schema : { type: 'object', properties: {} },
      });
    }
    req.toolChoice = body.tool_choice;
    if (Array.isArray(body.stop_sequences)) req.stops = body.stop_sequences.filter((s): s is string => typeof s === 'string');
  }
  return req;
}

/** The request body for a provider on the openai wire, built from either client wire. */
function openAIBody(req: NormRequest, model: string): Dict {
  const messages: Dict[] = [];
  for (const turn of req.turns) {
    if (turn.images.length === 0) {
      messages.push({ role: turn.role, content: turn.text });
      continue;
    }
    const parts: Dict[] = [];
    if (turn.text !== '') parts.push({ type: 'text', text: turn.text });
    for (const image of turn.images) parts.push({ type: 'image_url', image_url: { url: `data:${image.mime};base64,${image.data}` } });
    messages.push({ role: turn.role, content: parts });
  }
  if (messages.length === 0) messages.push({ role: 'user', content: '' });
  if (req.system !== '') messages.unshift({ role: 'system', content: req.system });
  const body: Dict = { model, messages };
  if (req.maxTokens !== null) body.max_tokens = req.maxTokens;
  if (req.temperature !== null) body.temperature = req.temperature;
  if (req.topP !== null) body.top_p = req.topP;
  if (req.stops.length > 0) body.stop = req.stops;
  if (req.tools.length > 0) {
    body.tools = req.tools.map((t) => ({ type: 'function', function: { name: t.name, description: t.description, parameters: t.schema } }));
    if (req.toolChoice !== undefined) body.tool_choice = req.toolChoice;
  }
  if (req.jsonMode) body.response_format = { type: 'json_object' };
  if (req.stream) {
    body.stream = true;
    body.stream_options = { include_usage: true };
  }
  return body;
}

/** The request body for a provider on the anthropic wire. */
function anthropicBody(req: NormRequest, model: string): Dict {
  const messages: Dict[] = [];
  for (const turn of req.turns) {
    const blocks: Dict[] = [];
    if (turn.text !== '' || turn.images.length === 0) blocks.push({ type: 'text', text: turn.text });
    for (const image of turn.images) blocks.push({ type: 'image', source: { type: 'base64', media_type: image.mime, data: image.data } });
    messages.push({ role: turn.role, content: blocks });
  }
  if (messages.length === 0) messages.push({ role: 'user', content: [{ type: 'text', text: '' }] });
  const body: Dict = { model, messages, max_tokens: req.maxTokens ?? 1024 };
  if (req.system !== '') body.system = req.system;
  if (req.temperature !== null) body.temperature = req.temperature;
  if (req.topP !== null) body.top_p = req.topP;
  if (req.stops.length > 0) body.stop_sequences = req.stops;
  if (req.tools.length > 0) {
    body.tools = req.tools.map((t) => ({ name: t.name, description: t.description, input_schema: t.schema }));
    const choice = req.toolChoice;
    if (isObj(choice)) {
      if (choice.type === 'auto') body.tool_choice = { type: 'auto' };
      else if (choice.type === 'any' || choice.type === 'required') body.tool_choice = { type: 'any' };
      else if (choice.type === 'tool' && typeof choice.name === 'string') body.tool_choice = { type: 'tool', name: choice.name };
      else if (isObj(choice.function) && typeof choice.function.name === 'string') body.tool_choice = { type: 'tool', name: choice.function.name };
    }
  }
  if (req.stream) body.stream = true;
  return body;
}

/** The request body for a provider on the gemini wire. */
function geminiBody(req: NormRequest): Dict {
  const contents: Dict[] = [];
  for (const turn of req.turns) {
    const parts: Dict[] = [];
    if (turn.text !== '' || turn.images.length === 0) parts.push({ text: turn.text });
    for (const image of turn.images) parts.push({ inlineData: { mimeType: image.mime, data: image.data } });
    contents.push({ role: turn.role === 'assistant' ? 'model' : 'user', parts });
  }
  if (contents.length === 0) contents.push({ role: 'user', parts: [{ text: '' }] });
  const body: Dict = { contents };
  if (req.system !== '') body.systemInstruction = { parts: [{ text: req.system }] };
  if (req.tools.length > 0) {
    body.tools = [{ functionDeclarations: req.tools.map((t) => ({ name: t.name, description: t.description, parameters: t.schema })) }];
  }
  const config: Dict = {};
  if (req.maxTokens !== null) config.maxOutputTokens = req.maxTokens;
  if (req.stops.length > 0) config.stopSequences = req.stops;
  if (req.temperature !== null) config.temperature = req.temperature;
  if (req.topP !== null) config.topP = req.topP;
  if (req.jsonMode) config.responseMimeType = 'application/json';
  if (Object.keys(config).length > 0) body.generationConfig = config;
  return body;
}

interface Answer {
  text: string;
  finish: string;
  usage: { input: number; output: number } | null;
}

function finishFamily(finish: string): 'stop' | 'length' | 'tool_calls' | 'content_filter' {
  if (finish === 'length' || finish === 'MAX_TOKENS' || finish === 'max_tokens') return 'length';
  if (finish === 'tool_calls' || finish === 'function_call' || finish === 'tool_use') return 'tool_calls';
  if (finish === 'content_filter' || finish === 'refusal' || finish === 'SAFETY') return 'content_filter';
  return 'stop';
}

function sseFrames(text: string): { event: string | null; data: string }[] {
  const out: { event: string | null; data: string }[] = [];
  for (const frame of text.replace(/\r\n/g, '\n').split('\n\n')) {
    if (frame.trim() === '') continue;
    let event: string | null = null;
    const data: string[] = [];
    for (const line of frame.split('\n')) {
      if (line.startsWith('event:')) event = line.slice(6).trim();
      else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
    }
    if (data.length > 0) out.push({ event, data: data.join('\n') });
  }
  return out;
}

function textOfParts(parts: unknown): string {
  if (!Array.isArray(parts)) return '';
  return parts.flatMap((p) => (isObj(p) && typeof p.text === 'string' ? [p.text] : [])).join('');
}

function answerFromOpenAI(wireBody: unknown): Answer | null {
  if (!isObj(wireBody)) return null;
  const choices = Array.isArray(wireBody.choices) ? wireBody.choices : [];
  const choice = isObj(choices[0]) ? choices[0] : null;
  if (choice === null || !isObj(choice.message)) return null;
  const usage = isObj(wireBody.usage) ? { input: num(wireBody.usage.prompt_tokens) ?? 0, output: num(wireBody.usage.completion_tokens) ?? 0 } : null;
  const content = choice.message.content;
  return { text: typeof content === 'string' ? content : textOfContent(content), finish: str(choice.finish_reason) || 'stop', usage };
}

function answerFromOpenAIStream(text: string): Answer | null {
  let out = '';
  let finish = 'stop';
  let usage: { input: number; output: number } | null = null;
  for (const frame of sseFrames(text)) {
    if (frame.data.trim() === '[DONE]') continue;
    let chunk: unknown;
    try {
      chunk = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isObj(chunk)) continue;
    if (isObj(chunk.usage)) usage = { input: num(chunk.usage.prompt_tokens) ?? 0, output: num(chunk.usage.completion_tokens) ?? 0 };
    const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
    const choice = isObj(choices[0]) ? choices[0] : null;
    if (choice === null) continue;
    if (isObj(choice.delta) && typeof choice.delta.content === 'string') out += choice.delta.content;
    if (typeof choice.finish_reason === 'string') finish = choice.finish_reason;
  }
  return { text: out, finish, usage };
}

function answerFromAnthropic(wireBody: unknown): Answer | null {
  if (!isObj(wireBody) || !Array.isArray(wireBody.content)) return null;
  const usage = isObj(wireBody.usage) ? { input: num(wireBody.usage.input_tokens) ?? 0, output: num(wireBody.usage.output_tokens) ?? 0 } : null;
  return { text: textOfParts(wireBody.content), finish: str(wireBody.stop_reason) || 'end_turn', usage };
}

function answerFromAnthropicStream(text: string): Answer | null {
  let out = '';
  let finish = 'end_turn';
  let input: number | null = null;
  let output: number | null = null;
  for (const frame of sseFrames(text)) {
    let data: unknown;
    try {
      data = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isObj(data)) continue;
    const type = frame.event ?? str(data.type);
    if (type === 'message_start') {
      if (isObj(data.message) && isObj(data.message.usage)) input = num(data.message.usage.input_tokens);
    } else if (type === 'content_block_delta') {
      if (isObj(data.delta) && typeof data.delta.text === 'string') out += data.delta.text;
    } else if (type === 'message_delta') {
      if (isObj(data.delta) && typeof data.delta.stop_reason === 'string') finish = data.delta.stop_reason;
      if (isObj(data.usage)) output = num(data.usage.output_tokens) ?? output;
    }
  }
  return { text: out, finish, usage: input === null && output === null ? null : { input: input ?? 0, output: output ?? 0 } };
}

function answerFromGemini(wireBody: unknown): Answer | null {
  if (!isObj(wireBody)) return null;
  const candidates = Array.isArray(wireBody.candidates) ? wireBody.candidates : [];
  const first = isObj(candidates[0]) ? candidates[0] : null;
  if (first === null) return null;
  const parts = isObj(first.content) ? first.content.parts : null;
  const meta = isObj(wireBody.usageMetadata) ? wireBody.usageMetadata : null;
  return {
    text: textOfParts(parts),
    finish: str(first.finishReason) || 'STOP',
    usage: meta === null ? null : { input: num(meta.promptTokenCount) ?? 0, output: num(meta.candidatesTokenCount) ?? 0 },
  };
}

function answerFromGeminiStream(text: string): Answer | null {
  let out = '';
  let finish = 'STOP';
  let usage: { input: number; output: number } | null = null;
  for (const frame of sseFrames(text)) {
    let chunk: unknown;
    try {
      chunk = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isObj(chunk)) continue;
    if (isObj(chunk.usageMetadata)) usage = { input: num(chunk.usageMetadata.promptTokenCount) ?? 0, output: num(chunk.usageMetadata.candidatesTokenCount) ?? 0 };
    const candidates = Array.isArray(chunk.candidates) ? chunk.candidates : [];
    const first = isObj(candidates[0]) ? candidates[0] : null;
    if (first === null) continue;
    out += textOfParts(isObj(first.content) ? first.content.parts : null);
    if (typeof first.finishReason === 'string') finish = first.finishReason;
  }
  return { text: out, finish, usage };
}

/** One provider call: the answer, or the refusal that sends the request on to another model. */
async function callUpstream(candidate: Candidate, req: NormRequest): Promise<{ answer: Answer } | { refused: { status: number; code: string } }> {
  const provider = candidate.provider;
  const model = candidate.model.id;
  const base = joinBase(provider.base);
  const headers: Dict = {
    'content-type': 'application/json',
    accept: req.stream ? 'text/event-stream' : 'application/json',
    ...authHeaders(provider),
  };
  let url: string;
  let body: Dict;
  if (provider.wire === 'openai') {
    url = `${base}/v1/chat/completions`;
    body = openAIBody(req, model);
  } else if (provider.wire === 'anthropic') {
    url = `${base}/v1/messages`;
    headers['anthropic-version'] = '2023-06-01';
    body = anthropicBody(req, model);
  } else {
    url = `${base}/v1beta/models/${encodeURIComponent(model)}:${req.stream ? 'streamGenerateContent?alt=sse' : 'generateContent'}`;
    body = geminiBody(req);
  }
  let res: Response;
  try {
    res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body), signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS) });
  } catch (err) {
    return { refused: { status: 0, code: `unreachable: ${(err as Error).message}` } };
  }
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    let code = '';
    try {
      const parsed: unknown = JSON.parse(text);
      if (isObj(parsed)) {
        if (isObj(parsed.error)) code = str(parsed.error.code) || str(parsed.error.type) || str(parsed.error.status);
        if (code === '') code = str(parsed.code);
      }
    } catch {
      // Not JSON: the HTTP status alone names the refusal.
    }
    return { refused: { status: res.status, code } };
  }
  const raw = await res.text();
  let answer: Answer | null;
  if (req.stream) {
    answer = provider.wire === 'openai' ? answerFromOpenAIStream(raw) : provider.wire === 'anthropic' ? answerFromAnthropicStream(raw) : answerFromGeminiStream(raw);
  } else {
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return { refused: { status: 0, code: 'unreadable body' } };
    }
    answer = provider.wire === 'openai' ? answerFromOpenAI(parsed) : provider.wire === 'anthropic' ? answerFromAnthropic(parsed) : answerFromGemini(parsed);
  }
  if (answer === null) return { refused: { status: 0, code: 'unreadable answer' } };
  return { answer };
}

// ------------------------------------------------------------- the answers

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) });
  res.end(text);
}

function sendSse(res: ServerResponse, text: string): void {
  res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache', connection: 'keep-alive' });
  res.end(text);
}

function sseEvent(event: string | null, data: unknown): string {
  return `${event === null ? '' : `event: ${event}\n`}data: ${JSON.stringify(data)}\n\n`;
}

/** The answer as an OpenAI completion. */
function openAIAnswer(answer: Answer): Dict {
  const usage = answer.usage ?? { input: 0, output: 0 };
  const family = finishFamily(answer.finish);
  return {
    id: 'chatcmpl-agg',
    object: 'chat.completion',
    created: 1767225600,
    model: 'auto',
    choices: [
      {
        index: 0,
        message: { role: 'assistant', content: answer.text },
        finish_reason: family,
        logprobs: null,
      },
    ],
    usage: { prompt_tokens: usage.input, completion_tokens: usage.output, total_tokens: usage.input + usage.output },
  };
}

/** The answer as an OpenAI event stream: a role chunk, the text, the finish chunk, usage, [DONE]. */
function openAIStreamText(answer: Answer): string {
  const usage = answer.usage ?? { input: 0, output: 0 };
  const family = finishFamily(answer.finish);
  const chunk = (delta: Dict, finish: string | null): Dict => ({
    id: 'chatcmpl-agg',
    object: 'chat.completion.chunk',
    created: 1767225600,
    model: 'auto',
    choices: [{ index: 0, delta, finish_reason: finish }],
  });
  const frames: string[] = [sseEvent(null, chunk({ role: 'assistant', content: '' }, null))];
  for (let at = 0; at < answer.text.length; at += 64) frames.push(sseEvent(null, chunk({ content: answer.text.slice(at, at + 64) }, null)));
  frames.push(sseEvent(null, chunk({}, family)));
  frames.push(
    sseEvent(null, {
      id: 'chatcmpl-agg',
      object: 'chat.completion.chunk',
      created: 1767225600,
      model: 'auto',
      choices: [],
      usage: { prompt_tokens: usage.input, completion_tokens: usage.output, total_tokens: usage.input + usage.output },
    }),
  );
  frames.push('data: [DONE]\n\n');
  return frames.join('');
}

/** The answer as an Anthropic message. */
function anthropicAnswer(answer: Answer): Dict {
  const usage = answer.usage ?? { input: 0, output: 0 };
  const family = finishFamily(answer.finish);
  return {
    id: 'msg_agg',
    type: 'message',
    role: 'assistant',
    model: 'auto',
    content: [{ type: 'text', text: answer.text }],
    stop_reason: family === 'length' ? 'max_tokens' : family === 'tool_calls' ? 'tool_use' : family === 'content_filter' ? 'refusal' : 'end_turn',
    stop_sequence: null,
    usage: { input_tokens: usage.input, output_tokens: usage.output },
  };
}

/** The answer as the Anthropic event order: message_start, one text block, message_delta, message_stop. */
function anthropicStreamText(answer: Answer): string {
  const usage = answer.usage ?? { input: 0, output: 0 };
  const family = finishFamily(answer.finish);
  const reason = family === 'length' ? 'max_tokens' : family === 'tool_calls' ? 'tool_use' : family === 'content_filter' ? 'refusal' : 'end_turn';
  const frames: string[] = [
    sseEvent('message_start', {
      type: 'message_start',
      message: { id: 'msg_agg', type: 'message', role: 'assistant', model: 'auto', content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: usage.input, output_tokens: 0 } },
    }),
    sseEvent('content_block_start', { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }),
  ];
  for (let at = 0; at < answer.text.length; at += 64) {
    frames.push(sseEvent('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: answer.text.slice(at, at + 64) } }));
  }
  frames.push(sseEvent('content_block_stop', { type: 'content_block_stop', index: 0 }));
  frames.push(sseEvent('message_delta', { type: 'message_delta', delta: { stop_reason: reason, stop_sequence: null }, usage: { output_tokens: usage.output } }));
  frames.push(sseEvent('message_stop', { type: 'message_stop' }));
  return frames.join('');
}

function servedModels(): Dict[] {
  const out: Dict[] = [];
  for (const p of probes) {
    if (!p.ok) continue;
    for (const model of [...p.models].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
      if (!isFree(p.provider.id, model)) continue;
      out.push({ id: model.id, provider: p.provider.id, display_name: model.display_name });
    }
  }
  return out;
}

function openAIModelList(): Dict {
  return { object: 'list', data: servedModels().map((m) => ({ id: m.id, object: 'model', created: 1767225600, owned_by: m.provider })) };
}

function anthropicModelList(): Dict {
  return {
    data: servedModels().map((m) => ({ type: 'model', id: m.id, display_name: m.display_name, created_at: '2026-01-01T00:00:00Z' })),
    has_more: false,
  };
}

function errorBody(api: 'openai' | 'anthropic', status: number, message: string): Dict {
  if (api === 'anthropic') {
    const type = status === 400 ? 'invalid_request_error' : status === 429 ? 'rate_limit_error' : 'api_error';
    return { type: 'error', error: { type, message } };
  }
  return { error: { message, type: status === 429 ? 'rate_limit_error' : 'invalid_request_error', code: null } };
}

async function readJson(req: IncomingMessage): Promise<Dict | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isObj(value) ? value : null;
  } catch {
    return null;
  }
}

/** Requirements read off a request body, for a brief whose manifest holds no row for it. */
function requiresFromBody(api: 'openai' | 'anthropic', body: Dict): Requires {
  const req = normalize(api, body);
  const requires: Requires = { tools: req.tools.length > 0, vision: req.turns.some((t) => t.images.length > 0), json: req.jsonMode };
  return requires;
}

let nextRequest = 0;

/** The manifest entry now arriving: the client sends the workload one request at a time, in order. */
function entryFor(api: 'openai' | 'anthropic', body: Dict): { id: string; requires: Requires } {
  const entry = brief.manifest[nextRequest];
  nextRequest += 1;
  if (entry !== undefined) return { id: entry.id, requires: entry.requires };
  return { id: `ad-hoc-${nextRequest}`, requires: requiresFromBody(api, body) };
}

/** Serve one client request: the planned candidates first, then the rest of the ranked list. */
async function serve(api: 'openai' | 'anthropic', body: Dict, res: ServerResponse): Promise<void> {
  const entry = entryFor(api, body);
  const req = normalize(api, body);
  req.stream = body.stream === true;
  const planned = plan.get(entry.id) ?? [];
  const candidates = planned.length > 0 ? planned : candidatesFor(entry.requires, api);
  const tried = new Set<string>();
  const refused: string[] = [];
  for (const candidate of candidates) {
    const key = `${candidate.provider.id}/${candidate.model.id}`;
    if (tried.has(key)) continue;
    tried.add(key);
    if (spent.has(candidate.provider.id)) continue;
    const result = await callUpstream(candidate, req);
    if ('refused' in result) {
      refused.push(`${key}: ${result.refused.status} ${result.refused.code}`);
      if (result.refused.status === 402) spent.add(candidate.provider.id);
      log(`refused ${key}: ${result.refused.status} ${result.refused.code}`);
      continue;
    }
    const answer = result.answer;
    if (api === 'openai') {
      if (req.stream) sendSse(res, openAIStreamText(answer));
      else sendJson(res, 200, openAIAnswer(answer));
      return;
    }
    if (req.stream) sendSse(res, anthropicStreamText(answer));
    else sendJson(res, 200, anthropicAnswer(answer));
    return;
  }
  log(`nothing served ${entry.id}: ${refused.join('; ')}`);
  sendJson(res, 503, errorBody(api, 503, `no free model served this request: ${refused.join('; ')}`));
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, anthropicModelList());
    else sendJson(res, 200, openAIModelList());
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    const body = await readJson(req);
    if (body === null) return sendJson(res, 400, errorBody('openai', 400, 'the request body is not one JSON object'));
    await serve('openai', body, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/messages') {
    const body = await readJson(req);
    if (body === null) return sendJson(res, 400, errorBody('anthropic', 400, 'the request body is not one JSON object'));
    await serve('anthropic', body, res);
    return;
  }
  const api = url.pathname === '/v1/messages' ? 'anthropic' : 'openai';
  sendJson(res, 404, errorBody(api, 404, `no route ${req.method ?? ''} ${url.pathname}`));
}

async function register(port: number): Promise<void> {
  const res = await fetch(`${ENV}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog }),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`POST /register answered HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
  const reply: unknown = await res.json();
  if (isObj(reply) && typeof reply.requests === 'number') log(`registered with ${reply.requests} requests`);
}

async function start(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  probes = await Promise.all(brief.providers.map((p) => probe(p)));
  for (const p of probes) if (!p.ok && p.error !== null) log(p.error);
  catalog = buildCatalog();
  planWorkload();

  if (process.argv.includes('--list')) {
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV === '') throw new Error('MAKEFUN_ENV_URL is not set');

  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) sendJson(res, 500, errorBody(req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'the server failed to answer'));
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => resolve());
  });
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  if (address.port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout');
  log(`listening on 0.0.0.0:${address.port}: ${catalog.providers.length} providers, ${servedModels().length} free models, ${plan.size} planned requests`);
  await register(address.port);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));

  let inFlight = false;
  setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isObj(work) && work.done === true) {
          server.close();
          process.exit(0);
        }
      } catch {
        // The env is not up yet: the next tick tries again.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_MS);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
