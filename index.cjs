'use strict';

const manifest = require('./papercusp.json');
const API = 'https://graph.threads.net/v1.0';
const FIELDS = 'id,text,username,permalink,timestamp,media_type,root_post,replied_to,is_reply,is_reply_owned_by_me';

function fault(kind, message, extra) { return Object.assign(new Error(`threads:${message}`), { kind }, extra || {}, extra ? { data: extra } : {}); }
const str = (v) => typeof v === 'string' ? v : '';
const obj = (v) => !!v && typeof v === 'object' && !Array.isArray(v);
const instant = (v) => typeof v === 'string' && Number.isFinite(Date.parse(v));
function configOf(raw) {
  const c = raw || {}; const userId = str(c.userId);
  if (!/^\d+$/.test(userId)) throw fault('transient', 'invalid_user_id');
  const bounded = (key, max, fallback) => { const n = c[key] === undefined ? fallback : c[key]; if (!Number.isInteger(n) || n < 1 || n > max) throw fault('transient', `invalid_${key}`); return n; };
  return { userId, watchWindow: bounded('watchWindow', 25, 25), maxPagesPerMedia: bounded('maxPagesPerMedia', 50, 50) };
}
function urlFor(path, params) { const u = new URL(`${API}/${path.replace(/^\//, '')}`); for (const [k, v] of Object.entries(params || {})) u.searchParams.set(k, String(v)); return u.href; }
function nextAfter(body, current) {
  const raw = body?.paging?.next; if (!raw) return null;
  let next; try { next = new URL(raw); } catch { throw fault('transient', 'invalid_paging_url'); }
  const cur = new URL(current); if (next.origin !== cur.origin || next.pathname !== cur.pathname || next.username || next.password || next.hash) throw fault('transient', 'paging_outside_resource');
  const after = next.searchParams.get('after'); if (!after || after.length > 8192 || after === cur.searchParams.get('after')) throw fault('cursor-expired', 'paging_made_no_progress'); return after;
}
async function graph(host, source, url, method = 'GET', params) {
  const response = await host.fetch({ source, request: { url, method, ...(params ? { headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(params).toString() } : {}) } });
  let body; try { body = JSON.parse(response.body); } catch { body = null; }
  if (response.status === 401 || response.status === 403) throw fault('auth', 'grant_refused');
  if (response.status === 429) { const n = Number((response.headers || {})['retry-after']); throw fault('rate-limited', 'throttled', Number.isFinite(n) && n > 0 ? { retryAfterSeconds: Math.ceil(n) } : {}); }
  if (response.status < 200 || response.status >= 300 || body?.error || body === null) throw fault('transient', method === 'POST' ? 'write_unconfirmed_do_not_retry' : 'graph_request_failed');
  return body;
}
function targetId(value) { if (!/^[A-Za-z0-9_-]+$/.test(str(value))) throw fault('transient', 'invalid_object_id'); return value; }
const refId = (value) => str(value) || str(value?.id);
function record(row, root, owner, rootPost = false) {
  if (!obj(row) || !str(row.id) || !instant(row.timestamp)) throw fault('transient', 'malformed_reply');
  const parent = refId(row.replied_to); const rootId = refId(row.root_post) || root;
  const event = rootPost || row.is_reply === false ? 'post' : 'reply';
  const payload = { id: row.id, text: str(row.text), platform: 'threads', kind: event, subjectId: rootId, subjectOwnerId: owner, occurredAt: row.timestamp };
  if (str(row.username)) payload.author = row.username;
  if (parent) payload.replyToId = parent;
  if (str(row.permalink)) payload.url = row.permalink;
  if (row.is_reply_owned_by_me === true) payload.ownedByMe = true;
  if (!payload.text) payload.textAbsent = true;
  return { datatype: 'social-post', nativeId: row.id, event, payload, occurredAt: row.timestamp, version: `${row.id}@${row.timestamp}` };
}
function fresh(config, since = {}) { return { v: 1, config, phase: 'roots', after: null, page: 0, roots: [], index: 0, since, seen: {}, emitted: [] }; }
function validWatermark(w) { return obj(w) && instant(w.at) && Array.isArray(w.ids) && w.ids.every((x) => typeof x === 'string'); }
function decode(raw, config) {
  if (raw == null) return fresh(config); let s; try { s = JSON.parse(raw); } catch { throw fault('cursor-expired', 'cursor_unreadable'); }
  if (!obj(s) || s.v !== 1 || !['roots', 'conversation', 'ready'].includes(s.phase) || !obj(s.config) || !Array.isArray(s.roots) || s.roots.length > 25 || !s.roots.every((x) => typeof x === 'string' && /^[A-Za-z0-9_-]+$/.test(x)) || !Number.isInteger(s.index) || s.index < 0 || s.index > s.roots.length || !Number.isInteger(s.page) || s.page < 0 || s.page > 50 || !(s.after === null || (typeof s.after === 'string' && s.after.length < 8192)) || !obj(s.since) || !obj(s.seen) || ![...Object.values(s.since), ...Object.values(s.seen)].every(validWatermark) || !Array.isArray(s.emitted) || !s.emitted.every((x) => typeof x === 'string')) throw fault('cursor-expired', 'cursor_unrecognised');
  if (JSON.stringify(s.config) !== JSON.stringify(config)) return fresh(config); if (s.phase === 'ready') return fresh(config, s.since); return s;
}
function isNew(w, r) { return !w || Date.parse(r.occurredAt) > Date.parse(w.at) || (Date.parse(r.occurredAt) === Date.parse(w.at) && !w.ids.includes(r.nativeId)); }
function advance(w, r) { if (!w || Date.parse(r.occurredAt) > Date.parse(w.at)) return { at: r.occurredAt, ids: [r.nativeId] }; if (Date.parse(r.occurredAt) === Date.parse(w.at) && !w.ids.includes(r.nativeId)) return { at: w.at, ids: [...w.ids, r.nativeId] }; return w; }
async function syncPage(request, host) {
  const c = configOf(request.config); const s = decode(request.cursor, c); const catchUp = Object.keys(s.since).length ? { path: 'backfill', reason: 'no-replay-supported' } : { path: 'cold-start' };
  const records = []; const emitted = new Set(s.emitted);
  const admit = (row, stream) => {
    s.seen[stream] = advance(s.seen[stream] || s.since[stream], row);
    const identity = `${row.event}:${row.version}`;
    if (isNew(s.since[stream], row) && !emitted.has(identity)) { records.push(row); emitted.add(identity); }
  };
  if (s.phase === 'roots') {
    const u = urlFor(`${c.userId}/threads`, { fields: FIELDS, limit: c.watchWindow, ...(s.after ? { after: s.after } : {}) }); const body = await graph(host, request.source, u); if (!Array.isArray(body.data)) throw fault('transient', 'malformed_root_collection');
    for (const row of body.data) {
      targetId(row?.id);
      if (row.is_reply === true || refId(row.replied_to)) continue;
      admit(record(row, row.id, c.userId, true), 'posts');
      if (!s.roots.includes(row.id) && s.roots.length < c.watchWindow) s.roots.push(row.id);
    }
    s.after = nextAfter(body, u); s.page += 1; if (!s.after || s.page >= 50 || s.roots.length >= c.watchWindow) { s.phase = s.roots.length ? 'conversation' : 'ready'; s.after = null; s.page = 0; }
  } else {
    const root = s.roots[s.index]; if (!root) throw fault('cursor-expired', 'cursor_root_missing');
    const u = urlFor(`${root}/conversation`, { fields: FIELDS, limit: 50, reverse: true, ...(s.after ? { after: s.after } : {}) }); const body = await graph(host, request.source, u); if (!Array.isArray(body.data)) throw fault('transient', 'malformed_conversation');
    const next = nextAfter(body, u);
    // This is a runaway refusal, never a successful truncated rescan. In-flight
    // pages keep the ORIGINAL watermark, so older unseen replies are admitted.
    if (next && s.page + 1 >= c.maxPagesPerMedia) throw fault('transient', 'conversation_page_bound_exceeded');
    for (const row of body.data) admit(record(row, root, c.userId), root);
    s.after = next; s.page += 1;
    if (!s.after) { s.index += 1; s.page = 0; if (s.index >= s.roots.length) s.phase = 'ready'; }
  }
  s.emitted = [...emitted];
  if (s.phase === 'ready') {
    s.since = { ...s.since, ...s.seen };
    const active = new Set(['posts', ...s.roots]);
    s.since = Object.fromEntries(Object.entries(s.since).filter(([key]) => active.has(key)));
    s.seen = {}; s.emitted = [];
  }
  return { records, nextCursor: JSON.stringify(s), hasMore: s.phase !== 'ready', catchUp };
}
async function statusOf(host, source, container) {
  const body = await graph(host, source, urlFor(container, { fields: 'status' }));
  if (!['FINISHED', 'IN_PROGRESS', 'ERROR', 'EXPIRED', 'PUBLISHED'].includes(body?.status)) throw fault('transient', 'unrecognised_container_status');
  return body.status;
}
async function invoke(request, host) {
  if (!manifest.provider.capabilities.includes(request.capability)) throw fault('transient', 'capability_unsupported');
  const c = configOf(request.config); const a = request.args || {};
  const graphemes = [...new Intl.Segmenter(undefined, { granularity: 'grapheme' }).segment(str(a.text))].length;
  if (!str(a.text).trim() || graphemes > 500) throw fault('transient', 'invalid_text');
  const params = { media_type: 'TEXT', text: a.text };
  if (request.capability === 'social.post') {
    if (a.destination !== c.userId) throw fault('transient', 'destination_mismatch');
  } else {
    if (a.target?.platform && a.target.platform !== 'threads') throw fault('transient', 'wrong_platform_target');
    const stored = a.target && 'externalId' in a.target;
    if (stored && (!obj(a.target.payload) || typeof a.target.externalId !== 'string')) throw fault('transient', 'invalid_stored_target');
    // Threads addresses the exact nested reply, unlike Instagram's top-level
    // comment edge. Root ownership comes from ingestion, never caller approval.
    const owner = stored ? a.target.payload.subjectOwnerId : a.target?.rootPostOwnerId;
    if (owner !== c.userId) throw fault('auth', 'reply_root_not_owned');
    params.reply_to_id = targetId(stored ? a.target.externalId || str(a.target.payload.id) : a.target?.replyToId);
  }
  if (a.media !== undefined && (!Array.isArray(a.media) || a.media.length > 1)) throw fault('transient', 'carousel_unsupported');
  if (a.media?.length) {
    const media = a.media[0]; let u; try { u = new URL(media?.url); } catch { throw fault('transient', 'invalid_media'); }
    if (!['image', 'video'].includes(media.type) || u.protocol !== 'https:' || u.username || u.password) throw fault('transient', 'invalid_media');
    params.media_type = media.type.toUpperCase(); params[media.type === 'image' ? 'image_url' : 'video_url'] = u.href;
  }
  const create = await graph(host, request.source, urlFor(`${c.userId}/threads`), 'POST', params);
  const container = targetId(create.id); let status;
  // A FINISHED status is direct evidence the container has settled; otherwise
  // observe its lifecycle at Meta's documented cadence, without retrying writes.
  for (let minute = 0; minute <= 5; minute += 1) {
    status = await statusOf(host, request.source, targetId(container));
    if (status === 'FINISHED') break;
    if (status !== 'IN_PROGRESS' || minute === 5) throw fault('transient', `container_${status.toLowerCase()}`, { containerId: container });
    await new Promise((resolve) => setTimeout(resolve, 60_000));
  }
  try {
    const published = await graph(host, request.source, urlFor(`${c.userId}/threads_publish`), 'POST', { creation_id: container });
    if (!str(published.id)) throw fault('transient', 'write_unconfirmed_no_media_id');
    return { externalId: published.id, ...(str(published.permalink) ? { url: published.permalink } : {}) };
  } catch {
    let observedStatus = null; try { observedStatus = await statusOf(host, request.source, container); } catch {}
    throw fault('transient', 'publish_ambiguous_do_not_retry', { containerId: container, observedStatus, retrySafe: observedStatus === 'PUBLISHED' ? false : ['ERROR', 'EXPIRED'].includes(observedStatus) ? true : null });
  }
}
module.exports = { name: manifest.name, version: manifest.version, papercusp: manifest.papercusp, capabilities: [], providerAdapter: { describe: () => manifest.provider, syncPage, invoke } };
