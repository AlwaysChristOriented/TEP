/*
 * Runs Fuse.js fuzzy search for the Bible and source-text indices off the
 * main thread. Typing in the search box posts a query here and waits for a
 * postMessage back — the main thread is never blocked by search cost,
 * regardless of query length or how large the indexed text is.
 */
// Vendored locally (not loaded from a CDN) so this worker never depends on a
// network fetch to start up — a service worker reliably caches same-origin
// requests across all browsers, whereas its ability to intercept a Worker's
// cross-origin importScripts() call is inconsistent (notably on Safari/iOS).
importScripts('./fuse.min.js');

// ignoreFieldNorm: Fuse penalizes matches by default the longer the field
// they're found in, which is right for something like a product-name search
// but wrong here — a Hadith entry can run many times longer than a Bible
// verse, so without this a short/partial query legitimately present in a
// long Hadith would score too poorly to clear threshold, even though the
// same text would match fine in a short verse. Disabling it means a match
// scores on how well it matches, not on how long the surrounding text is.
const FUSE_OPTIONS = { keys: ['text'], threshold: 0.32, includeScore: true, ignoreLocation: true, ignoreFieldNorm: true };

// Generous margin over every UI section's display cap (25 max) — keeps the
// postMessage payload small for common queries that match thousands of rows,
// without ever truncating what a panel actually needs to show.
const TOP_CAP = 30;

let bibleFuse = null;

// Fallback for queries where matching the whole string as one fuzzy pattern
// finds nothing (see buildBroadQuery in app.js) — e.g. a conversational
// question, or a curated topic-synonym expansion like "sodomite effeminate
// abomination" for a query of "homosexuality". Searches each word/phrase in
// `words` independently and merges hits by item, since Fuse has no built-in
// concept of "OR of N independent fuzzy patterns, ranked by how many hit."
// A small per-extra-match score bonus means a row containing several of the
// words ranks above one that only contains a single, weakly-matching word.
function orSearch(fuseIndex, words) {
  const bestByItem = new Map();
  words.forEach((word) => {
    fuseIndex.search(word).forEach((hit) => {
      const existing = bestByItem.get(hit.item);
      if (!existing) {
        bestByItem.set(hit.item, { item: hit.item, score: hit.score, matchCount: 1 });
      } else {
        existing.score = Math.min(existing.score, hit.score);
        existing.matchCount += 1;
      }
    });
  });
  const merged = [...bestByItem.values()].map((r) => ({
    item: r.item,
    score: Math.max(0, r.score - (r.matchCount - 1) * 0.05)
  }));
  merged.sort((a, b) => a.score - b.score);
  return merged;
}

// One Fuse index per source id (not one combined index across all source
// texts) so a tradition-filtered search only pays the cost of the
// collections it actually needs — e.g. filtering to Hinduism searches only
// the ~700-row Gita index instead of scanning all ~40k rows across every
// hadith collection too.
const sourceIndices = {}; // sourceId -> Fuse instance

self.onmessage = (e) => {
  const msg = e.data;

  if (msg.type === 'load') {
    const t0 = performance.now();
    if (msg.name === 'bible') {
      bibleFuse = msg.data.length ? new Fuse(msg.data, FUSE_OPTIONS) : null;
    } else if (msg.name === 'source') {
      const bySourceId = {};
      msg.data.forEach((row) => {
        (bySourceId[row.sourceId] = bySourceId[row.sourceId] || []).push(row);
      });
      Object.keys(sourceIndices).forEach((id) => delete sourceIndices[id]);
      Object.entries(bySourceId).forEach(([id, rows]) => {
        sourceIndices[id] = new Fuse(rows, FUSE_OPTIONS);
      });
    }
    console.log(`[search-worker] loaded '${msg.name}' (${msg.data.length} rows) in ${(performance.now() - t0).toFixed(1)}ms`);
    return;
  }

  if (msg.type === 'search') {
    const t0 = performance.now();
    const orWords = msg.orWords && msg.orWords.length ? msg.orWords : null;
    const bible = bibleFuse ? (orWords ? orSearch(bibleFuse, orWords) : bibleFuse.search(msg.query)) : [];
    const tBible = performance.now();

    const idsToSearch = msg.sourceIdWhitelist || Object.keys(sourceIndices);
    let source = [];
    const perSourceMs = {};
    idsToSearch.forEach((id) => {
      const idx = sourceIndices[id];
      if (!idx) return;
      const tStart = performance.now();
      source = source.concat(orWords ? orSearch(idx, orWords) : idx.search(msg.query));
      perSourceMs[id] = +(performance.now() - tStart).toFixed(1);
    });
    source.sort((a, b) => (a.score ?? 0) - (b.score ?? 0));
    const tSource = performance.now();

    const timing = {
      bibleMs: +(tBible - t0).toFixed(1),
      sourceMs: +(tSource - tBible).toFixed(1),
      totalMs: +(tSource - t0).toFixed(1),
      perSourceMs
    };
    console.log(`[search-worker] search "${msg.query}" —`, timing);

    self.postMessage({
      type: 'search-result',
      reqId: msg.reqId,
      phase: msg.phase,
      rawQuery: msg.rawQuery,
      query: msg.query,
      bible: { total: bible.length, top: bible.slice(0, TOP_CAP) },
      source: { total: source.length, top: source.slice(0, TOP_CAP) },
      timing
    });
  }
};
