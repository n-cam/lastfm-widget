require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const rateLimit = require('express-rate-limit');
const app = express();
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;
const AbortController = require('abort-controller');
const { v4: uuidv4 } = require('uuid'); // Add this to dependencies

// ==========================================
// 🚀 SHORT-TERM REQUEST CACHE
// ==========================================
const queryCache = new Map();

function getQueryCache(key) {
  const entry = queryCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expires) {
    queryCache.delete(key);
    return null;
  }
  return entry.data;
}

function setQueryCache(key, data, ttlMs = 600000) {
  if (queryCache.size > 1000) {
    const firstKey = queryCache.keys().next().value;
    queryCache.delete(firstKey);
  }
  
  queryCache.set(key, {
    data,
    expires: Date.now() + ttlMs
  });
}

// ==========================================
// 🆕 GLOBAL MUSICBRAINZ CACHE
// ==========================================
const mbGlobalCache = new Map();
const mbFailureCache = new Map();

// Database setup
let db;
let dbType;

if (process.env.DATABASE_URL) {
  console.log('🐘 Using PostgreSQL database');
  const { Pool } = require('pg');
  db = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    max: 15,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  });
  dbType = 'postgres';
} else {
  console.log('💾 Using SQLite database');
  const Database = require('better-sqlite3');
  db = new Database('database.db');
  db.pragma('journal_mode = WAL');
  dbType = 'sqlite';
}

const CACHED_USERS = (process.env.CACHED_USERS || '').split(',').filter(Boolean);

// ==========================================
// 🔍 PROGRESSIVE SCAN JOB TRACKING
// ==========================================
const scanJobs = new Map();

function createScanJob(username, filters, startRange, endRange, targetLimit, currentCount) {
  const jobId = uuidv4();
  scanJobs.set(jobId, {
    jobId,
    status: 'processing',
    username,
    filters,
    startRange,
    endRange,
    targetLimit,
    currentCount,
    progress: { current: 0, total: endRange - startRange, percentage: 0 },
    foundAlbums: [],
    clients: [],
    createdAt: Date.now(),
    lastUpdate: Date.now(),
    shouldStop: false
  });
  
  setTimeout(() => {
    const job = scanJobs.get(jobId);
    if (job) {
      job.clients.forEach(client => client.end());
      scanJobs.delete(jobId);
    }
  }, 60 * 60 * 1000);
  
  return jobId;
}

function updateJobProgress(jobId, update) {
  const job = scanJobs.get(jobId);
  if (!job) return;
  
  Object.assign(job, update);
  job.lastUpdate = Date.now();
  
  const message = JSON.stringify({
    status: job.status,
    progress: job.progress,
    foundAlbums: job.foundAlbums,
    newAlbumsCount: job.foundAlbums.length,
    timeRemaining: estimateTimeRemaining(job)
  });
  
  job.clients.forEach(client => {
    try {
      client.write(`data: ${message}\n\n`);
    } catch (e) {
      // Client disconnected
    }
  });
}

function estimateTimeRemaining(job) {
  if (job.progress.current === 0) return null;
  
  const elapsed = Date.now() - job.createdAt;
  const rate = elapsed / job.progress.current;
  const remaining = (job.progress.total - job.progress.current) * rate;
  
  const minutes = Math.ceil(remaining / 60000);
  if (minutes < 1) return '< 1 minute';
  if (minutes === 1) return '1 minute';
  return `${minutes} minutes`;
}

setInterval(() => {
  const now = Date.now();
  for (const [jobId, job] of scanJobs.entries()) {
    if ((job.status === 'complete' || job.status === 'stopped' || job.status === 'error') 
        && now - job.lastUpdate > 5 * 60 * 1000) {
      job.clients.forEach(client => {
        try { client.end(); } catch (e) {}
      });
      scanJobs.delete(jobId);
    }
  }
}, 60000);

// Initialize clean database schema
async function initDatabase() {
  if (dbType === 'postgres') {
    await db.query(`
      CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        last_update_full TIMESTAMP,
        last_update_recent TIMESTAMP,
        total_albums INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS albums_global (
        id SERIAL PRIMARY KEY,
        canonical_album TEXT NOT NULL,
        canonical_artist TEXT NOT NULL,
        album_name TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        release_year INTEGER,
        is_manual BOOLEAN DEFAULT FALSE, 
        musicbrainz_id TEXT,
        image_url TEXT,
        lastfm_url TEXT,
        album_type TEXT,
        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(canonical_album, canonical_artist)
      );

      CREATE TABLE IF NOT EXISTS user_albums (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        album_id INTEGER NOT NULL REFERENCES albums_global(id) ON DELETE CASCADE,
        playcount INTEGER DEFAULT 0,
        last_scrobble TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(username, album_id)
      );

      CREATE INDEX IF NOT EXISTS idx_user_albums_username ON user_albums(username);
      CREATE INDEX IF NOT EXISTS idx_user_albums_playcount ON user_albums(playcount);
      CREATE INDEX IF NOT EXISTS idx_albums_global_year ON albums_global(release_year);
      CREATE INDEX IF NOT EXISTS idx_albums_global_canonical ON albums_global(canonical_album, canonical_artist);
      CREATE INDEX IF NOT EXISTS idx_albums_global_mbid ON albums_global(musicbrainz_id);
    `);
  } else {
    db.exec(`
      CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        last_update_full TEXT,
        last_update_recent TEXT,
        total_albums INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS albums_global (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_album TEXT NOT NULL,
        canonical_artist TEXT NOT NULL,
        album_name TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        is_manual INTEGER DEFAULT 0,
        musicbrainz_id TEXT,
        release_year INTEGER,
        image_url TEXT,
        lastfm_url TEXT,
        album_type TEXT,
        first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(canonical_album, canonical_artist)
      );

      CREATE TABLE IF NOT EXISTS user_albums (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        album_id INTEGER NOT NULL REFERENCES albums_global(id) ON DELETE CASCADE,
        playcount INTEGER DEFAULT 0,
        last_scrobble TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(username, album_id)
      );

      CREATE INDEX IF NOT EXISTS idx_user_albums_username ON user_albums(username);
      CREATE INDEX IF NOT EXISTS idx_user_albums_playcount ON user_albums(playcount);
      CREATE INDEX IF NOT EXISTS idx_albums_global_year ON albums_global(release_year);
      CREATE INDEX IF NOT EXISTS idx_albums_global_canonical ON albums_global(canonical_album, canonical_artist);
      CREATE INDEX IF NOT EXISTS idx_albums_global_mbid ON albums_global(musicbrainz_id);
    `);
  }
}

async function dbQuery(query, params = []) {
  if (dbType === 'postgres') {
    // PostgreSQL: params should be an array
    const result = await db.query(query, params);
    return result.rows;
  }
  // SQLite: spread the params
  return db.prepare(query).all(...params);
}

async function dbGet(query, params = []) {
  if (dbType === 'postgres') {
    // PostgreSQL: params should be an array
    const result = await db.query(query, params);
    return result.rows[0] || null;
  }
  // SQLite: spread the params
  return db.prepare(query).get(...params);
}

async function dbRun(query, params = []) {
  if (dbType === 'postgres') {
    // PostgreSQL: params should be an array
    await db.query(query, params);
  } else {
    // SQLite: spread the params
    db.prepare(query).run(...params);
  }
}

initDatabase().then(async () => {
  console.log('✅ Database initialized');
  console.log('📋 Cached users:', CACHED_USERS.length > 0 ? CACHED_USERS.join(', ') : 'none');

  if (dbType === 'postgres') {
    try {
      const count = await dbGet('SELECT COUNT(*) as count FROM albums_global');
      console.log(`📊 Total albums in global cache: ${count.count}`);
    } catch (e) {
      console.log('Could not fetch initial stats');
    }
  }
});

// ============================================
// CENTRALIZED LAST.FM API CALLER
// ============================================
async function callLastFmAPI(method, params = {}, retries = 3) {
  const url = new URL('https://ws.audioscrobbler.com/2.0/');
  url.searchParams.set('method', method);
  url.searchParams.set('api_key', process.env.LASTFM_API_KEY);
  url.searchParams.set('format', 'json');
  
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }
  
  for (let attempt = 0; attempt < retries; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    const isLastAttempt = attempt === retries - 1;
    
    try {
      const response = await fetch(url.toString(), { 
        signal: controller.signal,
        headers: { 'User-Agent': 'LastFmTopAlbums/1.0.0 (contact@sortedsongs.com)' }
      });
      
      clearTimeout(timeout);

      // Handle specific retryable status codes
      if ((response.status === 429 || response.status === 503) && !isLastAttempt) {
        const waitTime = Math.pow(2, attempt) * 1000;
        console.warn(`⏳ Last.fm ${response.status}, retrying in ${waitTime/1000}s...`);
        await new Promise(r => setTimeout(r, waitTime));
        continue;
      }
      
      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const data = await response.json();

      // RED FLAG 2 FIX: Check for Last.fm internal errors
      if (data.error) {
        throw new Error(`Last.fm Error ${data.error}: ${data.message}`);
      }
      
      return data;
      
    } catch (err) {
      clearTimeout(timeout);
      
      // RED FLAG 1 FIX: Throw on last attempt or non-retryable error
      if (isLastAttempt || err.name === 'AbortError') {
        throw new Error(`Last.fm API failed: ${err.message}`);
      }
      
      const waitTime = Math.pow(2, attempt) * 1000;
      await new Promise(r => setTimeout(r, waitTime));
    }
  }
}

const cron = require('node-cron');

cron.schedule('0 3 * * 0', async () => {
  console.log('🔄 [' + new Date().toISOString() + '] Starting weekly auto-update...');
  
  for (const user of CACHED_USERS) {
    try {
      console.log(`  [${new Date().toISOString()}] Updating ${user}...`);
      await performBackgroundUpdate(user, false, 500);
      await new Promise(r => setTimeout(r, 60000));
    } catch (err) {
      console.error(`  [${new Date().toISOString()}] ❌ Failed to update ${user}:`, err);
    }
  }
  
  console.log('✅ [' + new Date().toISOString() + '] Weekly auto-update complete');
});

console.log('⏰ Cron job scheduled: Weekly updates every Sunday at 3am');

function cleanArtistName(name) {
  if (!name) return "";
  return name.trim();
}

function cleanAlbumName(name) {
  if (!name) return "";

  // 1. Remove specific years followed by "Remaster" (e.g., "2009 Remaster")
  const yearRemasterRegex = /\s*[\(\[]?\d{4}\s+Remaster(?:ed)?[\)\]]?/gi;

  // 2. Terms that are strictly "noise" when found in brackets or after separators
  const noiseTerms = [
    'Deluxe', 'Remastered', 'Remaster', 'Edition', 'Anniversary', 
    'Expanded', 'Special', 'Bonus', 'Live', 'Explicit', 'Extended', 
    'Target', 'Walmart', 'Japan', 'Import', 'Clean', 'Dirty', 
    'Digital', 'LP', 'Version', 'Set', 'Box Set', 'Mono', 'Stereo',
    'Reissue', 'Collector\'s', 'Standard', 'Super Deluxe',
    'Vol\\.?', 'Volume', 'Pt\\.?', 'Part',
    'Refill', 'Reloaded', 'Unlocked', 'Platinum', 'Legacy', 'Diamond'
  ].join('|');

  // SAFE: Remove noise inside brackets/parentheses
  // Matches: (Deluxe), [Remastered], (Expanded Edition)
  const bracketRegex = new RegExp(`\\s*[\\(\\[](?:${noiseTerms})[^\\)\\]]*[\\)\\]]`, 'gi');

  // SAFE: Remove noise after specific separators (Dash or Colon)
  // Matches: " - Remastered", " : Deluxe Edition"
  // Also matches: "Relapse: Refill", "SOS Deluxe: LANA" (because of the colon)
  const separatorRegex = new RegExp(`\\s*(?:-|:)\\s*(?:.*(?:${noiseTerms})).*`, 'gi');

  // SAFE: Remove specific multi-word phrases at the end of the string
  // We DO NOT remove just "Deluxe" to protect "Love Deluxe".
  // We ONLY remove "Deluxe Edition", "Deluxe Version", "Expanded Edition", etc.
  const phraseRegex = /\s+(?:Deluxe|Expanded|Special|Standard|Bonus)\s+(?:Edition|Version|Cut|Tracks).*$/i;

  // SPECIFIC FIX: Handle "SOS Deluxe: LANA" pattern where 'Deluxe' precedes the colon
  // Matches " Deluxe: ..." and removes it.
  // "Love Deluxe" (no colon) is SAFE. "SOS Deluxe: LANA" (has colon) is CLEANED.
  const deluxeColonRegex = /\s+Deluxe\s*:.*$/i;

  let cleaned = name;
  let prev;

  do {
    prev = cleaned;
    cleaned = cleaned
      .replace(yearRemasterRegex, '')
      .replace(bracketRegex, '')
      .replace(separatorRegex, '')
      .replace(phraseRegex, '')
      .replace(deluxeColonRegex, '')
      .replace(/\s+\(?(?:Mono|Stereo|Remaster(?:ed)?)\)?$/i, '') // Catch dangling stragglers
      .trim();
  } while (cleaned !== prev);

  return cleaned || name;
}

function normalizeForComparison(str) {
  if (!str) return '';
  try {
    return str.toLowerCase()
      // 1. Standardize "Stylized" and "Fancy" characters
      .replace(/[×✕✖]/g, 'x')      // Fixes Ed Sheeran (×) and Chloe × Halle
      .replace(/\$/g, 's')         // Fixes Ke$ha (Ke$ha -> kesha)
      .replace(/[‐‑‒–—]/g, '-')    // Fixes alt‐J
      .replace(/[‘’]/g, "'")       // Fixes curly apostrophes
      
      // 2. Decompose accents (é -> e)
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      
      // 3. Keep letters (including foreign scripts), numbers, and spaces
      // Use \p{L} for Unicode letter support (Kanji, Katakana, Hangul, etc.)
      .replace(/[^\p{L}\p{N}\s]/gu, '') 
      .replace(/\s+/g, ' ')
      .trim();
  } catch (e) {
    // Fallback for older environments
    return str.toLowerCase()
      .replace(/[×✕✖]/g, 'x')
      .replace(/[^a-z0-9\s]/gi, '')
      .trim();
  }
}

function stringSimilarity(str1, str2) {
  const s1 = normalizeForComparison(str1);
  const s2 = normalizeForComparison(str2);
  
  if (s1 === s2) return 1.0;
  if (s1.length === 0 || s2.length === 0) return 0.0;
  if (s1.includes(s2) || s2.includes(s1)) return 0.85;
  
  const chars1 = new Set(s1);
  const chars2 = new Set(s2);
  const intersection = new Set([...chars1].filter(x => chars2.has(x)));
  const union = new Set([...chars1, ...chars2]);
  
  const jaccard = intersection.size / union.size;
  const lengthRatio = Math.min(s1.length, s2.length) / Math.max(s1.length, s2.length);
  
  return jaccard * 0.7 + lengthRatio * 0.3;
}

// Add this helper function before buildSearchQueries
function toTitleCase(str) {
  if (!str) return str;
  
  // Special case: preserve single-letter album titles like "x" or "÷"
  if (str.length === 1) return str;
  
  return str
    .toLowerCase()
    .split(' ')
    .map((word, index) => {
      // Preserve single letters as-is (like "x" in "Chloe x Halle")
      if (word.length === 1) return word;
      
      const lowercase = ['a', 'an', 'the', 'and', 'or', 'but', 'of', 'in', 'on', 'at', 'to', 'for', 'with', 'from'];
      if (index === 0) return word.charAt(0).toUpperCase() + word.slice(1);
      return lowercase.includes(word) ? word : word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(' ');
}

function buildSearchQueries(artist, album) {
  const queries = [];
  
  const cleanA = artist.replace(/"/g, '');
  const cleanT = album.replace(/"/g, '');

  // 1. Strict Exact Match (Best for accuracy)
  queries.push(`releasegroup:"${cleanT}" AND artist:"${cleanA}"`);

  // 2. SHORT TITLE FIX (Fixes Ed Sheeran "X", Adele "21")
  // If title is 2 chars or less, FORCE strict title matching to avoid noise
  if (cleanT.length <= 2) {
     queries.push(`releasegroup:"${cleanT}" AND artist:(${cleanA})`);
     return queries; // Stop here for short titles to prevent bad matches
  }
  
  // 3. Strip "The" from Artist (Fixes 'The High Llamas')
  if (cleanA.toLowerCase().startsWith('the ')) {
    queries.push(`releasegroup:"${cleanT}" AND artist:"${cleanA.substring(4)}"`);
  }

  // 4. "Split" Artist Search (Fixes "Elvis Costello & The Attractions")
  // Searches for just "Elvis Costello" if the full band search fails
  const splitTerms = [' & ', ' x ', ' feat ', ' ft ', ' with '];
  for (const term of splitTerms) {
    if (cleanA.toLowerCase().includes(term)) {
        const primaryArtist = cleanA.split(new RegExp(term, 'i'))[0];
        if (primaryArtist.length > 2) {
            queries.push(`releasegroup:"${cleanT}" AND artist:"${primaryArtist}"`);
        }
    }
  }

  // 5. Loose/Fuzzy Search (Fixes punctuation issues like Mama's Gun)
  queries.push(`releasegroup:(${cleanT}) AND artist:(${cleanA})`);

  // 6. Super Loose (Remove special chars)
  const albumNoSpecial = cleanT.replace(/[^a-zA-Z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  const artistNoSpecial = cleanA.replace(/[^a-zA-Z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  
  if (albumNoSpecial !== cleanT || artistNoSpecial !== cleanA) {
    queries.push(`releasegroup:"${albumNoSpecial}" AND artist:"${artistNoSpecial}"`);
  }

  return queries;
}

function validateArtistMatch(normInput, normRgMain, allArtists, artistSim, searchTitleLower, titleSim) {
  // Exact match in any credited artist
  const isExactMatch = allArtists.some(a => a === normInput);
  if (isExactMatch) return true;
  
  // Close substring match
  const isSubstringMatch = allArtists.some(a => 
    (a.includes(normInput) && normInput.length > 3) || 
    (normInput.includes(a) && a.length > 3)
  );
  if (isSubstringMatch) return true;
  
  // Handle "The" prefix variations
  const inputNoThe = normInput.replace(/^the\s+/, '');
  const rgNoThe = normRgMain.replace(/^the\s+/, '');
  if (inputNoThe === rgNoThe && inputNoThe.length > 0) return true;
  
  // Handle "Ms." vs full name variations
  if (normInput.includes('ms ') || normRgMain.includes('ms ')) {
    const inputNoMs = normInput.replace(/ms\.?\s+/g, '');
    const rgNoMs = normRgMain.replace(/ms\.?\s+/g, '');
    if (inputNoMs === rgNoMs) return true;
  }
  
  // Various Artists / Soundtrack exception
  const isVAException = (
    normRgMain.includes('various artists') || 
    searchTitleLower.includes('cast') ||
    searchTitleLower.includes('soundtrack')
  ) && titleSim > 0.85;
  if (isVAException) return true;
  
  // Allow high similarity matches (helps with romanization, "x" vs "×", etc.)
  if (artistSim >= 0.90) return true;
  
  return false;
}

function calculateMatchScore(
  artistSimilarity, 
  titleSimilarity, 
  cleanedAlbum, 
  rgTitle,
  normInput,
  normRg,
  normTitleMB,
  normTitleSearch,
  searchTitleLower,
  primaryType,
  secondaryTypes
) {
  // Base Score
  let score = (artistSimilarity * 500) + (titleSimilarity * 500);

  // Severe penalty for artist mismatch
  if (artistSimilarity < 0.75) score -= 1500;
  
  // ---------------------------------------------------------
  // IMPROVED CORE TITLE CHECK (Punctuation Agnostic)
  // ---------------------------------------------------------
  // We strip ALL non-alphanumeric chars before checking inclusion.
  // This ensures "Mama's Gun" matches "Mamas Gun"
  const coreSearchToken = cleanedAlbum.split(/[(\-:]/)[0].toLowerCase().replace(/[^a-z0-9]/g, '');
  const coreRgToken = rgTitle.split(/[(\-:]/)[0].toLowerCase().replace(/[^a-z0-9]/g, '');
  
  if (!coreRgToken.includes(coreSearchToken) && !coreSearchToken.includes(coreRgToken)) {
    score -= 1000; 
  }

  // MASSIVE BONUS: Exact title match
  if (normTitleMB === normTitleSearch) score += 1000;

  // Type Logic
  if (primaryType === 'single' && normTitleMB !== normTitleSearch) score -= 1500;
  if (primaryType === 'ep' && normTitleMB !== normTitleSearch) score -= 800;
  if (primaryType === 'album') score += 500;
  if (primaryType === 'album' && secondaryTypes.length === 0) score += 300;

  // Word Count Penalty
  const searchWords = normTitleSearch.split(' ').length;
  const mbWords = normTitleMB.split(' ').length;
  if (mbWords > searchWords + 2) score -= 600;

  // Budget Keywords
  const budgetKeywords = ['tribute', 'karaoke', 'instrumental', 'version of', 'covers of'];
  if (budgetKeywords.some(word => normTitleMB.includes(word))) score -= 1500;

  // ---------------------------------------------------------
  // IMPROVED BAD TYPE LOGIC (Allow Compilations if Exact Match)
  // ---------------------------------------------------------
  const badTypes = ['live', 'demo', 'remix', 'dj-mix', 'compilation'];
  
  // Only penalize compilation if the title isn't an exact match
  const isExactMatch = normTitleMB === normTitleSearch;
  
  if (secondaryTypes.some(t => badTypes.includes(t)) && !badTypes.some(t => normTitleSearch.includes(t))) {
    if (secondaryTypes.includes('compilation') && isExactMatch) {
       // Do not penalize exact match compilations
    } else {
       score -= 800;
    }
  }

  // Length penalty
  if (rgTitle.length > cleanedAlbum.length + 12) score -= 500;
  
  // Artist bonuses
  if (normRg === normInput) score += 500; 
  else if (normRg.includes(normInput) || normInput.includes(normRg)) score += 200;

  return score;
}

async function getMusicBrainzData(artist, album) {
  const cleanedAlbum = cleanAlbumName(album);
  const cleanedArtist = cleanArtistName(artist);
  const cacheKey = `${normalizeForComparison(cleanedArtist)}::${normalizeForComparison(cleanedAlbum)}`;
  
  if (mbGlobalCache.has(cacheKey)) return mbGlobalCache.get(cacheKey);
  
  // REDUCED CACHE FAILURE TIME: 24h -> 1h (easier for debugging/retries)
  if (mbFailureCache.has(cacheKey)) {
    const failTime = mbFailureCache.get(cacheKey);
    if (Date.now() - failTime < 60 * 60 * 1000) { 
      return { 
        canonical_name: toTitleCase(cleanedAlbum), 
        canonical_artist: toTitleCase(cleanedArtist), 
        release_year: null, 
        musicbrainz_id: null, 
        type: 'Unknown' 
      };
    }
  }

  try {
    const queries = buildSearchQueries(cleanedArtist, cleanedAlbum);
    
    let allCandidates = [];
    for (const queryString of queries) {
      const query = encodeURIComponent(queryString);
      // INCREASED LIMIT 15 -> 25
      const mbUrl = `https://musicbrainz.org/ws/2/release-group/?query=${query}&fmt=json&limit=25`; 
      
      await new Promise(resolve => setTimeout(resolve, 1100)); 
      
      const response = await fetch(mbUrl, {
        headers: { 'User-Agent': `LastFmTopAlbums/1.0.0 ( ${process.env.YOUR_EMAIL || 'contact@sortedsongs.com'} )` }
      });
      
      if (!response.ok) continue;
      
      const data = await response.json();
      if (data['release-groups'] && data['release-groups'].length > 0) {
        allCandidates.push(...data['release-groups']);
        // If we found a perfect score, we can stop querying
        if (data['release-groups'][0].score === 100) break;
        if (allCandidates.length >= 40) break;
      }
    }
    
    if (allCandidates.length === 0) throw new Error("No candidates found");
    
    const uniqueCandidates = Array.from(new Map(allCandidates.map(rg => [rg.id, rg])).values());

    const candidates = uniqueCandidates
      .filter(rg => {
        // [Keep your existing filter logic here, it is fine with the updated scoring]
        // Just make sure to use the new calculateMatchScore function
        
        // ... (copy your existing filter block or paste the full function if needed)
        // Ensure you check rg['first-release-date'] exists
        if (!rg['first-release-date']) return false;

        const allArtists = (rg['artist-credit'] || []).map(a => normalizeForComparison(a.name || ""));
        const rgArtistMain = rg['artist-credit']?.[0]?.name || "";
        const normInput = normalizeForComparison(artist);
        const normRgMain = normalizeForComparison(rgArtistMain);
        
        const titleSim = stringSimilarity(cleanedAlbum, rg.title);
        const artistSim = stringSimilarity(artist, rgArtistMain);

        const artistMatch = validateArtistMatch(normInput, normRgMain, allArtists, artistSim, cleanedAlbum.toLowerCase(), titleSim);
        if (!artistMatch) return false;

        const minTitleSim = (artistSim > 0.95) ? 0.5 : 0.65;
        if (titleSim < minTitleSim) return false;

        return true;
      })
      .map(rg => {
        const year = parseInt(rg['first-release-date']?.split('-')[0] || "0", 10);
        const primaryType = rg['primary-type']?.toLowerCase() || null;
        const secondaryTypes = (rg['secondary-types'] || []).map(t => t.toLowerCase());
        const rgArtist = rg['artist-credit']?.[0]?.name || "";

        const normRg = normalizeForComparison(rgArtist);
        const normInput = normalizeForComparison(artist);
        const normTitleMB = normalizeForComparison(rg.title);
        const normTitleSearch = normalizeForComparison(cleanedAlbum);

        const artistSimilarity = stringSimilarity(artist, rgArtist);
        const titleSimilarity = stringSimilarity(cleanedAlbum, rg.title);
        
        const score = calculateMatchScore(
          artistSimilarity, titleSimilarity, cleanedAlbum, rg.title,
          normInput, normRg, normTitleMB, normTitleSearch,
          cleanedAlbum.toLowerCase(), primaryType, secondaryTypes
        );

        return { ...rg, score, year, rgArtist };
      })
      .sort((a, b) => {
      // 1. Primary sort: Score (Descending)
      if (b.score !== a.score) {
        return b.score - a.score;
      }
      // 2. Secondary sort: Year (Ascending/Oldest first)
      return (a.year || 9999) - (b.year || 9999);
    });

    if (candidates.length > 0 && candidates[0].score > 200) {
      const best = candidates[0];
      const result = {
        musicbrainz_id: best.id,
        release_year: best.year,
        type: best['primary-type'] || 'Album',
        canonical_name: toTitleCase(best.title),
        canonical_artist: toTitleCase(best.rgArtist)
      };
      mbGlobalCache.set(cacheKey, result);
      return result;
    }
    
    throw new Error(`No high-quality match (best score: ${candidates[0]?.score || 0})`);

  } catch (err) {
    console.error(`  ❌ MB Fail: "${cleanedAlbum}" by "${cleanedArtist}" - ${err.message}`);
    mbFailureCache.set(cacheKey, Date.now());
    return { 
      canonical_name: toTitleCase(cleanedAlbum), 
      canonical_artist: toTitleCase(cleanedArtist), 
      musicbrainz_id: null, 
      release_year: null, 
      type: 'Unknown' 
    };
  }
}

function formatQueryLog(username, params) {
  const { year, decade, yearStart, yearEnd, limit, artist } = params;
  let filterDesc = 'all time';
  
  if (year) filterDesc = `year ${year}`;
  else if (decade) filterDesc = `${decade}s decade`;
  else if (yearStart && yearEnd) filterDesc = `${yearStart}-${yearEnd} range`;
  
  if (artist) filterDesc += ` (artist: ${artist})`;
  
  return `📊 ${username} → ${filterDesc} (limit: ${limit})`;
}

async function performBackgroundUpdate(username, full, limit) {
  const processStart = Date.now();
  console.log(`🚀 [${new Date().toISOString()}] BACKGROUND: Starting update for ${username} (Limit: ${limit})`);

  try {
    await dbRun(
      `INSERT INTO users (username) VALUES ($1) ON CONFLICT (username) DO NOTHING`,
      [username]
    );

    const perPage = 50; 
    const pages = Math.ceil(limit / perPage);
    let totalUpdated = 0;

    for (let page = 1; page <= pages; page++) {
      console.log(`\n📄 [${new Date().toISOString()}] BACKGROUND: Processing Page ${page}/${pages} for ${username}`);
      let pageData;
    try {
      pageData = await callLastFmAPI('user.gettopalbums', {
        user: username,
        limit: perPage,
        page: page
      });
    } catch (err) {
      console.error(`  ❌ Failed to fetch page ${page}:`, err.message);
      break;
    }

      if (!pageData || !pageData.topalbums || !pageData.topalbums.album) {
        console.log("  ⚠️ No more albums found or failed to fetch page."); 
        break; 
      }

      const albums = Array.isArray(pageData.topalbums.album) ? pageData.topalbums.album : [pageData.topalbums.album];
       
      for (const a of albums) {
        try {
          const mbData = await getMusicBrainzData(a.artist.name, a.name);
          
          // --- START OF REPLACEMENT BLOCK ---
          // Upsert album by canonical name
          let albumId;
          if (dbType === 'postgres') {
            const result = await db.query(`
              INSERT INTO albums_global (
                canonical_album, canonical_artist, album_name, artist_name,
                musicbrainz_id, release_year, image_url, lastfm_url, album_type, updated_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
              ON CONFLICT (canonical_album, canonical_artist) 
              DO UPDATE SET 
                image_url = EXCLUDED.image_url,
                lastfm_url = EXCLUDED.lastfm_url,
                updated_at = CURRENT_TIMESTAMP,
                album_name = CASE WHEN albums_global.is_manual = FALSE THEN EXCLUDED.album_name ELSE albums_global.album_name END,
                artist_name = CASE WHEN albums_global.is_manual = FALSE THEN EXCLUDED.artist_name ELSE albums_global.artist_name END,
                musicbrainz_id = CASE WHEN albums_global.is_manual = FALSE THEN COALESCE(EXCLUDED.musicbrainz_id, albums_global.musicbrainz_id) ELSE albums_global.musicbrainz_id END,
                release_year = CASE WHEN albums_global.is_manual = FALSE THEN COALESCE(EXCLUDED.release_year, albums_global.release_year) ELSE albums_global.release_year END,
                album_type = CASE WHEN albums_global.is_manual = FALSE THEN COALESCE(EXCLUDED.album_type, albums_global.album_type) ELSE albums_global.album_type END
              RETURNING id
            `, [
              mbData.canonical_name,
              mbData.canonical_artist,
              a.name,
              a.artist.name,
              mbData.musicbrainz_id,
              mbData.release_year,
              a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
              a.url,
              mbData.type
            ]);
            albumId = result.rows[0].id;
          } else {
            const existing = await dbGet(
              'SELECT id, is_manual FROM albums_global WHERE canonical_album = ? AND canonical_artist = ?',
              [mbData.canonical_name, mbData.canonical_artist]
            );

            if (existing) {
              if (!existing.is_manual) {
                await dbRun(`
                  UPDATE albums_global SET
                    album_name = ?, artist_name = ?,
                    musicbrainz_id = COALESCE(?, musicbrainz_id),
                    release_year = COALESCE(?, release_year),
                    image_url = ?, lastfm_url = ?,
                    album_type = COALESCE(?, album_type),
                    updated_at = CURRENT_TIMESTAMP
                  WHERE id = ?
                `, [
                  a.name, a.artist.name, mbData.musicbrainz_id, mbData.release_year,
                  a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
                  a.url, mbData.type, existing.id
                ]);
              } else {
                await dbRun(`
                  UPDATE albums_global SET
                    image_url = ?, lastfm_url = ?, updated_at = CURRENT_TIMESTAMP
                  WHERE id = ?
                `, [
                  a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
                  a.url, existing.id
                ]);
              }
              albumId = existing.id;
            } else {
              const info = db.prepare(`
                INSERT INTO albums_global (
                  canonical_album, canonical_artist, album_name, artist_name,
                  musicbrainz_id, release_year, image_url, lastfm_url, album_type, updated_at, is_manual
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 0)
              `).run(
                mbData.canonical_name, mbData.canonical_artist, a.name, a.artist.name,
                mbData.musicbrainz_id, mbData.release_year,
                a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
                a.url, mbData.type
              );
              albumId = info.lastInsertRowid;
            }
          }
          // --- END OF REPLACEMENT BLOCK ---

          // Upsert user album data (Corrected: No duplicates)
          if (dbType === 'postgres') {
            await db.query(`
              INSERT INTO user_albums (username, album_id, playcount, updated_at)
              VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
              ON CONFLICT(username, album_id) 
              DO UPDATE SET 
                playcount = EXCLUDED.playcount, 
                updated_at = CURRENT_TIMESTAMP
            `, [username, albumId, parseInt(a.playcount)]);
          } else {
            await dbRun(`
              INSERT INTO user_albums (username, album_id, playcount, updated_at)
              VALUES (?, ?, ?, CURRENT_TIMESTAMP)
              ON CONFLICT(username, album_id) 
              DO UPDATE SET 
                playcount = excluded.playcount, 
                updated_at = CURRENT_TIMESTAMP
            `, [username, albumId, parseInt(a.playcount)]);
          }
          
          totalUpdated++;
          if (totalUpdated % 10 === 0) console.log(`  ... processed ${totalUpdated} albums`);

        } catch (innerErr) {
          console.error(`  ⚠️ Skipped album: ${a.name} - ${innerErr.message}`);
        }
      }

      await new Promise(r => setTimeout(r, 1000));
    }

    const updateQuery = `
      UPDATE users 
      SET ${full ? 'last_update_full' : 'last_update_recent'} = CURRENT_TIMESTAMP,
          total_albums = (SELECT COUNT(*) FROM user_albums WHERE username = $1)
      WHERE username = $1
    `;
    await dbRun(updateQuery, [username]);
    
    const duration = ((Date.now() - processStart) / 1000).toFixed(1);
    console.log(`✅ [${new Date().toISOString()}] BACKGROUND: Finished ${username}. Updated ${totalUpdated} albums in ${duration}s`);

  } catch (err) {
    console.error(`❌ [${new Date().toISOString()}] BACKGROUND ERROR for ${username}:`, err);
  }
}

async function performProgressiveScan(jobId) {
  const job = scanJobs.get(jobId);
  if (!job) return;
  
  const { username, startRange, endRange, filters, targetLimit, currentCount } = job;
  
  console.log(`\n🔍 Progressive Scan Started`);
  console.log(`   User: ${username}`);
  console.log(`   Range: ${startRange}-${endRange}`);
  console.log(`   Target: ${targetLimit} albums`);
  console.log(`   Job ID: ${jobId}`);
  
  try {
    // Step 1: Fetch all albums from Last.fm (FAST - ~1 second)
    console.log(`\n📡 Step 1: Fetching from Last.fm...`);
    const perPage = 500;
    const startPage = Math.ceil(startRange / perPage);
    const endPage = Math.ceil(endRange / perPage);
    
    let allLastFmAlbums = [];
    
    for (let page = startPage; page <= endPage; page++) {
      if (job.shouldStop) {
        console.log(`  ⏸️  Scan stopped during Last.fm fetch`);
        updateJobProgress(jobId, { status: 'stopped' });
        return;
      }
      
      let pageData;
    try {
      pageData = await callLastFmAPI('user.gettopalbums', {
        user: username,
        limit: perPage,
        page: page
      });
    } catch (err) {
      updateJobProgress(jobId, { 
        status: 'error',
        error: `Failed to fetch page ${page}: ${err.message}`
      });
      return;
    }
      
      if (!pageData.topalbums || !pageData.topalbums.album) break;
      
      const albums = Array.isArray(pageData.topalbums.album) 
        ? pageData.topalbums.album 
        : [pageData.topalbums.album];
      
      allLastFmAlbums.push(...albums);
      
      if (page < endPage) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    
    // Filter to the correct range
    const albumsToProcess = allLastFmAlbums.slice(
      Math.max(0, startRange - 1),
      Math.min(allLastFmAlbums.length, endRange)
    );
    
    console.log(`✅ Last.fm fetch complete: ${albumsToProcess.length} albums to process`);
    
    // Step 2: Process each album with MusicBrainz (SLOW - 1.1s each)
    console.log(`\n🎵 Step 2: Processing albums with MusicBrainz...`);
    
    let processedCount = 0;
    const totalToProcess = albumsToProcess.length;
    
    for (const a of albumsToProcess) {
      if (job.shouldStop) {
        console.log(`  ⏸️  Scan stopped at ${processedCount}/${totalToProcess}`);
        updateJobProgress(jobId, { status: 'stopped' });
        return;
      }
      
      // ========================================
      // CRITICAL: Do the SLOW MusicBrainz lookup
      // ========================================
      const mbData = await getMusicBrainzData(a.artist.name, a.name);
      
      // ========================================
      // CRITICAL: Update progress AFTER completion
      // ========================================
      processedCount++;
      
      // Send progress update to all connected clients
      updateJobProgress(jobId, {
        progress: { 
          current: processedCount,
          total: totalToProcess, 
          percentage: Math.round((processedCount / totalToProcess) * 100) 
        }
      });
      
      // Check if album matches filters
      let matches = true;
      
      if (filters.year && mbData.release_year !== filters.year) {
        matches = false;
      }
      
      if (filters.decade) {
        const decadeEnd = filters.decade + 9;
        if (!mbData.release_year || mbData.release_year < filters.decade || mbData.release_year > decadeEnd) {
          matches = false;
        }
      }
      
      if (filters.yearStart && filters.yearEnd) {
        if (!mbData.release_year || mbData.release_year < filters.yearStart || mbData.release_year > filters.yearEnd) {
          matches = false;
        }
      }
      
      // If matches, add to results
      if (matches) {
        const newAlbum = {
          name: a.name,
          artist: a.artist.name,
          playcount: parseInt(a.playcount),
          url: a.url,
          image: a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
          release_year: mbData.release_year,
          musicbrainz_id: mbData.musicbrainz_id,
          type: mbData.type
        };
        
        job.foundAlbums.push(newAlbum);
        
        if (processedCount % 10 === 0 || job.foundAlbums.length <= 5) {
          console.log(`  Progress: ${processedCount}/${totalToProcess} processed, ${job.foundAlbums.length} found`);
        }
        
        // Check if we've reached target
        if (currentCount + job.foundAlbums.length >= targetLimit) {
          console.log(`\n🎯 Target reached! ${job.foundAlbums.length} albums found`);
          updateJobProgress(jobId, {
            status: 'complete',
            progress: { current: totalToProcess, total: totalToProcess, percentage: 100 }
          });
          return;
        }
      }
    }
    
    // Scan complete
    console.log(`\n✅ Scan complete`);
    console.log(`   Processed: ${processedCount} albums`);
    console.log(`   Found: ${job.foundAlbums.length} matching albums`);
    
    updateJobProgress(jobId, { 
      status: 'complete',
      progress: { current: totalToProcess, total: totalToProcess, percentage: 100 }
    });
    
  } catch (err) {
    console.error(`\n❌ Scan error:`, err);
    updateJobProgress(jobId, { 
      status: 'error',
      error: err.message
    });
  }
}

app.use(express.static('public'));

app.use((req, res, next) => {
  const allowedOrigins = [
    'https://sortedsongs.com',
    'https://www.sortedsongs.com',
    'http://localhost:3000'
  ];
  
  const origin = req.headers.origin;
  if (allowedOrigins.includes(origin) || !origin) {
    res.header('Access-Control-Allow-Origin', origin || '*');
  }
  
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Credentials', 'true');
  
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  
  next();
});

app.use(express.json());

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 50,
  message: { error: 'Too many requests, please try again later' }
});

app.get('/warmup', async (req, res) => {
  try {
    await dbGet('SELECT 1');
    res.json({ status: 'warmed up', timestamp: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ error: 'warmup failed' });
  }
});

app.get('/health', (req, res) => {
  res.json({
     status: 'ok',
     timestamp: new Date().toISOString(),
     uptime: process.uptime()
  });
});

app.get('/', (req, res) => {
  res.sendFile(__dirname + '/public/index.html');
});

function shouldUseCache(username) {
  return CACHED_USERS.includes(username);
}

app.get('/api/update', (req, res) => {
  const username = req.query.user;
  const full = req.query.full === 'true';
  const limit = full ? parseInt(req.query.limit || 1000) : 200;

  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  if (!shouldUseCache(username)) {
    return res.status(403).json({ error: 'Cache updates only for approved users' });
  }

  res.json({
    success: true,
    message: "Update process started in the background. Check back in a few minutes.",
    username: username,
    status: "processing"
  });

  performBackgroundUpdate(username, full, limit);
});

app.use('/api/top-albums', apiLimiter);

app.get('/api/top-albums', async (req, res) => {
  const requestStart = Date.now();

  const username = req.query.user;
  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  const year = req.query.year ? parseInt(req.query.year) : null;
  const decade = req.query.decade ? parseInt(req.query.decade) : null;
  const yearStart = req.query.yearStart ? parseInt(req.query.yearStart) : null;
  const yearEnd = req.query.yearEnd ? parseInt(req.query.yearEnd) : null;
  const artist = req.query.artist ? req.query.artist.toLowerCase() : null;
  const limit = req.query.limit ? parseInt(req.query.limit) : 50;

  const cacheKey = `req:${username}|y:${year}|d:${decade}|r:${yearStart}-${yearEnd}|a:${artist}|l:${limit}`;
  const cachedResult = getQueryCache(cacheKey);

  if (cachedResult) {
    console.log(`⚡ Serving from short-term cache: ${cacheKey}`);
    return res.json(cachedResult);
  }

  console.log('\n' + '='.repeat(60));
  console.log(formatQueryLog(username, { year, decade, yearStart, yearEnd, limit, artist }));
  console.log('='.repeat(60));

  if (!shouldUseCache(username)) {
    console.log(`🔴 Mode: REAL-TIME`);
    
    const hasFilter = year || decade || yearStart || yearEnd;

    try {
      const maxAlbumsToScan = hasFilter ? 500 : 200;
      const perPage = 500;
      const pagesToFetch = Math.ceil(maxAlbumsToScan / perPage);
      
      console.log(`  📡 Fetching up to ${maxAlbumsToScan} albums...`);
      
      let allAlbums = [];
      
      for (let page = 1; page <= pagesToFetch; page++) {
        let pageSuccess = false;
        let pageData;

        try {
          pageData = await callLastFmAPI('user.gettopalbums', {
            user: username,
            limit: perPage,
            page: page
          });
          pageSuccess = true;
        } catch (err) {
          console.error(`  ❌ Failed to fetch page ${page}:`, err.message);
          if (allAlbums.length === 0) break;
        }

        if (pageSuccess) {
          const data = pageData; // keep your existing variable name
          allAlbums.push(...(Array.isArray(data.topalbums.album) ? data.topalbums.album : [data.topalbums.album]));

          console.log(`    ✓ Page ${page}: ${(Array.isArray(data.topalbums.album) ? data.topalbums.album.length : 1)} albums (total: ${allAlbums.length})`);

          if (!data.topalbums.album || (Array.isArray(data.topalbums.album) && data.topalbums.album.length < perPage)) break;
        }

        
        if (!pageSuccess && allAlbums.length === 0) break;
        if (page < pagesToFetch && pageSuccess) {
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      }

      if (allAlbums.length === 0) {
        console.log(`  ❌ No albums found`);
        console.log('='.repeat(60) + '\n');
        return res.status(404).json({ error: 'User not found or no albums' });
      }

      console.log(`  ✓ Total fetched: ${allAlbums.length} albums`);
      
      const processedAlbums = [];
      
      for (let i = 0; i < allAlbums.length; i++) {
        const a = allAlbums[i];
        const mbData = await getMusicBrainzData(a.artist.name, a.name);
        
        let shouldInclude = true;
        
        if (year && mbData.release_year !== year) shouldInclude = false;
        
        if (decade) {
          const decadeEnd = decade + 9;
          if (!mbData.release_year || mbData.release_year < decade || mbData.release_year > decadeEnd) {
            shouldInclude = false;
          }
        }
        
        if (yearStart && yearEnd) {
          if (!mbData.release_year || mbData.release_year < yearStart || mbData.release_year > yearEnd) {
            shouldInclude = false;
          }
        }
        
        if (shouldInclude) {
          processedAlbums.push({
            name: a.name,  // ✅ CORRECT - original Last.fm name
            artist: a.artist.name,  // ✅ CORRECT - original Last.fm artist
            playcount: parseInt(a.playcount),
            url: a.url,
            image: a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
            release_year: mbData.release_year,
            musicbrainz_id: mbData.musicbrainz_id,
            type: mbData.type
          });
        }
        
        if (processedAlbums.length >= limit) {
          console.log(`  ✓ Found ${limit} matches`);
          break;
        }
        
        if ((i + 1) % 10 === 0) {
          console.log(`    Processed ${i + 1}/${allAlbums.length}, found ${processedAlbums.length}`);
        }
      }

      console.log(`  ✅ Returning ${processedAlbums.length} albums`);
      console.log('='.repeat(60) + '\n');

      const duration = ((Date.now() - requestStart) / 1000).toFixed(2);
      console.log(`  ⏱️  Request completed in ${duration}s`);

      const responseData = {
        user: username,
        mode: 'realtime',
        filters: { year, decade, yearStart, yearEnd },
        count: processedAlbums.length,
        scanned: allAlbums.length,
        albums: processedAlbums
      };

      setQueryCache(cacheKey, responseData);

      return res.json(responseData);

    } catch (err) {
      console.error('❌ Real-time error:', err);
      console.log('='.repeat(60) + '\n');
      res.status(500).json({ error: 'Real-time fetch failed' });
    }
    return;
  }

  console.log(`🟢 Mode: CACHED`);

  try {
    console.log('  🔍 Building query with filters:', { year, decade, yearStart, yearEnd, artist, limit });
    
    let query = `
      SELECT 
        ag.id,
        ag.canonical_album,
        ag.canonical_artist,
        ag.album_name,
        ag.artist_name,
        ag.musicbrainz_id,
        ag.release_year,
        ag.image_url,
        ag.lastfm_url,
        ag.album_type,
        ua.playcount
      FROM user_albums ua
      JOIN albums_global ag ON ua.album_id = ag.id
      WHERE ua.username = $1
    `;
    
    const params = [username];
    let pIdx = 2;
    
    console.log('  🔍 Initial params:', params.length);

    if (year) {
      query += ` AND ag.release_year = $${pIdx} AND ag.release_year IS NOT NULL`;
      params.push(year);
      pIdx++;
    } 
    else if (decade || (yearStart && yearEnd)) {
      const start = decade || yearStart;
      const end = decade ? (decade + 9) : yearEnd;
      
      query += ` AND ag.release_year BETWEEN $${pIdx} AND $${pIdx + 1} AND ag.release_year IS NOT NULL`;
      params.push(start, end);
      pIdx += 2;
    }

    if (artist) {
      query += ` AND (LOWER(ag.artist_name) LIKE $${pIdx} OR LOWER(ag.canonical_artist) LIKE $${pIdx + 1})`;
      params.push(`%${artist}%`, `%${artist}%`);
      pIdx += 2;
    }

    query += ` ORDER BY ua.playcount DESC LIMIT $${pIdx}`;
    params.push(limit);

    const albums = await dbQuery(query, params);
    
    const userEntry = await dbGet('SELECT * FROM users WHERE username = $1', [username]);

    if (!userEntry) {
      console.log(`  ⚠️  User not in cache`);
      console.log('='.repeat(60) + '\n');
      return res.status(404).json({ error: 'User not cached' });
    }

    console.log(`  ✅ Returning ${albums.length} albums from cache`);
    console.log('='.repeat(60) + '\n');

    const duration = ((Date.now() - requestStart) / 1000).toFixed(2);
    console.log(`  ⏱️  Request completed in ${duration}s`);

    const responseData = {
      user: username,
      mode: 'cached',
      filters: { year, decade, yearStart, yearEnd, artist },
      count: albums.length,
      albums: albums.map(a => ({
        name: a.album_name || a.canonical_album,  
        artist: a.artist_name || a.canonical_artist,  
        playcount: a.playcount,
        url: a.lastfm_url,
        image: a.image_url,
        release_year: a.release_year,
        musicbrainz_id: a.musicbrainz_id,
        type: a.album_type
      }))
    };

    setQueryCache(cacheKey, responseData);

    res.json(responseData);
  } catch (err) {
    console.error('❌ Query failed:', err);
    console.log('='.repeat(60) + '\n');
    res.status(500).json({ error: 'Query failed' });
  }
});

app.get('/api/cache/stats', async (req, res) => {
  const username = req.query.user;

  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  if (!shouldUseCache(username)) {
    return res.json({
      username,
      mode: 'realtime',
      message: 'This user uses real-time data (no cache)'
    });
  }

  try {
    const user = await dbGet('SELECT * FROM users WHERE username = $1', [username]);
    const albumCount = await dbGet('SELECT COUNT(*) as count FROM user_albums WHERE username = $1', [username]);

    if (!user) {
      return res.status(404).json({ error: 'User not found in cache' });
    }

    res.json({
      username: user.username,
      mode: 'cached',
      total_albums: albumCount.count,
      last_update_full: user.last_update_full,
      last_update_recent: user.last_update_recent,
      created_at: user.created_at
    });
  } catch (err) {
    console.error('Stats error:', err);
    res.status(500).json({ error: 'Stats query failed' });
  }
});

app.get('/api/albums/all', async (req, res) => {
  const year = req.query.year ? parseInt(req.query.year) : null;
  const limit = req.query.limit ? parseInt(req.query.limit) : 100;

  try {
    let query = 'SELECT * FROM albums_global';
    const params = [];
    let paramIndex = 1;

    if (year) {
      query += ` WHERE release_year = $${paramIndex}`;
      params.push(year);
      paramIndex++;
    }

    query += ` ORDER BY updated_at DESC LIMIT $${paramIndex}`;
    params.push(limit);

    const albums = await dbQuery(query, params);

    res.json({
      count: albums.length,
      albums: albums
    });
  } catch (err) {
    console.error('All albums query error:', err);
    res.status(500).json({ error: 'Query failed' });
  }
});

app.get('/api/proxy-image', async (req, res) => {
  const imageUrl = req.query.url;
  if (!imageUrl) return res.status(400).send('No URL provided');
  try {
    const response = await fetch(imageUrl);
    const buffer = await response.buffer();
    res.set('Content-Type', response.headers.get('content-type'));
    res.set('Cache-Control', 'public, max-age=86400');
    res.send(buffer);
  } catch (err) {
    res.status(500).send('Proxy error');
  }
});

// ==========================================
// 🔍 UNIFIED SCAN START (for initial scans)
// ==========================================

app.post('/api/scan-start', express.json(), async (req, res) => {
  const { username, startRange = 1, endRange = 500, filters = {}, targetLimit } = req.body;
  
  if (!username) {
    return res.status(400).json({ error: 'Missing username' });
  }
  
  for (const [existingJobId, existingJob] of scanJobs.entries()) {
    if (existingJob.username === username && existingJob.status === 'processing') {
      return res.status(409).json({ 
        error: 'Scan already in progress',
        jobId: existingJobId 
      });
    }
  }
  
  const jobId = createScanJob(username, filters, startRange, endRange, targetLimit || 50, 0);
  
  res.json({ 
    success: true, 
    jobId,
    message: 'Scan started'
  });
  
  performProgressiveScan(jobId);
});

// ==========================================
// 🔍 PROGRESSIVE SCAN ENDPOINTS
// ==========================================

app.get('/api/scan-stream/:jobId', (req, res) => {
  const jobId = req.params.jobId;
  const job = scanJobs.get(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  
  job.clients.push(res);
  
  res.write(`data: ${JSON.stringify({
    status: job.status,
    progress: job.progress,
    foundAlbums: job.foundAlbums,
    newAlbumsCount: job.foundAlbums.length,
    timeRemaining: estimateTimeRemaining(job)
  })}\n\n`);
  
  req.on('close', () => {
    job.clients = job.clients.filter(client => client !== res);
  });
});

app.post('/api/scan-more', express.json(), async (req, res) => {
  const { username, startRange, endRange, filters, targetLimit, currentCount } = req.body;
  
  if (!username || !startRange || !endRange) {
    return res.status(400).json({ error: 'Missing required parameters' });
  }
  
  // Fix: Cancel any existing jobs for this user instead of blocking
  for (const [existingJobId, existingJob] of scanJobs.entries()) {
    if (existingJob.username === username && existingJob.status === 'processing') {
      // Mark old job as stopped
      existingJob.shouldStop = true;
      existingJob.status = 'stopped';
      
      // Close existing clients
      existingJob.clients.forEach(client => {
        try { client.end(); } catch (e) {}
      });
      
      // Remove from map
      scanJobs.delete(existingJobId);
      console.log(`⚠️ Cancelled previous job ${existingJobId} for user ${username}`);
    }
  }
  
  const jobId = createScanJob(username, filters, startRange, endRange, targetLimit, currentCount);
  
  res.json({ 
    success: true, 
    jobId,
    message: 'Scan started'
  });
  
  performProgressiveScan(jobId);
});

app.post('/api/scan-stop/:jobId', (req, res) => {
  const jobId = req.params.jobId;
  const job = scanJobs.get(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  job.shouldStop = true;
  job.status = 'stopped';
  
  updateJobProgress(jobId, { status: 'stopped' });
  
  res.json({ 
    success: true,
    foundAlbums: job.foundAlbums,
    message: `Scan stopped. Found ${job.foundAlbums.length} albums.`
  });
});

app.get('/api/scan-status/:jobId', (req, res) => {
  const jobId = req.params.jobId;
  const job = scanJobs.get(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  res.json({
    jobId: job.jobId,
    status: job.status,
    progress: job.progress,
    foundAlbums: job.foundAlbums,
    newAlbumsCount: job.foundAlbums.length,
    timeRemaining: estimateTimeRemaining(job),
    error: job.error
  });
});

app.get('/api/debug/mb-test', async (req, res) => {
  const artist = req.query.artist || 'Ms. Lauryn Hill';
  const album = req.query.album || 'The Miseducation of Lauryn Hill';
  
  // Temporarily enable debug
  const originalConsoleLog = console.log;
  const logs = [];
  console.log = (...args) => {
    logs.push(args.join(' '));
    originalConsoleLog(...args);
  };
  
  const result = await getMusicBrainzData(artist, album);
  
  console.log = originalConsoleLog;
  
  res.json({
    input: { artist, album },
    result,
    logs
  });
});

app.get('/api/admin/merge-duplicates', async (req, res) => {
  if (!req.query.confirm) {
    return res.json({ 
      message: 'This will merge duplicate albums with different cases. Add ?confirm=true to proceed.',
      warning: 'This is a one-time operation'
    });
  }

  try {
    let mergedCount = 0;
    
    // Find all albums
    const allAlbums = await dbQuery('SELECT * FROM albums_global ORDER BY id');
    
    // Group by normalized canonical name
    const groups = new Map();
    for (const album of allAlbums) {
      const key = `${album.canonical_album.toLowerCase()}::${album.canonical_artist.toLowerCase()}`;
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(album);
    }
    
    // Merge duplicates
    for (const [key, albums] of groups) {
      if (albums.length > 1) {
        // Keep the one with musicbrainz_id if available, otherwise the oldest
        const keeper = albums.find(a => a.musicbrainz_id) || albums[0];
        const toMerge = albums.filter(a => a.id !== keeper.id);
        
        console.log(`Merging ${albums.length} versions of "${keeper.canonical_album}"`);
        
        for (const duplicate of toMerge) {
          // FIXED: Update or delete user_albums entries
          if (dbType === 'postgres') {
            // For each user that has the duplicate, update playcount on keeper if needed
            await db.query(`
              INSERT INTO user_albums (username, album_id, playcount, updated_at)
              SELECT username, $1, playcount, updated_at
              FROM user_albums
              WHERE album_id = $2
              ON CONFLICT (username, album_id) 
              DO UPDATE SET 
                playcount = GREATEST(user_albums.playcount, EXCLUDED.playcount),
                updated_at = CURRENT_TIMESTAMP
            `, [keeper.id, duplicate.id]);
            
            // Delete the duplicate entries
            await db.query('DELETE FROM user_albums WHERE album_id = $1', [duplicate.id]);
            
            // Delete the duplicate album
            await db.query('DELETE FROM albums_global WHERE id = $1', [duplicate.id]);
          } else {
            // SQLite version
            await dbRun(`
              INSERT OR REPLACE INTO user_albums (username, album_id, playcount, updated_at)
              SELECT username, ?, MAX(playcount), CURRENT_TIMESTAMP
              FROM user_albums
              WHERE album_id IN (?, ?)
              GROUP BY username
            `, [keeper.id, keeper.id, duplicate.id]);
            
            await dbRun('DELETE FROM albums_global WHERE id = ?', [duplicate.id]);
          }
          
          mergedCount++;
        }
      }
    }
    
    res.json({
      success: true,
      merged: mergedCount,
      message: `Merged ${mergedCount} duplicate albums`
    });
  } catch (err) {
    console.error('Merge error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`\n🎵 Last.fm Top Albums API Server`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`\n📍 Server: http://localhost:${PORT}`);
  console.log(`💾 Database: ${dbType === 'postgres' ? 'PostgreSQL' : 'SQLite'}`);
  console.log(`💾 Cached users: ${CACHED_USERS.length > 0 ? CACHED_USERS.join(', ') : 'none'}`);
  console.log(`🔴 Public users: real-time mode\n`);
});