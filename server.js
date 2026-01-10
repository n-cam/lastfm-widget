require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const rateLimit = require('express-rate-limit');
const app = express();
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;
const AbortController = require('abort-controller');

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

  // 1. Remove specific years followed by "Remaster" (e.g., "2009 Remaster", "1994 Remastered")
  const yearRemasterRegex = /\s*[\(\[]?\d{4}\s+Remaster(?:ed)?[\)\]]?/gi;

  // 2. Comprehensive list of "noise" tags found in brackets or after dashes
  const noiseTerms = [
    'Deluxe', 'Remastered', 'Remaster', 'Edition', 'Anniversary', 
    'Expanded', 'Special', 'Bonus', 'Live', 'Explicit', 'Extended', 
    'Target', 'Walmart', 'Japan', 'Import', 'Clean', 'Dirty', 
    'Digital', 'LP', 'Version', 'Set', 'Box Set', 'Mono', 'Stereo',
    'Reissue', 'Collector\'s', 'Standard', 'Super Deluxe'
  ].join('|');

  const bracketRegex = new RegExp(`\\s*[\\(\\[](?:${noiseTerms})[\\)\\]]`, 'gi');
  const dashRegex = new RegExp(`\\s*-\\s*(?:${noiseTerms}).*`, 'gi');

  let cleaned = name;
  let prev;

  do {
    prev = cleaned;
    
    // Apply all cleaning rules
    cleaned = cleaned
      .replace(yearRemasterRegex, '') // Remove "2009 Remaster"
      .replace(bracketRegex, '')      // Remove "(Deluxe Edition)"
      .replace(dashRegex, '')         // Remove "- Remastered"
      .replace(/\s+\(?(?:Mono|Stereo|Remaster(?:ed)?)\)?$/i, '') // Catch dangling terms
      .trim();
      
  } while (cleaned !== prev); // Loop to catch nested tags like (Live) [Remaster]

  // Final safety: if cleaning leaves us with an empty string (rare), return original
  return cleaned || name;
}

function normalizeForComparison(str) {
  if (!str) return '';
  try {
    return str.toLowerCase()
      // 1. Standardize "Fancy" characters first
      .replace(/[×✕✖]/g, 'x')      // Fixes Chloe × Halle
      .replace(/[‐‑‒–—]/g, '-')    // Fixes alt‐J (standardizes all dashes)
      .replace(/[‘’]/g, "'")       // Standardizes curly apostrophes
      
      // 2. Decompose accents (e.g., "é" becomes "e" + accent mark)
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      
      // 3. Keep ONLY letters and numbers
      // We use \s to keep spaces temporarily to avoid "alt-J" becoming "altj" 
      // which can mess up word-split logic later.
      .replace(/[^\p{L}\p{N}\s]/gu, '') 
      .replace(/\s+/g, ' ')        // Collapse multiple spaces
      .trim();
  } catch (e) {
    // Fallback for older environments
    return str.toLowerCase()
      .replace(/[×✕✖]/g, 'x')
      .replace(/[^a-z0-9]/gi, '')
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

async function getMusicBrainzData(artist, album) {
  // 1. Clean inputs to strip "Remastered", "Deluxe", etc.
  const cleanedAlbum = cleanAlbumName(album);
  const cleanedArtist = cleanArtistName(artist);
  const cacheKey = `${normalizeForComparison(cleanedArtist)}::${normalizeForComparison(cleanedAlbum)}`;
  
  // 2. Check Caches
  if (mbGlobalCache.has(cacheKey)) return mbGlobalCache.get(cacheKey);
  if (mbFailureCache.has(cacheKey)) {
    const failTime = mbFailureCache.get(cacheKey);
    if (Date.now() - failTime < 24 * 60 * 60 * 1000) { // 24hr cooldown
        return { canonical_name: cleanedAlbum, canonical_artist: cleanedArtist, release_year: null, musicbrainz_id: null, type: 'Unknown' };
    }
  }

  try {
    // 3. Search Strategy: release-group (Abstract Album) > release (Specific CD)
    const queries = [
        `releasegroup:"${cleanedAlbum}" AND artist:"${cleanedArtist}"`, 
        `releasegroup:"${cleanedAlbum}" AND artistname:"${cleanedArtist}"`,
        `${cleanedAlbum} ${cleanedArtist}`
    ];
    
    let allCandidates = [];
    
    // 4. Execute Searches
    for (const queryString of queries) {
      const query = encodeURIComponent(queryString);
      const mbUrl = `https://musicbrainz.org/ws/2/release-group/?query=${query}&fmt=json&limit=15`;
      
      await new Promise(resolve => setTimeout(resolve, 1100)); // Safety delay
      
      const response = await fetch(mbUrl, {
        headers: { 'User-Agent': `LastFmTopAlbums/1.0.0 ( ${process.env.YOUR_EMAIL || 'contact@example.com'} )` }
      });
      
      if (!response.ok) continue;
      
      const data = await response.json();
      if (data['release-groups'] && data['release-groups'].length > 0) {
        allCandidates.push(...data['release-groups']);
        // If we found a strong match in the first strict query, stop early to save time
        if (data['release-groups'][0].score === 100) break;
        if (allCandidates.length >= 25) break;
      }
    }
    
    if (allCandidates.length === 0) throw new Error("No candidates found");

    // 5. Deduplicate by ID
    const uniqueCandidates = Array.from(new Map(allCandidates.map(rg => [rg.id, rg])).values());

    const candidates = uniqueCandidates
        .filter(rg => {
    if (!rg['first-release-date']) return false;

          const rgArtist = rg['artist-credit']?.[0]?.name || "";
          const normRg = normalizeForComparison(rgArtist);
          const normInput = normalizeForComparison(artist);
          const rgTitleLower = rg.title.toLowerCase();
          const searchTitleLower = cleanedAlbum.toLowerCase();

          // --- SHIELD 1: THE IRONCLAD ARTIST GATEKEEPER ---
          // This combines the "Direct Match" and "First Word" logic into one check.
          const isDirectMatch = normRg.includes(normInput) || normInput.includes(normRg);
          
          const firstWordInput = normInput.split(' ')[0];
          const firstWordRg = normRg.split(' ')[0];
          const firstWordMatch = firstWordInput === firstWordRg && firstWordInput.length > 2;

          // If it's not a direct match (Lauryn Hill/Silk Sonic) AND doesn't share a 
          // unique first word, it's a hallucination (Maxh. vs Emdasche). REJECT.
          if (!isDirectMatch && !firstWordMatch) return false;

          // --- SHIELD 2: SHORT TITLE PROTECTION ---
          // If title is <= 3 chars (x, v, 21), require an EXACT title match.
          if (cleanedAlbum.length <= 3 && rgTitleLower !== searchTitleLower) {
              return false;
          }

          // --- SHIELD 3: LIVE REJECTION ---
          const secondaryTypes = (rg['secondary-types'] || []).map(t => t.toLowerCase());
          const isExplicitlyLive = secondaryTypes.includes('live');
          const userWantsLive = searchTitleLower.includes('live') || searchTitleLower.includes('session');
          
          if (isExplicitlyLive && !userWantsLive && rgTitleLower.length > searchTitleLower.length + 5) {
              return false;
          }

          // --- SHIELD 4: REMIX REJECTION ---
          const isRemix = secondaryTypes.includes('remix') || rgTitleLower.includes('remix');
          if (isRemix && !searchTitleLower.includes('remix')) return false;

          // --- SHIELD 5: MINIMUM SIMILARITY ---
          return stringSimilarity(cleanedAlbum, rg.title) > 0.3;
      })

        .map(rg => {
          const year = parseInt(rg['first-release-date'].split('-')[0], 10);
          const primaryType = rg['primary-type']?.toLowerCase() || null;
          const rgArtist = rg['artist-credit']?.[0]?.name || "";

          const artistSimilarity = stringSimilarity(artist, rgArtist);
          const titleSimilarity = stringSimilarity(cleanedAlbum, rg.title);
          
          // 🧠 SCORING ENGINE
          let score = 0;
          
          // A. Heavy weighting on Artist (Prevents "Deacon Blue" vs "Arctic Monkeys")
          score += artistSimilarity * 400; 
          score += titleSimilarity * 200;

          // B. Bootleg Detector (Prevents "Tyler SLOW+REVERB")
          // If the MB title is significantly longer (> 8 chars) than our clean title, penalty.
          if (rg.title.length > cleanedAlbum.length + 5) {
             score -= 400;
          }

          // C. Strict Artist Penalty (Prevents "The Rip-Off Artist")
          // If not a substring match and sim < 0.8, penalty.
          const normRg = normalizeForComparison(rgArtist);
          const normInput = normalizeForComparison(artist);
          const isInclusiveMatch = normRg.includes(normInput) || normInput.includes(normRg);
          if (!isInclusiveMatch && artistSimilarity < 0.8) {
             score -= 200;
          }

          // D. Boosters
          if (primaryType === 'album') score += 150;
          if (primaryType === 'ep') score += 50;
          
          // Exact Match Bonuses
          if (normalizeForComparison(rg.title) === normalizeForComparison(cleanedAlbum)) score += 150;
          if (isInclusiveMatch) score += 150;

          // E. Bad Types Penalty (Prevents Demos/Live)
          const badSecondaryTypes = ['compilation', 'live', 'soundtrack', 'demo', 'remix', 'dj-mix'];
          if (rg['secondary-types']?.some(t => badSecondaryTypes.includes(t.toLowerCase()))) {
            score -= 500;
          }

          return { ...rg, score, year, rgArtist };
        })
        .sort((a, b) => {
          const threshold = 600;

          // If one is significantly better than the other (e.g. 800 vs 610), 
          // take the better match regardless of year.
          if (Math.abs(a.score - b.score) > 150) {
            return b.score - a.score;
          }

          // If they are both high-quality matches (e.g. 800 vs 780), pick the oldest.
          if (a.score > threshold && b.score > threshold) {
            return a.year - b.year;
          }
          
          return b.score - a.score;
        });

      if (candidates.length > 0) {
        const best = candidates[0];
        const result = {
          musicbrainz_id: best.id,
          release_year: best.year,
          type: best['primary-type'] || 'Album',
          canonical_name: best.title, // Use MB title
          canonical_artist: best.rgArtist // Use MB artist
        };
        mbGlobalCache.set(cacheKey, result);
        mbFailureCache.delete(cacheKey);
        return result;
      }
      
      // No match found
      throw new Error("No suitable candidates after filtering");

  } catch (err) {
    console.error(`  ❌ MB Fail: ${cleanedAlbum} - ${err.message}`);
    mbFailureCache.set(cacheKey, Date.now());
    return { canonical_name: cleanedAlbum, canonical_artist: cleanedArtist, musicbrainz_id: null, release_year: null, type: 'Unknown' };
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
      const lastfmUrl = `https://ws.audioscrobbler.com/2.0/?method=user.gettopalbums&user=${username}&api_key=${process.env.LASTFM_API_KEY}&format=json&limit=${perPage}&page=${page}`;
      
      let pageData;
      let retries = 0;
      const maxRetries = 3;

      while (retries < maxRetries) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        try {
          const response = await fetch(lastfmUrl, { signal: controller.signal });

          clearTimeout(timeout);

          if (!response.ok) {
            if ((response.status === 429 || response.status === 503) && retries < maxRetries - 1) {
              const waitTime = Math.pow(2, retries) * 1000;
              console.log(`  ⏳ Retry #${retries + 1} in ${waitTime/1000}s due to ${response.status}`);
              await new Promise(r => setTimeout(r, waitTime));
              retries++;
              continue;
            }
            throw new Error(`Last.fm API error: ${response.status}`);
          }

          pageData = await response.json();
          break;

        } catch (err) {
          clearTimeout(timeout);
          if (err.name === 'AbortError') {
            console.warn(`  ⚠️ Fetch timeout on page ${page}, retry #${retries + 1}`);
          } else {
            console.error(`  ⚠️ Fetch error on page ${page}:`, err.message);
          }

          if (retries < maxRetries - 1) {
            const waitTime = Math.pow(2, retries) * 1000;
            await new Promise(r => setTimeout(r, waitTime));
            retries++;
          } else {
            console.error(`  ❌ Failed to fetch page ${page} after ${maxRetries} attempts, skipping.`);
            pageData = null;
            break;
          }
        }
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
        const lastfmUrl = `https://ws.audioscrobbler.com/2.0/?method=user.gettopalbums&user=${username}&api_key=${process.env.LASTFM_API_KEY}&format=json&limit=${perPage}&page=${page}`;
        
        let retryCount = 0;
        const maxRetries = 3;
        let pageSuccess = false;
        
        while (retryCount < maxRetries && !pageSuccess) {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 10000);
          try {
            const response = await fetch(lastfmUrl, { signal: controller.signal });

            clearTimeout(timeout);

            if (!response.ok) {
              if ((response.status === 429 || response.status === 503) && retryCount < maxRetries - 1) {
                const waitTime = Math.pow(2, retryCount) * 1000;
                console.log(`  ⏳ Retrying in ${waitTime/1000}s due to ${response.status}`);
                await new Promise(r => setTimeout(r, waitTime));
                retryCount++;
                continue;
              }
              throw new Error(`Last.fm API error: ${response.status}`);
            }

            const data = await response.json();
            allAlbums.push(...(Array.isArray(data.topalbums.album) ? data.topalbums.album : [data.topalbums.album]));
            
            console.log(`    ✓ Page ${page}: ${(Array.isArray(data.topalbums.album) ? data.topalbums.album.length : 1)} albums (total: ${allAlbums.length})`);

            pageSuccess = true;

            if (!data.topalbums.album || (Array.isArray(data.topalbums.album) && data.topalbums.album.length < perPage)) break;

          } catch (err) {
            clearTimeout(timeout);
            if (err.name === 'AbortError') {
              console.warn(`  ⚠️ Fetch timeout on page ${page}, retry #${retryCount + 1}`);
            } else {
              console.error(`  ⚠️ Fetch error on page ${page}:`, err.message);
            }

            if (retryCount < maxRetries - 1) {
              const waitTime = Math.pow(2, retryCount) * 1000;
              await new Promise(r => setTimeout(r, waitTime));
              retryCount++;
            } else {
              console.error(`  ❌ Failed to fetch page ${page} after ${maxRetries} attempts, skipping.`);
              pageSuccess = true;
            }
          }
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
            name: mbData.canonical_name || a.name,
            artist: mbData.canonical_artist || a.artist.name,
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
        name: a.canonical_album || a.album_name,
        artist: a.canonical_artist || a.artist_name,
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

app.listen(PORT, () => {
  console.log(`\n🎵 Last.fm Top Albums API Server`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`\n📍 Server: http://localhost:${PORT}`);
  console.log(`💾 Database: ${dbType === 'postgres' ? 'PostgreSQL' : 'SQLite'}`);
  console.log(`💾 Cached users: ${CACHED_USERS.length > 0 ? CACHED_USERS.join(', ') : 'none'}`);
  console.log(`🔴 Public users: real-time mode\n`);
});