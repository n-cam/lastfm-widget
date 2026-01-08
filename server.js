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

// Database setup
let db;
let dbType;

if (process.env.DATABASE_URL) {
  console.log('🐘 Using PostgreSQL database');
  const { Pool } = require('pg');
  db = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    max: 5,
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

// Initialize optimized database schema
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
        musicbrainz_id TEXT PRIMARY KEY,
        album_name TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        canonical_album TEXT,
        canonical_artist TEXT,
        release_year INTEGER,
        image_url TEXT,
        lastfm_url TEXT,
        album_type TEXT,
        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS user_albums (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        musicbrainz_id TEXT NOT NULL,
        playcount INTEGER DEFAULT 0,
        last_scrobble TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(username, musicbrainz_id)
      );

      CREATE INDEX IF NOT EXISTS idx_user_albums_username ON user_albums(username);
      CREATE INDEX IF NOT EXISTS idx_user_albums_playcount ON user_albums(playcount);
      CREATE INDEX IF NOT EXISTS idx_albums_global_year ON albums_global(release_year);
      CREATE INDEX IF NOT EXISTS idx_albums_global_canonical ON albums_global(canonical_album, canonical_artist);
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
        musicbrainz_id TEXT PRIMARY KEY,
        album_name TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        canonical_album TEXT,
        canonical_artist TEXT,
        release_year INTEGER,
        image_url TEXT,
        lastfm_url TEXT,
        album_type TEXT,
        first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS user_albums (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        musicbrainz_id TEXT NOT NULL,
        playcount INTEGER DEFAULT 0,
        last_scrobble TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(username, musicbrainz_id)
      );

      CREATE INDEX IF NOT EXISTS idx_user_albums_username ON user_albums(username);
      CREATE INDEX IF NOT EXISTS idx_user_albums_playcount ON user_albums(playcount);
      CREATE INDEX IF NOT EXISTS idx_albums_global_year ON albums_global(release_year);
      CREATE INDEX IF NOT EXISTS idx_albums_global_canonical ON albums_global(canonical_album, canonical_artist);
    `);
  }
}

async function dbQuery(query, params = []) {
  if (dbType === 'postgres') {
    const result = await db.query(query, params);
    return result.rows;
  }
  return db.prepare(query).all(...params);
}

async function dbGet(query, params = []) {
  if (dbType === 'postgres') {
    const result = await db.query(query, params);
    return result.rows[0] || null;
  }
  return db.prepare(query).get(...params);
}

async function dbRun(query, params = []) {
  if (dbType === 'postgres') {
    await db.query(query, params);
  } else {
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

const mbCache = new Map();
const mbFailureCache = new Map(); // ⭐ NEW: Track failed lookups

const cron = require('node-cron');

cron.schedule('0 3 * * 0', async () => {
  console.log('🔄 Starting weekly auto-update...');
  
  for (const user of CACHED_USERS) {
    try {
      console.log(`  Updating ${user}...`);
      await performBackgroundUpdate(user, false, 500);
      await new Promise(r => setTimeout(r, 60000));
    } catch (err) {
      console.error(`  ❌ Failed to update ${user}:`, err);
    }
  }
  
  console.log('✅ Weekly auto-update complete');
});

console.log('⏰ Cron job scheduled: Weekly updates every Sunday at 3am');

function cleanAlbumName(name) {
  return name
    .replace(/\s*\(.*?(Deluxe|Remaster|Edition|Anniversary|Expanded|Special|Bonus|Live|Explicit|Extended|Target|Walmart|Japan|Import|Clean|Dirty).*?\)/gi, '')
    .replace(/\s*\[.*?(Deluxe|Remaster|Edition|Anniversary|Expanded|Special|Bonus|Live|Explicit|Extended|Target|Walmart|Japan|Import|Clean|Dirty).*?\]/gi, '')
    .replace(/\s*-\s*(Deluxe|Remaster|Edition|Anniversary|Expanded|Special|Bonus).*/gi, '')
    .trim();
}

function cleanArtistName(name) {
  return name
    .replace(/^(Ms\.?|Mr\.?|Mrs\.?|Dr\.?)\s+/i, '')
    .replace(/^(The|A|An)\s+/i, '')
    .trim();
}

function normalizeForComparison(str) {
  try {
    return str.toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^\p{L}\p{N}]/gu, '')
      .trim();
  } catch (e) {
    return str.toLowerCase()
      .replace(/[^a-z0-9\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\uAC00-\uD7AF]/gi, '')
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
  const cacheKey = `${artist}::${album}`;
  
  if (mbCache.has(cacheKey)) {
    return mbCache.get(cacheKey);
  }

  // ⭐ NEW: Check if we recently failed this lookup
  if (mbFailureCache.has(cacheKey)) {
    const failTime = mbFailureCache.get(cacheKey);
    if (Date.now() - failTime < 24 * 60 * 60 * 1000) {
      const cleanedAlbum = cleanAlbumName(album);
      return { 
        musicbrainz_id: `fallback_${cleanedAlbum}_${artist}`, 
        release_year: null, 
        type: null,
        canonical_name: cleanedAlbum,
        canonical_artist: artist
      };
    }
  }

  try {
    const cleanedAlbum = cleanAlbumName(album);
    const cleanedArtist = cleanArtistName(artist);
    
    const queries = [
      `release:"${album}" AND artist:"${artist}"`,
      `release:"${cleanedAlbum}" AND artist:"${cleanedArtist}"`,
      `${cleanedAlbum} ${cleanedArtist}`
    ];
    
    let allCandidates = [];
    
    for (const queryString of queries) {
      const query = encodeURIComponent(queryString);
      const mbUrl = `https://musicbrainz.org/ws/2/release-group/?query=${query}&fmt=json&limit=15`;
      
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      const response = await fetch(mbUrl, {
        headers: {
          'User-Agent': `LastFmTopAlbums/1.0.0 ( ${process.env.YOUR_EMAIL || 'contact@example.com'} )`
        }
      });
      
      const data = await response.json();
      
      if (data['release-groups'] && data['release-groups'].length > 0) {
        allCandidates.push(...data['release-groups']);
        if (data['release-groups'].length >= 3) break;
      }
    }
    
    if (allCandidates.length > 0) {
      const uniqueCandidates = Array.from(
        new Map(allCandidates.map(rg => [rg.id, rg])).values()
      );
      
      const candidates = uniqueCandidates
        .filter(rg => {
          if (!rg['first-release-date']) return false;
          if (!rg['artist-credit'] || !rg['artist-credit'][0]) return false;
          
          const rgArtist = rg['artist-credit'][0].name;
          const artistSimilarity = stringSimilarity(artist, rgArtist);
          
          if (artistSimilarity < 0.35) return false;
          
          const titleSimilarity = stringSimilarity(cleanedAlbum, rg.title);
          if (titleSimilarity < 0.3) return false;
          
          const primaryType = rg['primary-type'];
          if (primaryType === 'Single') return false;
          
          return true;
        })
        .map(rg => {
          const year = parseInt(rg['first-release-date']?.split('-')[0], 10);
          const primaryType = rg['primary-type']?.toLowerCase() || null;
          const rgArtist = rg['artist-credit']?.[0]?.name || "";

          const artistSimilarity = stringSimilarity(artist, rgArtist);
          const titleSimilarity = stringSimilarity(cleanedAlbum, rg.title);

          let score = 0;

          score += artistSimilarity * 250;
          score += titleSimilarity * 200;

          if (primaryType === 'album') score += 150;
          else if (primaryType === 'ep') score += 50;

          const badSecondaryTypes = ['compilation', 'live', 'soundtrack'];
          if (
            rg['secondary-types']?.some(t =>
              badSecondaryTypes.includes(t.toLowerCase())
            )
          ) {
            score -= 80;
          }

          if (
            normalizeForComparison(rg.title) ===
            normalizeForComparison(cleanedAlbum)
          ) {
            score += 100;
          }

          if (
            normalizeForComparison(rgArtist) ===
            normalizeForComparison(artist)
          ) {
            score += 100;
          }

          return {
            ...rg,
            score,
            year,
            artistSimilarity,
            titleSimilarity,
            rgArtist
          };
        })
        .sort((a, b) => {
          if (Math.abs(b.score - a.score) > 10) {
            return b.score - a.score;
          }
          return a.year - b.year;
        });
      
      if (candidates.length > 0) {
        const best = candidates[0];
        const result = {
          musicbrainz_id: best.id,
          release_year: best.year,
          type: best['primary-type'],
          canonical_name: best.title,
          canonical_artist: best.rgArtist
        };
        
        mbCache.set(cacheKey, result);
        mbFailureCache.delete(cacheKey); // ⭐ Clear failure cache on success
        return result;
      }
    }
    
    // ⭐ NEW: Mark as failed
    mbFailureCache.set(cacheKey, Date.now());
    
    const noMatch = { 
      musicbrainz_id: `fallback_${cleanAlbumName(album)}_${artist}`, 
      release_year: null, 
      type: null,
      canonical_name: cleanAlbumName(album),
      canonical_artist: artist
    };
    mbCache.set(cacheKey, noMatch);
    return noMatch;
    
  } catch (err) {
    console.error(`MusicBrainz lookup failed for ${artist} - ${album}:`, err.message);
    
    // ⭐ NEW: Mark as failed
    mbFailureCache.set(cacheKey, Date.now());
    
    const cleanedAlbum = cleanAlbumName(album);
    return { 
      musicbrainz_id: `fallback_${cleanedAlbum}_${artist}`, 
      release_year: null, 
      type: null,
      canonical_name: cleanedAlbum,
      canonical_artist: artist
    };
  }
}

// ⭐ NEW: Helper function to find existing albums
async function findExistingAlbum(canonicalAlbum, canonicalArtist) {
  const existing = await dbGet(`
    SELECT musicbrainz_id, release_year
    FROM albums_global
    WHERE canonical_album = $1 
      AND canonical_artist = $2
    ORDER BY 
      CASE 
        WHEN release_year IS NOT NULL THEN 0 
        ELSE 1 
      END,
      musicbrainz_id
    LIMIT 1
  `, [canonicalAlbum, canonicalArtist]);
  
  return existing;
}

// ⭐ NEW: Merge fallback entries into canonical entries
async function mergeIntoCanonical(fallbackId, canonicalId) {
  console.log(`  🔄 Merging ${fallbackId} → ${canonicalId}`);
  
  if (dbType === 'postgres') {
    await db.query(`
      INSERT INTO user_albums (username, musicbrainz_id, playcount, updated_at)
      SELECT username, $2, playcount, CURRENT_TIMESTAMP
      FROM user_albums
      WHERE musicbrainz_id = $1
      ON CONFLICT (username, musicbrainz_id)
      DO UPDATE SET 
        playcount = user_albums.playcount + EXCLUDED.playcount,
        updated_at = CURRENT_TIMESTAMP
    `, [fallbackId, canonicalId]);
  } else {
    const userEntries = await dbQuery(
      'SELECT username, playcount FROM user_albums WHERE musicbrainz_id = ?',
      [fallbackId]
    );
    
    for (const entry of userEntries) {
      await dbRun(`
        INSERT INTO user_albums (username, musicbrainz_id, playcount, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(username, musicbrainz_id) 
        DO UPDATE SET 
          playcount = playcount + excluded.playcount,
          updated_at = CURRENT_TIMESTAMP
      `, [entry.username, canonicalId, entry.playcount]);
    }
  }
  
  await dbRun('DELETE FROM user_albums WHERE musicbrainz_id = $1', [fallbackId]);
  await dbRun('DELETE FROM albums_global WHERE musicbrainz_id = $1', [fallbackId]);
}

async function fetchAllAlbums(username, totalLimit) {
  const perPage = 1000;
  const pages = Math.ceil(totalLimit / perPage);
  let allAlbums = [];

  console.log(`📥 Fetching ${totalLimit} albums in ${pages} page(s)...`);

  for (let page = 1; page <= pages; page++) {
    const limit = Math.min(perPage, totalLimit - allAlbums.length);
    const lastfmUrl = `https://ws.audioscrobbler.com/2.0/?method=user.gettopalbums&user=${username}&api_key=${process.env.LASTFM_API_KEY}&format=json&limit=${limit}&page=${page}`;
    
    console.log(`  Page ${page}/${pages}: Fetching ${limit} albums...`);
    
    const response = await fetch(lastfmUrl);
    const data = await response.json();

    if (!data.topalbums || !data.topalbums.album) {
      console.log(`  ⚠️  No more albums found at page ${page}`);
      break;
    }

    const albums = Array.isArray(data.topalbums.album) ? data.topalbums.album : [data.topalbums.album];
    allAlbums.push(...albums);

    console.log(`  ✓ Page ${page}/${pages}: Got ${albums.length} albums (total: ${allAlbums.length})`);

    if (albums.length < limit) {
      console.log(`  ℹ️  Reached end of user's library`);
      break;
    }

    if (page < pages) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }

  return allAlbums;
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

// ⭐ UPDATED: performBackgroundUpdate with duplicate prevention
async function performBackgroundUpdate(username, full, limit) {
  const processStart = Date.now();
  console.log(`🚀 BACKGROUND: Starting update for ${username} (Limit: ${limit})`);

  try {
    await dbRun(
      `INSERT INTO users (username) VALUES ($1) ON CONFLICT (username) DO NOTHING`,
      [username]
    );

    const perPage = 50; 
    const pages = Math.ceil(limit / perPage);
    let totalUpdated = 0;
    let mergedCount = 0;

    for (let page = 1; page <= pages; page++) {
      console.log(`\n📄 BACKGROUND: Processing Page ${page}/${pages} for ${username}`);
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
          
          // ⭐ NEW: Check if this album already exists by canonical name
          const existing = await findExistingAlbum(
            mbData.canonical_name, 
            mbData.canonical_artist
          );
          
          let finalMbId = mbData.musicbrainz_id;
          
          if (existing) {
            if (existing.release_year && !mbData.release_year) {
              // Existing has year, new doesn't → use existing
              finalMbId = existing.musicbrainz_id;
              if (existing.musicbrainz_id !== mbData.musicbrainz_id) {
                await mergeIntoCanonical(mbData.musicbrainz_id, existing.musicbrainz_id);
                mergedCount++;
              }
              
            } else if (!existing.release_year && mbData.release_year) {
              // New has year, existing doesn't → upgrade
              finalMbId = mbData.musicbrainz_id;
              
              if (existing.musicbrainz_id !== mbData.musicbrainz_id) {
                await mergeIntoCanonical(existing.musicbrainz_id, mbData.musicbrainz_id);
                mergedCount++;
              }
              
            } else if (existing.musicbrainz_id !== mbData.musicbrainz_id) {
              const existingIsFallback = existing.musicbrainz_id.startsWith('fallback_');
              const newIsFallback = mbData.musicbrainz_id.startsWith('fallback_');
              
              if (existingIsFallback && !newIsFallback) {
                finalMbId = mbData.musicbrainz_id;
                await mergeIntoCanonical(existing.musicbrainz_id, mbData.musicbrainz_id);
                mergedCount++;
              } else {
                finalMbId = existing.musicbrainz_id;
              }
            } else {
              finalMbId = existing.musicbrainz_id;
            }
          }
          
          await dbRun(`
            INSERT INTO albums_global (
              musicbrainz_id, album_name, artist_name, 
              canonical_album, canonical_artist, release_year,
              image_url, lastfm_url, album_type, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
            ON CONFLICT(musicbrainz_id) 
            DO UPDATE SET 
              album_name = EXCLUDED.album_name,
              artist_name = EXCLUDED.artist_name,
              canonical_album = EXCLUDED.canonical_album,
              canonical_artist = EXCLUDED.canonical_artist,
              release_year = COALESCE(EXCLUDED.release_year, albums_global.release_year),
              image_url = EXCLUDED.image_url,
              lastfm_url = EXCLUDED.lastfm_url,
              album_type = EXCLUDED.album_type,
              updated_at = CURRENT_TIMESTAMP
          `, [
            finalMbId, 
            a.name, 
            a.artist.name, 
            mbData.canonical_name, 
            mbData.canonical_artist, 
            mbData.release_year, 
            a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
            a.url, 
            mbData.type
          ]);

          await dbRun(`
            INSERT INTO user_albums (username, musicbrainz_id, playcount, updated_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            ON CONFLICT(username, musicbrainz_id) 
            DO UPDATE SET 
              playcount = EXCLUDED.playcount, 
              updated_at = CURRENT_TIMESTAMP
          `, [username, finalMbId, parseInt(a.playcount)]);
          
          totalUpdated++;
          if (totalUpdated % 10 === 0) console.log(`  ... processed ${totalUpdated} albums (merged ${mergedCount})`);

        } catch (innerErr) {
          console.error(`  ⚠️ Skipped album: ${a.name}`);
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
    console.log(`✅ BACKGROUND: Finished ${username}. Updated ${totalUpdated} albums, merged ${mergedCount} duplicates in ${duration}s`);

  } catch (err) {
    console.error(`❌ BACKGROUND ERROR for ${username}:`, err);
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
  
  // ... (Previous code: "return;" from the real-time block)

  console.log(`🟢 Mode: CACHED`);

  try {
    // 1. Base Query with $1 placeholder for username
    let query = `
      SELECT 
        ag.musicbrainz_id,
        ag.album_name,
        ag.artist_name,
        ag.canonical_album,
        ag.canonical_artist,
        ag.release_year,
        ag.image_url,
        ag.lastfm_url,
        ag.album_type,
        ua.playcount
      FROM user_albums ua
      JOIN albums_global ag ON ua.musicbrainz_id = ag.musicbrainz_id
      WHERE ua.username = $1
    `;
    
    // 2. Initialize params array with username
    const params = [username];
    let pIdx = 2; // Start counting extra params from $2

    // 3. Add Year or Decade Filters using $ placeholders
    if (year) {
      query += ` AND ag.release_year = $${pIdx} AND ag.release_year IS NOT NULL`;
      params.push(year);
      pIdx++;
    } 
    else if (decade || (yearStart && yearEnd)) {
      // Handle both "decade" param and "yearStart/End" param
      const start = decade || yearStart;
      const end = decade ? (decade + 9) : yearEnd;
      
      query += ` AND ag.release_year BETWEEN $${pIdx} AND $${pIdx + 1} AND ag.release_year IS NOT NULL`;
      params.push(start, end);
      pIdx += 2;
    }

    // 4. Add Artist Filter
    if (artist) {
      query += ` AND (LOWER(ag.artist_name) LIKE $${pIdx} OR LOWER(ag.canonical_artist) LIKE $${pIdx + 1})`;
      params.push(`%${artist}%`, `%${artist}%`);
      pIdx += 2;
    }

    // 5. Add Sorting and LIMIT
    // IMPORTANT: We must use the $ symbol for the limit parameter too
    query += ` ORDER BY ua.playcount DESC LIMIT $${pIdx}`;
    params.push(limit);

    // 6. Execute Query
    const albums = await dbQuery(query, params);
    
    // Check if user exists to prevent empty cache confusion
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
    }

    query += ` ORDER BY updated_at DESC LIMIT $${paramIndex + (year ? 1 : 0)}`;
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

app.listen(PORT, () => {
  console.log(`\n🎵 Last.fm Top Albums API Server`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`\n📍 Server: http://localhost:${PORT}`);
  console.log(`💾 Database: ${dbType === 'postgres' ? 'PostgreSQL' : 'SQLite'}`);
  console.log(`💾 Cached users: ${CACHED_USERS.length > 0 ? CACHED_USERS.join(', ') : 'none'}`);
  console.log(`🔴 Public users: real-time mode\n`);
});