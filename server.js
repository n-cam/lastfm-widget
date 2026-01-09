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
        musicbrainz_id TEXT,
        release_year INTEGER,
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
  const cleanedAlbum = cleanAlbumName(album);
  const cleanedArtist = cleanArtistName(artist);
  const cacheKey = `${normalizeForComparison(cleanedArtist)}::${normalizeForComparison(cleanedAlbum)}`;
  
  if (mbGlobalCache.has(cacheKey)) {
    return mbGlobalCache.get(cacheKey);
  }

  if (mbFailureCache.has(cacheKey)) {
    const failTime = mbFailureCache.get(cacheKey);
    if (Date.now() - failTime < 24 * 60 * 60 * 1000) {
      return { 
        musicbrainz_id: null,
        release_year: null, 
        type: null,
        canonical_name: cleanedAlbum,
        canonical_artist: cleanedArtist
      };
    }
  }

  try {
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
          canonical_name: cleanAlbumName(best.title),
          canonical_artist: cleanArtistName(best.rgArtist)
        };
        
        mbGlobalCache.set(cacheKey, result);
        mbFailureCache.delete(cacheKey);
        return result;
      }
    }
    
    mbFailureCache.set(cacheKey, Date.now());
    
    const noMatch = { 
      musicbrainz_id: null,
      release_year: null, 
      type: null,
      canonical_name: cleanedAlbum,
      canonical_artist: cleanedArtist
    };
    mbGlobalCache.set(cacheKey, noMatch);
    return noMatch;
    
  } catch (err) {
    console.error(`MusicBrainz lookup failed for ${artist} - ${album}:`, err.message);
    mbFailureCache.set(cacheKey, Date.now());
    
    return { 
      musicbrainz_id: null,
      release_year: null, 
      type: null,
      canonical_name: cleanedAlbum,
      canonical_artist: cleanedArtist
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
                album_name = EXCLUDED.album_name,
                artist_name = EXCLUDED.artist_name,
                musicbrainz_id = COALESCE(EXCLUDED.musicbrainz_id, albums_global.musicbrainz_id),
                release_year = COALESCE(EXCLUDED.release_year, albums_global.release_year),
                image_url = EXCLUDED.image_url,
                lastfm_url = EXCLUDED.lastfm_url,
                album_type = COALESCE(EXCLUDED.album_type, albums_global.album_type),
                updated_at = CURRENT_TIMESTAMP
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
              'SELECT id FROM albums_global WHERE canonical_album = ? AND canonical_artist = ?',
              [mbData.canonical_name, mbData.canonical_artist]
            );

            if (existing) {
              await dbRun(`
                UPDATE albums_global SET
                  album_name = ?,
                  artist_name = ?,
                  musicbrainz_id = COALESCE(?, musicbrainz_id),
                  release_year = COALESCE(?, release_year),
                  image_url = ?,
                  lastfm_url = ?,
                  album_type = COALESCE(?, album_type),
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
              `, [
                a.name,
                a.artist.name,
                mbData.musicbrainz_id,
                mbData.release_year,
                a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
                a.url,
                mbData.type,
                existing.id
              ]);
              albumId = existing.id;
            } else {
              const info = db.prepare(`
                INSERT INTO albums_global (
                  canonical_album, canonical_artist, album_name, artist_name,
                  musicbrainz_id, release_year, image_url, lastfm_url, album_type, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
              `).run(
                mbData.canonical_name,
                mbData.canonical_artist,
                a.name,
                a.artist.name,
                mbData.musicbrainz_id,
                mbData.release_year,
                a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
                a.url,
                mbData.type
              );
              albumId = info.lastInsertRowid;
            }
          }

          // Upsert user album data
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

    if (year) {
      query += ` AND ag.release_year = ${pIdx} AND ag.release_year IS NOT NULL`;
      params.push(year);
      pIdx++;
    } 
    else if (decade || (yearStart && yearEnd)) {
      const start = decade || yearStart;
      const end = decade ? (decade + 9) : yearEnd;
      
      query += ` AND ag.release_year BETWEEN ${pIdx} AND ${pIdx + 1} AND ag.release_year IS NOT NULL`;
      params.push(start, end);
      pIdx += 2;
    }

    if (artist) {
      query += ` AND (LOWER(ag.artist_name) LIKE ${pIdx} OR LOWER(ag.canonical_artist) LIKE ${pIdx + 1})`;
      params.push(`%${artist}%`, `%${artist}%`);
      pIdx += 2;
    }

    query += ` ORDER BY ua.playcount DESC LIMIT ${pIdx}`;
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
      query += ` WHERE release_year = ${paramIndex}`;
      params.push(year);
      paramIndex++;
    }

    query += ` ORDER BY updated_at DESC LIMIT ${paramIndex}`;
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