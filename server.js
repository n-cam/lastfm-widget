require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const app = express();
const PORT = process.env.PORT || 3000;

// Database setup - use PostgreSQL if DATABASE_URL exists, otherwise SQLite
let db;
let dbType;

if (process.env.DATABASE_URL) {
  // PostgreSQL for production (Render)
  console.log('🐘 Using PostgreSQL database');
  const { Pool } = require('pg');
  db = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
  });
  dbType = 'postgres';
} else {
  // SQLite for local development
  console.log('💾 Using SQLite database');
  const Database = require('better-sqlite3');
  db = new Database('database.db');
  db.pragma('journal_mode = WAL');
  dbType = 'sqlite';
}

// List of users who get persistent caching
const CACHED_USERS = (process.env.CACHED_USERS || '').split(',').filter(Boolean);

// Initialize database tables
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

      CREATE TABLE IF NOT EXISTS albums (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        album_name TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        playcount INTEGER DEFAULT 0,
        release_year INTEGER,
        musicbrainz_id TEXT,
        canonical_album TEXT,
        canonical_artist TEXT,
        image_url TEXT,
        lastfm_url TEXT,
        album_type TEXT,
        last_scrobble TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(username, musicbrainz_id)
      );

      CREATE INDEX IF NOT EXISTS idx_username ON albums(username);
      CREATE INDEX IF NOT EXISTS idx_release_year ON albums(release_year);
      CREATE INDEX IF NOT EXISTS idx_playcount ON albums(playcount);
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

      CREATE TABLE IF NOT EXISTS albums (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        album_name TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        playcount INTEGER DEFAULT 0,
        release_year INTEGER,
        musicbrainz_id TEXT,
        canonical_album TEXT,
        canonical_artist TEXT,
        image_url TEXT,
        lastfm_url TEXT,
        album_type TEXT,
        last_scrobble TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(username, musicbrainz_id)
      );

      CREATE INDEX IF NOT EXISTS idx_username ON albums(username);
      CREATE INDEX IF NOT EXISTS idx_release_year ON albums(release_year);
      CREATE INDEX IF NOT EXISTS idx_playcount ON albums(playcount);
    `);
  }
}

// Database helper functions
async function dbQuery(query, params = []) {
  if (dbType === 'postgres') {
    const result = await db.query(query, params);
    return result.rows;
  } else {
    return db.prepare(query).all(...params);
  }
}

async function dbGet(query, params = []) {
  if (dbType === 'postgres') {
    const result = await db.query(query, params);
    return result.rows[0] || null;
  } else {
    return db.prepare(query).get(...params);
  }
}

async function dbRun(query, params = []) {
  if (dbType === 'postgres') {
    await db.query(query, params);
  } else {
    db.prepare(query).run(...params);
  }
}

// Initialize on startup
initDatabase().then(() => {
  console.log('✅ Database initialized');
  console.log('📋 Cached users:', CACHED_USERS.length > 0 ? CACHED_USERS.join(', ') : 'none');
});

// In-memory cache for MusicBrainz lookups
const mbCache = new Map();

// Helper functions (same as before)
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
  return str.toLowerCase()
    .replace(/^(the|a|an)\s+/i, '')
    .replace(/^(ms\.?|mr\.?|mrs\.?|dr\.?)\s+/i, '')
    .replace(/[^a-z0-9]/g, '')
    .trim();
}

function stringSimilarity(str1, str2) {
  const s1 = normalizeForComparison(str1);
  const s2 = normalizeForComparison(str2);
  
  if (s1 === s2) return 1.0;
  if (s1.includes(s2) || s2.includes(s1)) return 0.8;
  
  const chars1 = new Set(s1);
  const chars2 = new Set(s2);
  const intersection = new Set([...chars1].filter(x => chars2.has(x)));
  const union = new Set([...chars1, ...chars2]);
  
  return intersection.size / union.size;
}

async function getMusicBrainzData(artist, album) {
  const cacheKey = `${artist}::${album}`;
  
  if (mbCache.has(cacheKey)) {
    return mbCache.get(cacheKey);
  }

  try {
    const cleanedAlbum = cleanAlbumName(album);
    const cleanedArtist = cleanArtistName(artist);
    
    const query = encodeURIComponent(`release:"${cleanedAlbum}" AND artist:"${cleanedArtist}"`);
    const mbUrl = `https://musicbrainz.org/ws/2/release-group/?query=${query}&fmt=json&limit=10`;
    
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    const response = await fetch(mbUrl, {
      headers: {
        'User-Agent': `LastFmTopAlbums/1.0.0 ( ${process.env.YOUR_EMAIL} )`
      }
    });
    
    const data = await response.json();
    
    if (data['release-groups'] && data['release-groups'].length > 0) {
      const candidates = data['release-groups']
        .filter(rg => {
          if (!rg['first-release-date']) return false;
          if (!rg['artist-credit'] || !rg['artist-credit'][0]) return false;
          
          const rgArtist = rg['artist-credit'][0].name;
          const artistSimilarity = stringSimilarity(artist, rgArtist);
          if (artistSimilarity < 0.5) return false;
          
          const titleSimilarity = stringSimilarity(cleanedAlbum, rg.title);
          if (titleSimilarity < 0.4) return false;
          
          const primaryType = rg['primary-type'];
          if (primaryType === 'Single') return false;
          
          return true;
        })
        .map(rg => {
          const year = parseInt(rg['first-release-date'].split('-')[0]);
          const primaryType = rg['primary-type'];
          const rgArtist = rg['artist-credit'][0].name;
          
          const artistSimilarity = stringSimilarity(artist, rgArtist);
          const titleSimilarity = stringSimilarity(cleanedAlbum, rg.title);
          
          let score = 0;
          score += artistSimilarity * 200;
          score += titleSimilarity * 150;
          if (primaryType === 'Album') score += 50;
          if (year < 2000) score += 30;
          else if (year < 2010) score += 20;
          else if (year < 2015) score += 10;
          
          if (normalizeForComparison(rg.title) === normalizeForComparison(cleanedAlbum)) {
            score += 100;
          }
          if (normalizeForComparison(rgArtist) === normalizeForComparison(artist)) {
            score += 100;
          }
          
          return { ...rg, score, year, artistSimilarity, titleSimilarity, rgArtist };
        })
        .sort((a, b) => {
          if (Math.abs(b.score - a.score) > 10) return b.score - a.score;
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
        return result;
      }
    }
    
    const noMatch = { 
      musicbrainz_id: `fallback_${cleanedAlbum}_${artist}`, 
      release_year: null, 
      type: null,
      canonical_name: cleanedAlbum,
      canonical_artist: artist
    };
    mbCache.set(cacheKey, noMatch);
    return noMatch;
    
  } catch (err) {
    console.error(`MusicBrainz lookup failed for ${artist} - ${album}:`, err.message);
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

async function fetchAllAlbums(username, totalLimit) {
  const perPage = 1000;
  const pages = Math.ceil(totalLimit / perPage);
  let allAlbums = [];

  console.log(`📥 Fetching ${totalLimit} albums in ${pages} page(s)...`);

  for (let page = 1; page <= pages; page++) {
    const limit = Math.min(perPage, totalLimit - allAlbums.length);
    const lastfmUrl = `http://ws.audioscrobbler.com/2.0/?method=user.gettopalbums&user=${username}&api_key=${process.env.LASTFM_API_KEY}&format=json&limit=${limit}&page=${page}`;
    
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

// Serve static files
app.use(express.static('public'));

// CORS and JSON middleware
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
  
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  
  next();
});

app.use(express.json());

// Root
app.get('/', (req, res) => {
  res.sendFile(__dirname + '/public/index.html');
});

// Check if user should use cache
function shouldUseCache(username) {
  return CACHED_USERS.includes(username);
}

// Get top albums (hybrid: cache or real-time)
app.get('/api/top-albums', async (req, res) => {
  const username = req.query.user;

  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  // Real-time mode for non-cached users
  if (!shouldUseCache(username)) {
    console.log(`➡️  ${username} - using real-time mode`);
    
    const year = req.query.year ? parseInt(req.query.year) : null;
    const decade = req.query.decade ? parseInt(req.query.decade) : null;
    const yearStart = req.query.yearStart ? parseInt(req.query.yearStart) : null;
    const yearEnd = req.query.yearEnd ? parseInt(req.query.yearEnd) : null;
    const limit = req.query.limit ? parseInt(req.query.limit) : 50;

    try {
      const hasFilter = year || decade || yearStart || yearEnd;
      const fetchLimit = hasFilter ? 1000 : Math.min(200, limit);
      
      const lastfmUrl = `http://ws.audioscrobbler.com/2.0/?method=user.gettopalbums&user=${username}&api_key=${process.env.LASTFM_API_KEY}&format=json&limit=${fetchLimit}`;
      
      console.log(`  📡 Fetching ${fetchLimit} albums from Last.fm API...`);
      const response = await fetch(lastfmUrl);
      
      if (!response.ok) {
        console.error(`  ❌ Last.fm API returned ${response.status}`);
        return res.status(response.status).json({ error: 'Last.fm API error', status: response.status });
      }
      
      const data = await response.json();

      if (!data.topalbums || !data.topalbums.album) {
        return res.status(404).json({ error: 'User not found or no albums' });
      }

      const albums = Array.isArray(data.topalbums.album) ? data.topalbums.album : [data.topalbums.album];
      console.log(`  ✓ Got ${albums.length} albums from Last.fm (scanning for ${limit} matches)`);

      const processedAlbums = [];
      
      console.log(`  🔍 Looking up release years...`);
      
      for (let i = 0; i < albums.length; i++) {
        const a = albums[i];
        
        const mbData = await getMusicBrainzData(a.artist.name, a.name);
        
        let shouldInclude = true;
        
        if (year && mbData.release_year !== year) {
          shouldInclude = false;
        }
        
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
          break;
        }
        
        if ((i + 1) % 10 === 0) {
          console.log(`    Processed ${i + 1}/${albums.length} albums, found ${processedAlbums.length} matches`);
        }
      }

      console.log(`  ✓ Returning ${processedAlbums.length} albums`);

      return res.json({
        user: username,
        mode: 'realtime',
        filters: { year, decade, yearStart, yearEnd },
        count: processedAlbums.length,
        albums: processedAlbums
      });

    } catch (err) {
      console.error('Real-time fetch error:', err);
      return res.status(500).json({ error: 'Failed to fetch albums', details: err.message });
    }
  }

  // Use cache for cached users
  console.log(`💾 Using cache for ${username}`);
  
  const year = req.query.year ? parseInt(req.query.year) : null;
  const decade = req.query.decade ? parseInt(req.query.decade) : null;
  const yearStart = req.query.yearStart ? parseInt(req.query.yearStart) : null;
  const yearEnd = req.query.yearEnd ? parseInt(req.query.yearEnd) : null;
  const artist = req.query.artist ? req.query.artist.toLowerCase() : null;
  const limit = req.query.limit ? parseInt(req.query.limit) : 50;

  try {
    let query = `SELECT * FROM albums WHERE username = $1`;
    const params = [username];
    let paramIndex = 2;

    if (year) {
      query += ` AND release_year = $${paramIndex}`;
      params.push(year);
      paramIndex++;
    } else if (decade) {
      const decadeStart = decade;
      const decadeEnd = decade + 9;
      query += ` AND release_year BETWEEN $${paramIndex} AND $${paramIndex + 1}`;
      params.push(decadeStart, decadeEnd);
      paramIndex += 2;
    } else if (yearStart && yearEnd) {
      query += ` AND release_year BETWEEN $${paramIndex} AND $${paramIndex + 1}`;
      params.push(yearStart, yearEnd);
      paramIndex += 2;
    }

    if (artist) {
      query += ` AND (LOWER(artist_name) LIKE $${paramIndex} OR LOWER(canonical_artist) LIKE $${paramIndex + 1})`;
      params.push(`%${artist}%`, `%${artist}%`);
      paramIndex += 2;
    }

    query += ` ORDER BY playcount DESC LIMIT $${paramIndex}`;
    params.push(limit);

    const albums = await dbQuery(query, params);
    const user = await dbGet('SELECT * FROM users WHERE username = $1', [username]);

    if (!user) {
      return res.status(404).json({ 
        error: 'User not cached. Run /api/update?user=' + username + '&full=true first' 
      });
    }

    res.json({
      user: username,
      mode: 'cached',
      filters: { year, decade, yearStart, yearEnd, artist },
      count: albums.length,
      user_info: {
        total_albums: user.total_albums,
        last_update_full: user.last_update_full,
        last_update_recent: user.last_update_recent
      },
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
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Query failed', details: err.message });
  }
});

// Update cache (only for cached users)
app.get('/api/update', async (req, res) => {
  const username = req.query.user;
  const full = req.query.full === 'true';
  const limit = full ? parseInt(req.query.limit || 5000) : 200;

  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  if (!shouldUseCache(username)) {
    return res.status(403).json({ 
      error: 'Cache updates are only available for specific users.' 
    });
  }

  const estimatedMinutes = full ? Math.round((limit / 60) * 1.2) : 3;
  console.log(`\n🔄 ${full ? 'FULL' : 'QUICK'} UPDATE for ${username}`);
  console.log(`   Limit: ${limit} albums`);
  console.log(`   Estimated time: ${estimatedMinutes} minutes`);

  try {
    await dbRun(`INSERT INTO users (username) VALUES ($1) ON CONFLICT (username) DO NOTHING`, [username]);

    const albums = await fetchAllAlbums(username, limit);

    if (albums.length === 0) {
      return res.status(404).json({ error: "No albums found" });
    }

    console.log(`✅ Total fetched: ${albums.length} albums from Last.fm`);

    let updated = 0;

    for (const a of albums) {
      try {
        const mbData = await getMusicBrainzData(a.artist.name, a.name);
        
        const query = `
          INSERT INTO albums (
            username, album_name, artist_name, playcount, 
            release_year, musicbrainz_id, canonical_album, canonical_artist,
            image_url, lastfm_url, album_type, updated_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP)
          ON CONFLICT(username, musicbrainz_id) 
          DO UPDATE SET 
            playcount = EXCLUDED.playcount,
            updated_at = CURRENT_TIMESTAMP
        `;
        
        await dbRun(query, [
          username,
          a.name,
          a.artist.name,
          parseInt(a.playcount),
          mbData.release_year,
          mbData.musicbrainz_id,
          mbData.canonical_name,
          mbData.canonical_artist,
          a.image.find(img => img.size === 'extralarge')?.['#text'] || '',
          a.url,
          mbData.type
        ]);
        
        updated++;
      } catch (err) {
        console.error(`Error processing album:`, err.message);
      }
    }

    const updateQuery = `
      UPDATE users 
      SET ${full ? 'last_update_full' : 'last_update_recent'} = CURRENT_TIMESTAMP,
          total_albums = (SELECT COUNT(*) FROM albums WHERE username = $1)
      WHERE username = $1
    `;
    await dbRun(updateQuery, [username]);

    console.log(`✅ Update complete: ${updated} updated`);

    res.json({
      success: true,
      username,
      update_type: full ? 'full' : 'recent',
      albums_fetched: albums.length,
      albums_updated: updated,
      total_cached: updated
    });

  } catch (err) {
    console.error('Update error:', err);
    res.status(500).json({ error: 'Update failed', details: err.message });
  }
});

// Cache stats
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
    const albumCount = await dbGet('SELECT COUNT(*) as count FROM albums WHERE username = $1', [username]);

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
    console.error(err);
    res.status(500).json({ error: 'Query failed', details: err.message });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`\n🎵 Last.fm Top Albums API Server`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`\n📍 Server: http://localhost:${PORT}`);
  console.log(`💾 Database: ${dbType === 'postgres' ? 'PostgreSQL' : 'SQLite'}`);
  console.log(`💾 Cached users: ${CACHED_USERS.length > 0 ? CACHED_USERS.join(', ') : 'none'}`);
  console.log(`🔴 Public users: real-time mode\n`);
});