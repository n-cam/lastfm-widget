require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const Database = require('better-sqlite3');
const app = express();
const PORT = process.env.PORT || 3000;

// Initialize SQLite database
const db = new Database('database.db');
db.pragma('journal_mode = WAL'); // Better performance

// Create tables
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

console.log('✅ Database initialized');

// In-memory cache for MusicBrainz lookups (session cache)
const mbCache = new Map();

// Helper: Clean album name (remove edition labels)
function cleanAlbumName(name) {
  return name
    .replace(/\s*\(.*?(Deluxe|Remaster|Edition|Anniversary|Expanded|Special|Bonus|Live|Explicit|Extended|Target|Walmart|Japan|Import|Clean|Dirty).*?\)/gi, '')
    .replace(/\s*\[.*?(Deluxe|Remaster|Edition|Anniversary|Expanded|Special|Bonus|Live|Explicit|Extended|Target|Walmart|Japan|Import|Clean|Dirty).*?\]/gi, '')
    .replace(/\s*-\s*(Deluxe|Remaster|Edition|Anniversary|Expanded|Special|Bonus).*/gi, '')
    .trim();
}

// Helper: Clean artist name for searching
function cleanArtistName(name) {
  return name
    .replace(/^(Ms\.?|Mr\.?|Mrs\.?|Dr\.?)\s+/i, '')
    .replace(/^(The|A|An)\s+/i, '')
    .trim();
}

// Helper: Normalize strings for comparison
function normalizeForComparison(str) {
  return str.toLowerCase()
    .replace(/^(the|a|an)\s+/i, '')
    .replace(/^(ms\.?|mr\.?|mrs\.?|dr\.?)\s+/i, '')
    .replace(/[^a-z0-9]/g, '')
    .trim();
}

// Helper: Calculate string similarity (0-1)
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

// Helper: Query MusicBrainz for album metadata
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

// Serve static files from public directory
app.use(express.static('public'));

// Root route to serve index.html
app.get('/', (req, res) => {
  res.sendFile(__dirname + '/public/index.html');
});

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

// Root test
app.get('/', (req, res) => {
  res.send('Last.fm Top Albums API with SQLite caching is running!');
});

// Helper: Fetch albums from Last.fm with pagination
async function fetchAllAlbums(username, totalLimit) {
  const perPage = 1000; // Last.fm API maximum
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

    // Small delay between pages to be nice to Last.fm
    if (page < pages) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }

  return allAlbums;
}

// Update user cache (full or recent)
app.get('/api/update', async (req, res) => {
  const username = req.query.user;
  const full = req.query.full === 'true';
  const limit = full ? parseInt(req.query.limit || 5000) : 200;

  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  const estimatedMinutes = full ? Math.round((limit / 60) * 1.2) : 3;
  console.log(`\n🔄 ${full ? 'FULL' : 'QUICK'} UPDATE for ${username}`);
  console.log(`   Limit: ${limit} albums`);
  console.log(`   Estimated time: ${estimatedMinutes} minutes`);

  try {
    // Ensure user exists in database
    const insertUser = db.prepare(`
      INSERT OR IGNORE INTO users (username) VALUES (?)
    `);
    insertUser.run(username);

    // Fetch albums from Last.fm (with pagination for full updates)
    const albums = await fetchAllAlbums(username, limit);

    if (albums.length === 0) {
      return res.status(404).json({ error: "No albums found" });
    }

    console.log(`✅ Total fetched: ${albums.length} albums from Last.fm`);

    const upsertAlbum = db.prepare(`
      INSERT INTO albums (
        username, album_name, artist_name, playcount, 
        release_year, musicbrainz_id, canonical_album, canonical_artist,
        image_url, lastfm_url, album_type, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(username, musicbrainz_id) 
      DO UPDATE SET 
        playcount = excluded.playcount,
        updated_at = CURRENT_TIMESTAMP
    `);

    let processed = 0;
    let updated = 0;
    let skipped = 0;
    let errors = 0;
    const startTime = Date.now();

    console.log(`\n🔍 Processing albums with MusicBrainz lookup...`);

    for (const a of albums) {
      processed++;
      
      // Progress indicator every 50 albums
      if (processed % 50 === 0 || processed === albums.length) {
        const elapsed = Math.round((Date.now() - startTime) / 1000 / 60);
        const rate = processed / ((Date.now() - startTime) / 1000);
        const remaining = Math.round((albums.length - processed) / rate / 60);
        console.log(`[${processed}/${albums.length}] ${elapsed}m elapsed, ~${remaining}m remaining`);
      }

      try {
        const mbData = await getMusicBrainzData(a.artist.name, a.name);
        
        upsertAlbum.run(
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
        );
        updated++;
      } catch (err) {
        if (err.message.includes('UNIQUE constraint')) {
          skipped++;
        } else {
          console.error(`  ⚠️  Error processing ${a.artist.name} - ${a.name}:`, err.message);
          errors++;
        }
      }
    }

    // Update user metadata
    const updateUser = db.prepare(`
      UPDATE users 
      SET ${full ? 'last_update_full' : 'last_update_recent'} = CURRENT_TIMESTAMP,
          total_albums = (SELECT COUNT(*) FROM albums WHERE username = ?)
      WHERE username = ?
    `);
    updateUser.run(username, username);

    console.log(`✅ Update complete: ${updated} updated, ${skipped} skipped`);

    res.json({
      success: true,
      username,
      update_type: full ? 'full' : 'recent',
      albums_fetched: data.topalbums.album.length,
      albums_updated: updated,
      albums_skipped: skipped,
      total_cached: updated + skipped
    });

  } catch (err) {
    console.error('Update error:', err);
    res.status(500).json({ error: 'Update failed', details: err.message });
  }
});

// Get top albums (from cache)
app.get('/api/top-albums', async (req, res) => {
  const username = req.query.user;
  const year = req.query.year ? parseInt(req.query.year) : null;
  const decade = req.query.decade ? parseInt(req.query.decade) : null;
  const yearStart = req.query.yearStart ? parseInt(req.query.yearStart) : null;
  const yearEnd = req.query.yearEnd ? parseInt(req.query.yearEnd) : null;
  const artist = req.query.artist ? req.query.artist.toLowerCase() : null;
  const limit = req.query.limit ? parseInt(req.query.limit) : 50;

  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  try {
    let query = `
      SELECT * FROM albums 
      WHERE username = ?
    `;
    const params = [username];

    // Year filter
    if (year) {
      query += ` AND release_year = ?`;
      params.push(year);
    } 
    // Decade filter
    else if (decade) {
      const decadeStart = decade;
      const decadeEnd = decade + 9;
      query += ` AND release_year BETWEEN ? AND ?`;
      params.push(decadeStart, decadeEnd);
    }
    // Date range filter (NEW!)
    else if (yearStart && yearEnd) {
      query += ` AND release_year BETWEEN ? AND ?`;
      params.push(yearStart, yearEnd);
    }

    // Artist filter (NEW!)
    if (artist) {
      query += ` AND (LOWER(artist_name) LIKE ? OR LOWER(canonical_artist) LIKE ?)`;
      params.push(`%${artist}%`, `%${artist}%`);
    }

    query += ` ORDER BY playcount DESC LIMIT ?`;
    params.push(limit);

    const albums = db.prepare(query).all(...params);

    // Get user info
    const user = db.prepare('SELECT * FROM users WHERE username = ?').get(username);

    if (!user) {
      return res.status(404).json({ 
        error: 'User not cached. Run /api/update?user=' + username + '&full=true first' 
      });
    }

    res.json({
      user: username,
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

// Cache stats endpoint
app.get('/api/cache/stats', (req, res) => {
  const username = req.query.user;

  if (username) {
    const user = db.prepare('SELECT * FROM users WHERE username = ?').get(username);
    const albumCount = db.prepare('SELECT COUNT(*) as count FROM albums WHERE username = ?').get(username);

    if (!user) {
      return res.status(404).json({ error: 'User not found in cache' });
    }

    res.json({
      username: user.username,
      total_albums: albumCount.count,
      last_update_full: user.last_update_full,
      last_update_recent: user.last_update_recent,
      created_at: user.created_at
    });
  } else {
    const totalUsers = db.prepare('SELECT COUNT(*) as count FROM users').get();
    const totalAlbums = db.prepare('SELECT COUNT(*) as count FROM albums').get();
    const users = db.prepare('SELECT username, total_albums, last_update_recent FROM users').all();

    res.json({
      total_users: totalUsers.count,
      total_albums: totalAlbums.count,
      users
    });
  }
});

// Get top artists endpoint (NEW!)
app.get('/api/top-artists', (req, res) => {
  const username = req.query.user;
  const limit = req.query.limit ? parseInt(req.query.limit) : 50;

  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  try {
    const artists = db.prepare(`
      SELECT 
        canonical_artist as artist,
        COUNT(*) as album_count,
        SUM(playcount) as total_plays
      FROM albums 
      WHERE username = ?
      GROUP BY canonical_artist
      ORDER BY total_plays DESC
      LIMIT ?
    `).all(username, limit);

    res.json({
      user: username,
      count: artists.length,
      artists
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Query failed', details: err.message });
  }
});

// Get albums by specific artist (NEW!)
app.get('/api/artist-albums', (req, res) => {
  const username = req.query.user;
  const artist = req.query.artist;

  if (!username || !artist) {
    return res.status(400).json({ error: "Missing 'user' or 'artist' query param" });
  }

  try {
    const albums = db.prepare(`
      SELECT * FROM albums 
      WHERE username = ? 
      AND (LOWER(artist_name) LIKE ? OR LOWER(canonical_artist) LIKE ?)
      ORDER BY playcount DESC
    `).all(username, `%${artist.toLowerCase()}%`, `%${artist.toLowerCase()}%`);

    res.json({
      user: username,
      artist: artist,
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
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Query failed', details: err.message });
  }
});

// Get albums with missing release years (NEW!)
app.get('/api/missing-years', (req, res) => {
  const username = req.query.user;

  if (!username) return res.status(400).json({ error: "Missing 'user' query param" });

  try {
    const albums = db.prepare(`
      SELECT * FROM albums 
      WHERE username = ? AND release_year IS NULL
      ORDER BY playcount DESC
    `).all(username);

    res.json({
      user: username,
      count: albums.length,
      albums: albums.map(a => ({
        id: a.id,
        name: a.canonical_album || a.album_name,
        artist: a.canonical_artist || a.artist_name,
        playcount: a.playcount,
        url: a.lastfm_url,
        image: a.image_url,
        musicbrainz_id: a.musicbrainz_id
      }))
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Query failed', details: err.message });
  }
});

// Manual release year override (NEW!)
app.post('/api/override-year', (req, res) => {
  const id = req.query.id;
  const year = req.query.year ? parseInt(req.query.year) : null;

  if (!id) return res.status(400).json({ error: "Missing 'id' query param" });
  if (!year || year < 1900 || year > 2100) {
    return res.status(400).json({ error: "Invalid year (must be 1900-2100)" });
  }

  try {
    const result = db.prepare(`
      UPDATE albums 
      SET release_year = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `).run(year, id);

    if (result.changes === 0) {
      return res.status(404).json({ error: 'Album not found' });
    }

    const album = db.prepare('SELECT * FROM albums WHERE id = ?').get(id);

    res.json({
      success: true,
      message: `Release year updated to ${year}`,
      album: {
        id: album.id,
        name: album.canonical_album || album.album_name,
        artist: album.canonical_artist || album.artist_name,
        release_year: album.release_year
      }
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Update failed', details: err.message });
  }
});

// Batch update release years (NEW!)
app.post('/api/batch-override-years', express.json(), (req, res) => {
  const updates = req.body.updates; // Array of {id, year}

  if (!updates || !Array.isArray(updates)) {
    return res.status(400).json({ error: "Missing 'updates' array in request body" });
  }

  try {
    const stmt = db.prepare(`
      UPDATE albums 
      SET release_year = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `);

    let updated = 0;
    let errors = 0;

    for (const update of updates) {
      try {
        const result = stmt.run(update.year, update.id);
        if (result.changes > 0) updated++;
      } catch (err) {
        errors++;
      }
    }

    res.json({
      success: true,
      updated,
      errors,
      total: updates.length
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Batch update failed', details: err.message });
  }
});

// Clear cache endpoint
app.post('/api/cache/clear', (req, res) => {
  const username = req.query.user;

  if (username) {
    db.prepare('DELETE FROM albums WHERE username = ?').run(username);
    db.prepare('DELETE FROM users WHERE username = ?').run(username);
    res.json({ message: `Cache cleared for ${username}` });
  } else {
    db.prepare('DELETE FROM albums').run();
    db.prepare('DELETE FROM users').run();
    res.json({ message: 'All cache cleared' });
  }
});

// ===== AUTOMATIC UPDATE SCHEDULER =====

let schedulerEnabled = process.env.AUTO_UPDATE === 'true';
let updateInterval = null;

// Function to check and update stale users
async function autoUpdateUsers() {
  console.log('\n🤖 Auto-update: Checking for stale caches...');
  
  try {
    // Find users who haven't been updated in 24 hours
    const staleUsers = db.prepare(`
      SELECT username, last_update_recent 
      FROM users 
      WHERE datetime(last_update_recent) < datetime('now', '-24 hours')
      OR last_update_recent IS NULL
    `).all();

    if (staleUsers.length === 0) {
      console.log('✅ All user caches are up to date!');
      return;
    }

    console.log(`📝 Found ${staleUsers.length} user(s) with stale cache`);

    for (const user of staleUsers) {
      console.log(`\n🔄 Auto-updating ${user.username}...`);
      
      try {
        // Fetch recent albums (quick update)
        const albums = await fetchAllAlbums(user.username, 200);
        
        if (albums.length === 0) {
          console.log(`  ⚠️  No albums found for ${user.username}`);
          continue;
        }

        const upsertAlbum = db.prepare(`
          INSERT INTO albums (
            username, album_name, artist_name, playcount, 
            release_year, musicbrainz_id, canonical_album, canonical_artist,
            image_url, lastfm_url, album_type, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
          ON CONFLICT(username, musicbrainz_id) 
          DO UPDATE SET 
            playcount = excluded.playcount,
            updated_at = CURRENT_TIMESTAMP
        `);

        let updated = 0;

        for (const a of albums) {
          const mbData = await getMusicBrainzData(a.artist.name, a.name);
          
          try {
            upsertAlbum.run(
              user.username,
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
            );
            updated++;
          } catch (err) {
            // Skip duplicates
          }
        }

        // Update user timestamp
        db.prepare(`
          UPDATE users 
          SET last_update_recent = CURRENT_TIMESTAMP,
              total_albums = (SELECT COUNT(*) FROM albums WHERE username = ?)
          WHERE username = ?
        `).run(user.username, user.username);

        console.log(`  ✅ Updated ${updated} albums for ${user.username}`);

      } catch (err) {
        console.error(`  ❌ Failed to update ${user.username}:`, err.message);
      }

      // Small delay between users
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    console.log('\n✅ Auto-update cycle complete!');

  } catch (err) {
    console.error('❌ Auto-update failed:', err);
  }
}

// Start scheduler
function startScheduler() {
  if (updateInterval) {
    console.log('⚠️  Scheduler already running');
    return;
  }

  console.log('🤖 Starting auto-update scheduler (runs every 24 hours)');
  
  // Run immediately on start (optional)
  // autoUpdateUsers();
  
  // Run every 24 hours (86400000 ms)
  updateInterval = setInterval(autoUpdateUsers, 24 * 60 * 60 * 1000);
  schedulerEnabled = true;
}

// Stop scheduler
function stopScheduler() {
  if (updateInterval) {
    clearInterval(updateInterval);
    updateInterval = null;
    schedulerEnabled = false;
    console.log('🛑 Auto-update scheduler stopped');
  }
}

// Manual trigger endpoint
app.post('/api/scheduler/run', async (req, res) => {
  res.json({ message: 'Auto-update started in background' });
  autoUpdateUsers(); // Run async
});

// Get scheduler status
app.get('/api/scheduler/status', (req, res) => {
  res.json({
    enabled: schedulerEnabled,
    running: updateInterval !== null,
    interval: '24 hours',
    next_run: updateInterval ? 'Within 24 hours' : 'Not scheduled'
  });
});

// Start/stop scheduler endpoints
app.post('/api/scheduler/start', (req, res) => {
  startScheduler();
  res.json({ message: 'Scheduler started', enabled: true });
});

app.post('/api/scheduler/stop', (req, res) => {
  stopScheduler();
  res.json({ message: 'Scheduler stopped', enabled: false });
});

// Auto-start scheduler if enabled in environment
if (schedulerEnabled) {
  startScheduler();
}

// Start server
app.listen(PORT, () => {
  console.log(`\n🎵 Last.fm Top Albums API Server`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`\n📍 Server: http://localhost:${PORT}`);
  console.log(`🤖 Auto-update: ${schedulerEnabled ? '✅ ENABLED' : '❌ DISABLED'}`);
  
  console.log(`\n📝 CORE ENDPOINTS:`);
  console.log(`   • Full import:  /api/update?user=nikzx&full=true&limit=5000`);
  console.log(`   • Quick update: /api/update?user=nikzx`);
  console.log(`   • Top albums:   /api/top-albums?user=nikzx&year=2020`);
  console.log(`   • Cache stats:  /api/cache/stats?user=nikzx`);
  
  console.log(`\n🎯 ADVANCED FILTERS:`);
  console.log(`   • Date range:   /api/top-albums?user=nikzx&yearStart=2017&yearEnd=2021`);
  console.log(`   • Artist:       /api/top-albums?user=nikzx&artist=taylor`);
  console.log(`   • Top artists:  /api/top-artists?user=nikzx&limit=50`);
  console.log(`   • Artist deep:  /api/artist-albums?user=nikzx&artist=taylor`);
  console.log(`   • Decade view:  /api/decade-summary?user=nikzx`);
  
  console.log(`\n🤖 SCHEDULER CONTROLS:`);
  console.log(`   • Status:       GET  /api/scheduler/status`);
  console.log(`   • Start:        POST /api/scheduler/start`);
  console.log(`   • Stop:         POST /api/scheduler/stop`);
  console.log(`   • Run now:      POST /api/scheduler/run`);
  
  console.log(`\n⏱️  ESTIMATED TIMES:`);
  console.log(`   • 1000 albums:  ~20 minutes`);
  console.log(`   • 5000 albums:  ~100 minutes (1.7 hours)`);
  console.log(`   • Quick update: ~3-5 minutes`);
  
  console.log(`\n💡 TIP: Add AUTO_UPDATE=true to .env to enable auto-updates\n`);
});