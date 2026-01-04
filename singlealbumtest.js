require('dotenv').config();
const Database = require('better-sqlite3');
const { getMusicBrainzData } = require('./index.js'); // import your function

const db = new Database('database.db');

(async () => {
  const username = 'nikzx';
  const albumName = 'Bob Dylan';
  const artistName = 'Bob Dylan';

  try {
    const mbData = await getMusicBrainzData(artistName, albumName);
    console.log('MusicBrainz result:', mbData);

    // Optional: update DB for this one album
    const existing = db.prepare(`
      SELECT playcount, lastfm_url, image_url FROM albums
      WHERE username = ? AND album_name = ? AND artist_name = ?
    `).get(username, albumName, artistName);

    db.prepare(`
      INSERT INTO albums (
        username, album_name, artist_name, playcount,
        release_year, musicbrainz_id, canonical_album, canonical_artist,
        image_url, lastfm_url, album_type, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(username, musicbrainz_id) 
      DO UPDATE SET 
        release_year = excluded.release_year,
        canonical_album = excluded.canonical_album,
        canonical_artist = excluded.canonical_artist,
        album_type = excluded.album_type,
        updated_at = CURRENT_TIMESTAMP
    `).run(
      username,
      albumName,
      artistName,
      existing?.playcount || 0,
      mbData.release_year,
      mbData.musicbrainz_id,
      mbData.canonical_name,
      mbData.canonical_artist,
      existing?.image_url || '',
      existing?.lastfm_url || '',
      mbData.type
    );

    console.log('✅ Album updated in DB!');
  } catch (err) {
    console.error('Error:', err);
  }
})();
