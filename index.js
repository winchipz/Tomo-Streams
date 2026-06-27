const express = require('express');
const axios = require('axios');
const cors = require('cors');
const WebSocket = require('ws');
const path = require('path');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

// Serve index.html from same folder
app.use(express.static(__dirname));

const TWITCH_CLIENT_ID = process.env.TWITCH_CLIENT_ID || 'v7w0wlmm9cb2y1zcfo50ys0z4mhzgx';
const TWITCH_CLIENT_SECRET = process.env.TWITCH_CLIENT_SECRET || '8xqfs957kx7o7st6mfqo2klbztyxc9';

let twitchToken = null;
let tokenExpiry = 0;

async function getTwitchToken() {
  if (twitchToken && Date.now() < tokenExpiry) return twitchToken;
  const res = await axios.post(
    `https://id.twitch.tv/oauth2/token?client_id=${TWITCH_CLIENT_ID}&client_secret=${TWITCH_CLIENT_SECRET}&grant_type=client_credentials`
  );
  twitchToken = res.data.access_token;
  tokenExpiry = Date.now() + (res.data.expires_in - 60) * 1000;
  return twitchToken;
}

async function twitchAPI(endpoint, params = {}) {
  const token = await getTwitchToken();
  const res = await axios.get(`https://api.twitch.tv/helix/${endpoint}`, {
    headers: {
      'Client-ID': TWITCH_CLIENT_ID,
      'Authorization': `Bearer ${token}`
    },
    params
  });
  return res.data;
}

app.get('/api/streams', async (req, res) => {
  try {
    const { game_id, first = 20, after } = req.query;
    const params = { first };
    if (game_id) params.game_id = game_id;
    if (after) params.after = after;
    const data = await twitchAPI('streams', params);
    if (data.data.length > 0) {
      const userIds = data.data.map(s => s.user_id);
      const users = await twitchAPI('users', { id: userIds });
      const userMap = {};
      users.data.forEach(u => userMap[u.id] = u.profile_image_url);
      data.data.forEach(s => s.profile_image = userMap[s.user_id] || '');
    }
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/games', async (req, res) => {
  try {
    const data = await twitchAPI('games/top', { first: 20 });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/search', async (req, res) => {
  try {
    const { q } = req.query;
    const data = await twitchAPI('search/channels', { query: q, live_only: true, first: 20 });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/stream/:username', async (req, res) => {
  try {
    const data = await twitchAPI('streams', { user_login: req.params.username });
    const user = await twitchAPI('users', { login: req.params.username });
    res.json({ stream: data.data[0] || null, user: user.data[0] || null });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`✅ Tomo Stream running on port ${PORT}`);
});

const wss = new WebSocket.Server({ server });
const rooms = {};

wss.on('connection', (ws) => {
  let currentRoom = null;

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'join') {
        currentRoom = msg.roomId;
        if (!rooms[currentRoom]) rooms[currentRoom] = { stream: null, clients: new Set() };
        rooms[currentRoom].clients.add(ws);
        if (rooms[currentRoom].stream) {
          ws.send(JSON.stringify({ type: 'stream_change', channel: rooms[currentRoom].stream }));
        }
        broadcast(currentRoom, { type: 'user_joined', count: rooms[currentRoom].clients.size }, ws);
        ws.send(JSON.stringify({ type: 'joined', count: rooms[currentRoom].clients.size }));
      }
      if (msg.type === 'change_stream' && currentRoom && rooms[currentRoom]) {
        rooms[currentRoom].stream = msg.channel;
        broadcast(currentRoom, { type: 'stream_change', channel: msg.channel }, null);
      }
      if (msg.type === 'reaction' && currentRoom) {
        broadcast(currentRoom, { type: 'reaction', emoji: msg.emoji }, ws);
      }
    } catch (e) {
      console.error('WS error:', e.message);
    }
  });

  ws.on('close', () => {
    if (currentRoom && rooms[currentRoom]) {
      rooms[currentRoom].clients.delete(ws);
      const count = rooms[currentRoom].clients.size;
      broadcast(currentRoom, { type: 'user_left', count }, null);
      if (count === 0) delete rooms[currentRoom];
    }
  });
});

function broadcast(roomId, msg, exclude) {
  if (!rooms[roomId]) return;
  const data = JSON.stringify(msg);
  rooms[roomId].clients.forEach(client => {
    if (client !== exclude && client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}
