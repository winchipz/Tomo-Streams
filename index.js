const express = require('express');
const axios = require('axios');
const cors = require('cors');
const WebSocket = require('ws');
const path = require('path');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const TWITCH_CLIENT_ID = process.env.TWITCH_CLIENT_ID || 'v7w0wlmm9cb2y1zcfo50ys0z4mhzgx';
const TWITCH_CLIENT_SECRET = process.env.TWITCH_CLIENT_SECRET || '8xqfs957kx7o7st6mfqo2klbztyxc9';

let twitchToken = null, tokenExpiry = 0;

async function getTwitchToken() {
  if (twitchToken && Date.now() < tokenExpiry) return twitchToken;
  const res = await axios.post(`https://id.twitch.tv/oauth2/token?client_id=${TWITCH_CLIENT_ID}&client_secret=${TWITCH_CLIENT_SECRET}&grant_type=client_credentials`);
  twitchToken = res.data.access_token;
  tokenExpiry = Date.now() + (res.data.expires_in - 60) * 1000;
  return twitchToken;
}

async function twitchAPI(endpoint, params = {}) {
  const token = await getTwitchToken();
  const res = await axios.get(`https://api.twitch.tv/helix/${endpoint}`, {
    headers: { 'Client-ID': TWITCH_CLIENT_ID, 'Authorization': `Bearer ${token}` },
    params
  });
  return res.data;
}

// API routes
app.get('/api/streams', async (req, res) => {
  try {
    const { game_id, first = 30, after } = req.query;
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
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/games', async (req, res) => {
  try {
    const { first = 20 } = req.query;
    const data = await twitchAPI('games/top', { first });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/search', async (req, res) => {
  try {
    const data = await twitchAPI('search/channels', { query: req.query.q, live_only: true, first: 20 });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/stream/:username', async (req, res) => {
  try {
    const [streamData, userData] = await Promise.all([
      twitchAPI('streams', { user_login: req.params.username }),
      twitchAPI('users', { login: req.params.username })
    ]);
    res.json({ stream: streamData.data[0] || null, user: userData.data[0] || null });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/health', (req, res) => res.json({ status: 'ok', rooms: Object.keys(rooms).length }));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// WebSocket server
const PORT = process.env.PORT || 8080;
const server = app.listen(PORT, () => console.log(`✅ Tomo Stream running on port ${PORT}`));
const wss = new WebSocket.Server({ server });

// Room structure:
// { stream, clients: Map<ws, {name, color}>, queue: [{channel, addedBy}] }
const rooms = {};

function getRoom(roomId) {
  if (!rooms[roomId]) {
    rooms[roomId] = { stream: null, clients: new Map(), queue: [] };
  }
  return rooms[roomId];
}

function broadcastToRoom(roomId, msg, exclude = null) {
  const room = rooms[roomId];
  if (!room) return;
  const data = JSON.stringify(msg);
  room.clients.forEach((info, client) => {
    if (client !== exclude && client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}

function getRoomViewers(room) {
  const viewers = [];
  room.clients.forEach((info, ws) => {
    viewers.push({ name: info.name, color: info.color });
  });
  return viewers;
}

wss.on('connection', ws => {
  let roomId = null;
  let clientName = 'Viewer';
  let clientColor = 'av-0';

  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw);
      const room = roomId ? getRoom(roomId) : null;

      switch (msg.type) {

        case 'join': {
          roomId = msg.roomId;
          clientName = msg.name || 'Viewer';
          clientColor = msg.color || 'av-0';
          const r = getRoom(roomId);
          r.clients.set(ws, { name: clientName, color: clientColor });

          // Send current state to new joiner
          ws.send(JSON.stringify({
            type: 'joined',
            count: r.clients.size,
            stream: r.stream,
            queue: r.queue,
            viewers: getRoomViewers(r)
          }));

          // Notify others
          broadcastToRoom(roomId, {
            type: 'user_joined',
            count: r.clients.size,
            name: clientName,
            color: clientColor
          }, ws);
          break;
        }

        case 'change_stream': {
          if (!room) break;
          room.stream = msg.channel;
          broadcastToRoom(roomId, {
            type: 'stream_change',
            channel: msg.channel
          }, null);
          break;
        }

        case 'sync_pause': {
          if (!room) break;
          broadcastToRoom(roomId, { type: 'sync_pause', by: clientName }, ws);
          break;
        }

        case 'sync_play': {
          if (!room) break;
          broadcastToRoom(roomId, { type: 'sync_play', by: clientName }, ws);
          break;
        }

        case 'sync_skip': {
          if (!room) break;
          broadcastToRoom(roomId, {
            type: 'sync_skip',
            seconds: msg.seconds || 10,
            by: clientName
          }, ws);
          break;
        }

        case 'reaction': {
          if (!room) break;
          broadcastToRoom(roomId, {
            type: 'reaction',
            emoji: msg.emoji,
            x: msg.x,
            y: msg.y,
            by: clientName
          }, ws);
          break;
        }

        case 'chat': {
          if (!room) break;
          broadcastToRoom(roomId, {
            type: 'chat',
            name: clientName,
            color: clientColor,
            text: msg.text,
            ts: Date.now()
          }, ws);
          break;
        }

        case 'queue_add': {
          if (!room) break;
          room.queue.push({ channel: msg.channel, addedBy: clientName });
          broadcastToRoom(roomId, {
            type: 'queue_update',
            queue: room.queue
          }, null);
          break;
        }

        case 'queue_remove': {
          if (!room) break;
          room.queue.splice(msg.idx, 1);
          broadcastToRoom(roomId, {
            type: 'queue_update',
            queue: room.queue
          }, null);
          break;
        }
      }

    } catch(e) {
      console.error('WS error:', e.message);
    }
  });

  ws.on('close', () => {
    if (roomId && rooms[roomId]) {
      rooms[roomId].clients.delete(ws);
      const count = rooms[roomId].clients.size;
      broadcastToRoom(roomId, {
        type: 'user_left',
        count,
        name: clientName
      }, null);
      if (count === 0) {
        // Keep room alive for 5 min in case everyone reconnects
        setTimeout(() => {
          if (rooms[roomId]?.clients.size === 0) delete rooms[roomId];
        }, 300000);
      }
    }
  });

  ws.on('error', err => console.error('WS client error:', err.message));
});

// Cleanup empty rooms every 10 min
setInterval(() => {
  Object.keys(rooms).forEach(id => {
    if (rooms[id].clients.size === 0) delete rooms[id];
  });
}, 600000);
