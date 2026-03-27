import express from "express";
import cors from "cors";
import bodyParser from "body-parser";
import helmet from "helmet";
import path from "path";
import { fileURLToPath } from "url";
import { TikTokLiveConnection, WebcastEvent, ControlEvent } from "tiktok-live-connector";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const PORT = process.env.PORT || 3000;
const CLEANUP_TIMEOUT = parseInt(process.env.CLEANUP_TIMEOUT) || 5 * 60 * 1000;
const MAX_RECONNECT_ATTEMPTS = 3;
const HEARTBEAT_INTERVAL = 30000;

const app = express();

app.use(helmet({
  contentSecurityPolicy: false,
  crossOriginEmbedderPolicy: false
}));
app.use(cors({
  origin: process.env.CORS_ORIGIN || "*",
  credentials: true
}));
app.use(bodyParser.json());

// Serve static files from /public
app.use(express.static(path.join(__dirname, "public")));

const rooms = new Map();

const stats = {
  totalConnections: 0,
  totalMessages: 0,
  activeRooms: () => rooms.size,
  startTime: Date.now()
};

const logger = {
  info: (username, message, data = {}) =>
    console.log(`[${new Date().toISOString()}] [${username}] INFO: ${message}`, Object.keys(data).length ? data : ""),
  error: (username, message, error = {}) =>
    console.error(`[${new Date().toISOString()}] [${username}] ERROR: ${message}`, error?.message || error),
  warn: (username, message, data = {}) =>
    console.warn(`[${new Date().toISOString()}] [${username}] WARN: ${message}`, Object.keys(data).length ? data : "")
};

// ── SSE HELPERS ──
function sseSend(res, event, data) {
  if (res.destroyed || res.writableEnded) return false;
  try {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
    return true;
  } catch (e) {
    return false;
  }
}

function broadcast(username, event, data) {
  const room = rooms.get(username);
  if (!room) return;
  stats.totalMessages++;
  const dead = [];
  for (const res of room.clients) {
    if (!sseSend(res, event, data)) dead.push(res);
  }
  dead.forEach(r => room.clients.delete(r));
}

function setupHeartbeat(username, res) {
  const hb = setInterval(() => {
    if (!sseSend(res, "heartbeat", { timestamp: Date.now() })) clearInterval(hb);
  }, HEARTBEAT_INTERVAL);
  res.on("close", () => clearInterval(hb));
  return hb;
}

function scheduleCleanup(username) {
  const room = rooms.get(username);
  if (!room) return;
  if (room.cleanupTimer) { clearTimeout(room.cleanupTimer); room.cleanupTimer = null; }
  if (room.clients.size === 0) {
    logger.info(username, `Scheduling cleanup in ${CLEANUP_TIMEOUT}ms`);
    room.cleanupTimer = setTimeout(async () => {
      try {
        if (room.connection?.isConnected) await room.connection.disconnect();
        rooms.delete(username);
        logger.info(username, "Room cleaned up");
      } catch (e) {
        logger.error(username, "Cleanup error", e);
      }
    }, CLEANUP_TIMEOUT);
  }
}

async function attemptReconnect(username, attempt = 1) {
  const room = rooms.get(username);
  if (!room || attempt > MAX_RECONNECT_ATTEMPTS) {
    if (room) {
      logger.error(username, "Max reconnect attempts reached");
      broadcast(username, "connection_failed", { message: "Yeniden bağlanılamadı" });
    }
    return false;
  }
  logger.info(username, `Reconnect attempt ${attempt}/${MAX_RECONNECT_ATTEMPTS}`);
  broadcast(username, "reconnecting", { attempt, maxAttempts: MAX_RECONNECT_ATTEMPTS });
  await new Promise(r => setTimeout(r, Math.min(1000 * 2 ** (attempt - 1), 30000)));
  try {
    const state = await room.connection.connect();
    room.state = state;
    room.lastActive = Date.now();
    room.reconnectAttempts = 0;
    logger.info(username, `Reconnected to roomId ${state.roomId}`);
    broadcast(username, "reconnected", { roomId: state.roomId, attempt });
    return true;
  } catch (err) {
    logger.error(username, `Reconnect attempt ${attempt} failed`, err);
    return attemptReconnect(username, attempt + 1);
  }
}

// ── CREATE TIKTOK CONNECTION ──
async function createConnection(username, options = {}) {
  if (rooms.has(username)) {
    rooms.get(username).lastActive = Date.now();
    return rooms.get(username);
  }

  logger.info(username, "Creating new connection");

  const connection = new TikTokLiveConnection(username, {
    enableExtendedGiftInfo: true,
    processInitialData: false,
    ...options
  });

  const roomObj = {
    connection,
    clients: new Set(),
    state: null,
    lastActive: Date.now(),
    cleanupTimer: null,
    reconnectAttempts: 0,
    stats: { chatMessages: 0, gifts: 0, likes: 0, joins: 0, follows: 0 }
  };

  rooms.set(username, roomObj);
  stats.totalConnections++;

  connection.connect()
    .then(state => {
      roomObj.state = state;
      roomObj.lastActive = Date.now();
      logger.info(username, `Connected to roomId ${state.roomId}`);
      broadcast(username, "connected", {
        roomId: state.roomId,
        roomInfo: state.roomInfo ?? null,
        timestamp: Date.now()
      });
    })
    .catch(err => {
      logger.error(username, "Initial connection failed", err);
      broadcast(username, "error", { message: err?.message ?? String(err), type: "connection_error" });
    });

  connection.on(ControlEvent.CONNECTED, s => {
    roomObj.state = s; roomObj.lastActive = Date.now();
  });

  connection.on(ControlEvent.DISCONNECTED, () => {
    logger.warn(username, "Disconnected");
    broadcast(username, "disconnected", { message: "Bağlantı kesildi", timestamp: Date.now() });
    if (roomObj.clients.size > 0) attemptReconnect(username);
    else scheduleCleanup(username);
  });

  connection.on(ControlEvent.ERROR, err => {
    logger.error(username, "Control error", err);
    broadcast(username, "error", { message: err?.message ?? String(err), type: "control_error" });
  });

  connection.on(ControlEvent.STREAM_END, () => {
    logger.info(username, "Stream ended");
    broadcast(username, "stream_ended", { message: "Yayın sona erdi", timestamp: Date.now() });
  });

  connection.on(WebcastEvent.CHAT, data => {
    roomObj.lastActive = Date.now();
    roomObj.stats.chatMessages++;
    broadcast(username, "chat", {
      type: "chat",
      user: { uniqueId: data.user?.uniqueId, nickname: data.user?.nickname, profilePictureUrl: data.user?.profilePictureUrl },
      comment: data.comment,
      timestamp: Date.now()
    });
  });

  connection.on(WebcastEvent.GIFT, data => {
    roomObj.lastActive = Date.now();
    roomObj.stats.gifts++;
    broadcast(username, "gift", {
      type: "gift",
      user: { uniqueId: data.user?.uniqueId, nickname: data.user?.nickname, profilePictureUrl: data.user?.profilePictureUrl },
      giftName: data.giftName,
      giftId: data.giftId,
      repeatCount: data.repeatCount,
      repeatEnd: data.repeatEnd,
      giftType: data.giftType,
      diamondCount: data.diamondCount,
      timestamp: Date.now()
    });
  });

  connection.on(WebcastEvent.MEMBER, data => {
    roomObj.lastActive = Date.now();
    roomObj.stats.joins++;
    broadcast(username, "member", {
      type: "member",
      user: { uniqueId: data.user?.uniqueId, nickname: data.user?.nickname, profilePictureUrl: data.user?.profilePictureUrl },
      timestamp: Date.now()
    });
  });

  connection.on(WebcastEvent.LIKE, data => {
    roomObj.lastActive = Date.now();
    roomObj.stats.likes += data.likeCount || 1;
    broadcast(username, "like", {
      type: "like",
      user: data.user ? { uniqueId: data.user.uniqueId, nickname: data.user.nickname } : null,
      likeCount: data.likeCount,
      totalLikeCount: data.totalLikeCount,
      timestamp: Date.now()
    });
  });

  connection.on(WebcastEvent.FOLLOW, data => {
    roomObj.lastActive = Date.now();
    roomObj.stats.follows++;
    broadcast(username, "follow", {
      type: "follow",
      user: { uniqueId: data.user?.uniqueId, nickname: data.user?.nickname, profilePictureUrl: data.user?.profilePictureUrl },
      timestamp: Date.now()
    });
  });

  connection.on(WebcastEvent.SHARE, data => {
    roomObj.lastActive = Date.now();
    broadcast(username, "share", {
      type: "share",
      user: data.user ? { uniqueId: data.user.uniqueId, nickname: data.user.nickname } : null,
      timestamp: Date.now()
    });
  });

  connection.on(WebcastEvent.ROOM_USER, data => {
    roomObj.lastActive = Date.now();
    broadcast(username, "room_user", {
      type: "room_user",
      viewerCount: data.viewerCount,
      topViewers: data.topViewers,
      timestamp: Date.now()
    });
  });

  return roomObj;
}

// ── REST API ──
app.post("/api/connect", async (req, res) => {
  const { username } = req.body || {};
  if (!username || typeof username !== "string")
    return res.status(400).json({ error: "username gerekli" });
  try {
    const roomObj = await createConnection(username);
    res.json({ success: true, message: "Bağlantı istendi", username, isConnected: roomObj.connection?.isConnected ?? false, clientCount: roomObj.clients.size });
  } catch (e) {
    logger.error(username, "Connection failed", e);
    res.status(500).json({ success: false, error: e?.message ?? String(e) });
  }
});

app.post("/api/disconnect", async (req, res) => {
  const { username } = req.body || {};
  if (!username) return res.status(400).json({ error: "username gerekli" });
  const room = rooms.get(username);
  if (!room) return res.status(404).json({ error: "Oda bulunamadı" });
  try {
    broadcast(username, "force_disconnect", { message: "Sunucu bağlantıyı kapattı" });
    for (const client of room.clients) { try { client.end(); } catch (e) {} }
    if (room.connection?.isConnected) await room.connection.disconnect();
    if (room.cleanupTimer) clearTimeout(room.cleanupTimer);
    rooms.delete(username);
    res.json({ success: true, message: "Bağlantı kesildi", username });
  } catch (e) {
    res.status(500).json({ success: false, error: e?.message ?? String(e) });
  }
});

app.get("/api/status/:username", (req, res) => {
  const room = rooms.get(req.params.username);
  if (!room) return res.status(404).json({ connected: false, message: "Oda bulunamadı" });
  res.json({
    connected: room.connection?.isConnected ?? false,
    clientCount: room.clients.size,
    roomId: room.state?.roomId ?? null,
    lastActive: room.lastActive,
    stats: room.stats,
    uptime: Date.now() - room.lastActive
  });
});

app.get("/api/rooms", (req, res) => {
  const roomList = Array.from(rooms.entries()).map(([username, room]) => ({
    username, connected: room.connection?.isConnected ?? false,
    clientCount: room.clients.size, roomId: room.state?.roomId ?? null,
    lastActive: room.lastActive, stats: room.stats
  }));
  res.json({ rooms: roomList, totalRooms: rooms.size, serverStats: { ...stats, activeRooms: stats.activeRooms(), uptime: Date.now() - stats.startTime } });
});

// ── SSE ENDPOINT ──
app.get("/events/:username", async (req, res) => {
  const username = req.params.username;
  if (!username) return res.status(400).json({ error: "username gerekli" });

  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders?.();

  sseSend(res, "connected", { message: "SSE bağlandı", username, timestamp: Date.now() });

  try {
    const roomObj = await createConnection(username);
    roomObj.clients.add(res);
    if (roomObj.cleanupTimer) { clearTimeout(roomObj.cleanupTimer); roomObj.cleanupTimer = null; }

    setupHeartbeat(username, res);

    if (roomObj.state) {
      sseSend(res, "room_state", {
        roomId: roomObj.state.roomId,
        roomInfo: roomObj.state.roomInfo,
        stats: roomObj.stats,
        timestamp: Date.now()
      });
    }

    req.on("close", () => {
      roomObj.clients.delete(res);
      logger.info(username, `SSE client ayrıldı. Kalan: ${roomObj.clients.size}`);
      if (roomObj.clients.size === 0) scheduleCleanup(username);
    });

    req.on("error", () => roomObj.clients.delete(res));
  } catch (err) {
    logger.error(username, "SSE setup failed", err);
    sseSend(res, "error", { message: err?.message ?? String(err), type: "setup_error" });
    res.end();
  }
});

// ── OVERLAY ROUTES ──
// These serve the overlay HTML pages from /public/overlay/
app.get("/overlay/:type", (req, res) => {
  const type = req.params.type;
  const allowed = ["chat", "gift", "leaderboard"];
  if (!allowed.includes(type)) return res.status(404).send("Overlay bulunamadı");
  res.sendFile(path.join(__dirname, "public", "overlay", `${type}.html`));
});

// ── HEALTH ──
app.get("/health", (req, res) => {
  res.json({ status: "healthy", uptime: Date.now() - stats.startTime, stats: { ...stats, activeRooms: stats.activeRooms(), memoryUsage: process.memoryUsage() } });
});

// ── ROOT ──
app.get("/api", (req, res) => {
  res.json({
    service: "TikPanel — TikTok Live Dashboard",
    version: "1.0.0",
    endpoints: {
      dashboard: "GET /",
      connect: "POST /api/connect",
      disconnect: "POST /api/disconnect",
      status: "GET /api/status/:username",
      rooms: "GET /api/rooms",
      events: "GET /events/:username",
      overlays: {
        chat: "GET /overlay/chat?user=USERNAME",
        gift: "GET /overlay/gift?user=USERNAME",
        leaderboard: "GET /overlay/leaderboard?user=USERNAME"
      },
      health: "GET /health"
    },
    activeRooms: Array.from(rooms.keys()),
    stats: { ...stats, activeRooms: stats.activeRooms() }
  });
});

// ── ERROR HANDLER ──
app.use((err, req, res, next) => {
  logger.error("app", "Unhandled error", err);
  res.status(500).json({ error: "Sunucu hatası", message: err?.message ?? String(err) });
});

// ── GRACEFUL SHUTDOWN ──
process.on("SIGTERM", async () => {
  logger.info("server", "SIGTERM alındı, kapatılıyor...");
  for (const [username, room] of rooms.entries()) {
    try {
      broadcast(username, "server_shutdown", { message: "Sunucu kapatılıyor" });
      if (room.connection?.isConnected) await room.connection.disconnect();
    } catch (e) {
      logger.error(username, "Shutdown error", e);
    }
  }
  process.exit(0);
});

// ── START ──
app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════╗
║TikPanel — Live Dashboard     .║
║Dashboard : https://conis.com/  .║
║API : http://localhost:${PORT}/api.║
║Overlay : /overlay/chat?user=USERNAME║
╚══════════════════════════════════════════════╝
  `);
});
