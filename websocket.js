"use strict";

const http = require("http");
const { URL } = require("url");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = Number(process.env.PORT) || 8080;
const rooms = new Map();
const clients = new WeakMap();
let userSequence = 1;

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());
  }
  return rooms.get(roomId);
}

function removeFromRoom(ws) {
  const meta = clients.get(ws);
  if (!meta) {
    return null;
  }

  const roomClients = rooms.get(meta.roomId);
  if (!roomClients) {
    return meta;
  }

  roomClients.delete(ws);
  if (roomClients.size === 0) {
    rooms.delete(meta.roomId);
  }

  return meta;
}

function broadcastToRoom(roomId, sender, payload) {
  const roomClients = rooms.get(roomId);
  if (!roomClients) {
    return;
  }

  const message = JSON.stringify(payload);
  for (const client of roomClients) {
    if (client === sender) {
      continue;
    }
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

function getMessageText(rawData) {
  const text = rawData.toString().trim();
  if (!text) {
    return "";
  }

  try {
    const parsed = JSON.parse(text);
    if (typeof parsed === "string") {
      return parsed.trim();
    }
    if (parsed && typeof parsed.message === "string") {
      return parsed.message.trim();
    }
    return "";
  } catch {
    return text;
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify(
        {
          app: "Room-based WebSocket Messaging",
          wsEndpoint:
            "ws://<host>:8080/?roomid=<room-id>&username=<your-name>",
          notes: [
            "roomid is required",
            "message is broadcast to everyone in the same room except sender",
          ],
        },
        null,
        2
      )
    );
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not found");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  const requestUrl = new URL(req.url, `http://${req.headers.host}`);
  const roomId = requestUrl.searchParams.get("roomid")?.trim();
  const usernameParam = requestUrl.searchParams.get("username")?.trim();

  if (!roomId) {
    ws.close(1008, "Missing required query parameter: roomid");
    return;
  }

  const username = usernameParam || `user-${userSequence++}`;
  const roomClients = getOrCreateRoom(roomId);
  roomClients.add(ws);

  clients.set(ws, { roomId, username });

  ws.send(
    JSON.stringify({
      type: "connected",
      roomId,
      username,
      message: `Connected to room "${roomId}" as "${username}"`,
    })
  );

  broadcastToRoom(roomId, ws, {
    type: "system",
    roomId,
    message: `${username} joined the room`,
  });

  ws.on("message", (data, isBinary) => {
    if (isBinary) {
      return;
    }

    const meta = clients.get(ws);
    if (!meta) {
      return;
    }

    const message = getMessageText(data);
    if (!message) {
      return;
    }

    broadcastToRoom(meta.roomId, ws, {
      type: "message",
      roomId: meta.roomId,
      from: meta.username,
      message,
      sentAt: new Date().toISOString(),
    });
  });

  ws.on("close", () => {
    const meta = removeFromRoom(ws);
    if (!meta) {
      return;
    }

    broadcastToRoom(meta.roomId, ws, {
      type: "system",
      roomId: meta.roomId,
      message: `${meta.username} left the room`,
    });
  });

  ws.on("error", () => {
    removeFromRoom(ws);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`WebSocket server running on http://0.0.0.0:${PORT}`);
});
