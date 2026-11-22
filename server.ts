import express from 'express';
import http from 'http';
import path from 'path';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer as createViteServer } from 'vite';
import type { IncidentCategory, PublicRoomState } from './shared/types.ts';
import { GameManager } from './server/gameEngine.ts';
import {
  getIncidentReports,
  getLeaderboard,
  getMatchReportById,
  getMatchReports,
  getOrCreateProfile,
  grantBailout,
  createIncidentReport,
} from './server/storage.ts';

interface ClientMeta {
  ws: WebSocket;
  playerId: string | null;
  roomCode: string | null;
}

const PORT = Number(process.env.PORT) || 3000;

async function startServer() {
  const app = express();
  app.use(express.json());

  const server = http.createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });
  const clients = new Map<WebSocket, ClientMeta>();

  const broadcastRoomState = (roomCode: string, _genericState: PublicRoomState) => {
    const room = gameManager.getRoom(roomCode);
    if (!room) return;
    for (const meta of clients.values()) {
      if (meta.roomCode === roomCode && meta.ws.readyState === WebSocket.OPEN) {
        const personalized = gameManager.toPublicState(
          room,
          meta.playerId ?? undefined
        );
        meta.ws.send(
          JSON.stringify({ type: 'room:state', payload: personalized })
        );
      }
    }
  };

  const gameManager = new GameManager(broadcastRoomState);

  // REST APIs
  app.get('/api/health', (_req, res) => {
    res.json({ ok: true, timestamp: Date.now() });
  });

  app.post('/api/profile/sync', (req, res) => {
    const { playerId, name, avatarId } = req.body ?? {};
    if (!playerId || typeof playerId !== 'string') {
      res.status(400).json({ error: 'playerId kerak' });
      return;
    }
    const profile = getOrCreateProfile(playerId, name, avatarId);
    res.json({ profile });
  });

  app.post('/api/profile/bailout', (req, res) => {
    const { playerId } = req.body ?? {};
    if (!playerId || typeof playerId !== 'string') {
      res.status(400).json({ error: 'playerId kerak' });
      return;
    }
    const result = grantBailout(playerId);
    res.json(result);
  });

  app.get('/api/leaderboard', (req, res) => {
    const sortBy = String(req.query.sortBy || 'netProfit');
    const players = getLeaderboard(sortBy);
    res.json({ players });
  });

  app.get('/api/reports/matches', (req, res) => {
    const playerId = req.query.playerId ? String(req.query.playerId) : undefined;
    const reports = getMatchReports(playerId);
    res.json({ reports });
  });

  app.get('/api/reports/matches/:id', (req, res) => {
    const report = getMatchReportById(req.params.id);
    if (!report) {
      res.status(404).json({ error: 'Hisobot topilmadi' });
      return;
    }
    res.json({ report });
  });

  app.get('/api/reports/incidents', (_req, res) => {
    res.json({ incidents: getIncidentReports() });
  });

  app.post('/api/reports/incidents', (req, res) => {
    const {
      reporterId,
      reporterName,
      targetPlayerName,
      roomCode,
      matchId,
      category,
      description,
    } = req.body ?? {};
    if (!reporterId || !description || typeof description !== 'string') {
      res.status(400).json({ error: 'Hisobot matni kiritilishi shart.' });
      return;
    }
    const allowedCats: IncidentCategory[] = [
      'fair_play',
      'voice_abuse',
      'stalling',
      'betting_dispute',
      'technical_bug',
    ];
    const validCat: IncidentCategory = allowedCats.includes(category)
      ? category
      : 'fair_play';

    const report = createIncidentReport({
      reporterId: String(reporterId),
      reporterName: String(reporterName || "Noma'lum o'yinchi"),
      targetPlayerName: targetPlayerName ? String(targetPlayerName) : undefined,
      roomCode: roomCode ? String(roomCode) : undefined,
      matchId: matchId ? String(matchId) : undefined,
      category: validCat,
      description,
    });
    res.status(201).json({ incident: report });
  });

  app.get('/api/rooms/public', (_req, res) => {
    res.json({ rooms: gameManager.getPublicRooms() });
  });

  // WebSocket handling (Game + WebRTC Voice Chat Signaling)
  wss.on('connection', (ws) => {
    const meta: ClientMeta = { ws, playerId: null, roomCode: null };
    clients.set(ws, meta);

    ws.on('message', (raw) => {
      let msg: { type: string; payload?: Record<string, unknown> };
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }

      const sendError = (error: string) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'error', payload: { message: error } }));
        }
      };

      const p = msg.payload ?? {};

      switch (msg.type) {
        case 'auth:hello': {
          meta.playerId = String(p.playerId || '');
          if (meta.playerId) {
            const prof = getOrCreateProfile(
              meta.playerId,
              typeof p.name === 'string' ? p.name : undefined,
              typeof p.avatarId === 'string' ? p.avatarId : undefined
            );
            ws.send(JSON.stringify({ type: 'profile:sync', payload: prof }));
          }
          break;
        }
        case 'room:create': {
          const playerId = String(p.playerId || meta.playerId || '');
          const playerName = String(p.playerName || "O'yinchi");
          const avatarId = String(p.avatarId || 'skull');
          const mode = p.mode === 'bot' ? 'bot' : 'multiplayer';
          const result = gameManager.createRoom({
            playerId,
            playerName,
            avatarId,
            mode,
            settings: (p.settings as Record<string, unknown>) ?? {},
          });
          if (result.error || !result.room) {
            sendError(result.error || 'Xona yaratib bo‘lmadi.');
            return;
          }
          meta.playerId = playerId;
          meta.roomCode = result.room.code;
          ws.send(
            JSON.stringify({
              type: 'room:state',
              payload: gameManager.toPublicState(result.room, playerId),
            })
          );
          break;
        }
        case 'room:join': {
          const playerId = String(p.playerId || meta.playerId || '');
          const playerName = String(p.playerName || "O'yinchi");
          const avatarId = String(p.avatarId || 'skull');
          const code = String(p.code || '');
          const result = gameManager.joinRoom({
            code,
            playerId,
            playerName,
            avatarId,
          });
          if (result.error || !result.room) {
            sendError(result.error || 'Xonaga kirib bo‘lmadi.');
            return;
          }
          meta.playerId = playerId;
          meta.roomCode = result.room.code;
          ws.send(
            JSON.stringify({
              type: 'room:state',
              payload: gameManager.toPublicState(result.room, playerId),
            })
          );
          break;
        }
        case 'room:leave': {
          if (meta.roomCode && meta.playerId) {
            gameManager.leaveRoom(meta.roomCode, meta.playerId);
            meta.roomCode = null;
          }
          ws.send(JSON.stringify({ type: 'room:left' }));
          break;
        }
        case 'room:toggle_ready': {
          if (meta.roomCode && meta.playerId) {
            gameManager.toggleReady(meta.roomCode, meta.playerId);
          }
          break;
        }
        case 'room:update_settings': {
          if (meta.roomCode && meta.playerId) {
            const res = gameManager.updateSettings(
              meta.roomCode,
              meta.playerId,
              (p.settings as Record<string, unknown>) ?? {}
            );
            if (res.error) sendError(res.error);
          }
          break;
        }
        case 'room:start': {
          if (meta.roomCode && meta.playerId) {
            const res = gameManager.startMatch(meta.roomCode, meta.playerId);
            if (res.error) sendError(res.error);
          }
          break;
        }
        case 'room:chat': {
          if (meta.roomCode && meta.playerId && typeof p.text === 'string') {
            gameManager.addChatMessage(meta.roomCode, meta.playerId, p.text);
          }
          break;
        }
        case 'game:shoot': {
          if (meta.roomCode && meta.playerId && typeof p.targetId === 'string') {
            const res = gameManager.handleShoot(
              meta.roomCode,
              meta.playerId,
              p.targetId
            );
            if (res.error) sendError(res.error);
          }
          break;
        }
        case 'game:use_item': {
          if (meta.roomCode && meta.playerId) {
            const res = gameManager.handleUseItem(
              meta.roomCode,
              meta.playerId,
              Number(p.itemIndex),
              typeof p.targetId === 'string' ? p.targetId : undefined,
              p.stealItemIndex !== undefined ? Number(p.stealItemIndex) : undefined
            );
            if (res.error) sendError(res.error);
          }
          break;
        }
        case 'game:side_bet': {
          if (meta.roomCode && meta.playerId) {
            const prediction = p.prediction === 'live' ? 'live' : 'blank';
            const res = gameManager.handlePlaceSideBet(
              meta.roomCode,
              meta.playerId,
              prediction,
              Number(p.amount)
            );
            if (res.error) sendError(res.error);
          }
          break;
        }
        case 'game:gamble_crate': {
          if (meta.roomCode && meta.playerId) {
            const res = gameManager.handleGambleCrate(
              meta.roomCode,
              meta.playerId
            );
            if (res.error) sendError(res.error);
          }
          break;
        }
        case 'game:double_decision': {
          if (meta.roomCode && meta.playerId) {
            const decision = p.decision === 'double' ? 'double' : 'cash_out';
            const res = gameManager.handleDoubleDecision(
              meta.roomCode,
              meta.playerId,
              decision
            );
            if (res.error) sendError(res.error);
          }
          break;
        }
        case 'voice:state': {
          if (meta.roomCode && meta.playerId) {
            gameManager.updateVoiceState(meta.roomCode, meta.playerId, {
              voiceJoined:
                p.voiceJoined !== undefined ? Boolean(p.voiceJoined) : undefined,
              micMuted:
                p.micMuted !== undefined ? Boolean(p.micMuted) : undefined,
              speaking:
                p.speaking !== undefined ? Boolean(p.speaking) : undefined,
              deafened:
                p.deafened !== undefined ? Boolean(p.deafened) : undefined,
            });
          }
          break;
        }
        case 'voice:signal': {
          if (!meta.roomCode || !meta.playerId || !p.targetPeerId) return;
          const targetId = String(p.targetPeerId);
          for (const targetMeta of clients.values()) {
            if (
              targetMeta.roomCode === meta.roomCode &&
              targetMeta.playerId === targetId &&
              targetMeta.ws.readyState === WebSocket.OPEN
            ) {
              targetMeta.ws.send(
                JSON.stringify({
                  type: 'voice:signal',
                  payload: {
                    fromPeerId: meta.playerId,
                    signal: p.signal,
                  },
                })
              );
            }
          }
          break;
        }
      }
    });

    ws.on('close', () => {
      if (meta.playerId) {
        gameManager.markDisconnected(meta.playerId);
      }
      clients.delete(ws);
    });
  });

  // Vite middleware in dev, static dist in production
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.resolve(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`Buckshot Ruletka server listening on http://0.0.0.0:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error('Server startup failed:', err);
  process.exit(1);
});
