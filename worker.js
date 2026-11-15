import { DurableObject } from "cloudflare:workers";

// Rota /ws?room=CODIGO&role=host|join  -> encaminha para a sala (Durable Object)
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === "/mm") {
      if (req.headers.get("Upgrade") !== "websocket") return new Response("websocket only", { status: 426 });
      return env.MM.get(env.MM.idFromName("main")).fetch(req);
    }
    if (u.pathname !== "/ws") return new Response("Fronteira server ok");
    if (req.headers.get("Upgrade") !== "websocket") return new Response("websocket only", { status: 426 });
    const room = (u.searchParams.get("room") || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 8);
    if (room.length < 3) return new Response("codigo invalido", { status: 400 });
    return env.ROOM.get(env.ROOM.idFromName(room)).fetch(req);
  },
};

// Uma sala = um host (roda a simulação) + até 8 convidados.
// Host -> todos os convidados (estado do jogo). Convidado -> host (comandos).
export class Room extends DurableObject {
  host() { return this.ctx.getWebSockets().find((s) => s.deserializeAttachment()?.host); }
  guests() { return this.ctx.getWebSockets().filter((s) => !s.deserializeAttachment()?.host); }

  async fetch(req) {
    const role = new URL(req.url).searchParams.get("role");
    const host = this.host();
    let att;
    if (role === "host") {
      if (host) return new Response("sala ja existe", { status: 409 });
      att = { host: true };
    } else {
      if (!host) return new Response("sala nao encontrada", { status: 404 });
      const used = new Set(this.guests().map((s) => s.deserializeAttachment().pid));
      let pid = 1; while (used.has(pid)) pid++;
      if (pid > 8) return new Response("sala cheia", { status: 403 });
      att = { host: false, pid };
    }
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment(att);
    if (att.host) server.send(JSON.stringify({ t: "hosting" }));
    else {
      server.send(JSON.stringify({ t: "welcome", pid: att.pid }));
      host.send(JSON.stringify({ t: "join", pid: att.pid }));
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, msg) {
    const a = ws.deserializeAttachment();
    if (a.host) { for (const g of this.guests()) { try { g.send(msg); } catch {} } return; }
    if (typeof msg !== "string" || msg.length > 2000) return;
    const h = this.host();
    if (h) h.send(JSON.stringify({ t: "cmd", pid: a.pid, d: msg }));
  }

  webSocketClose(ws) {
    const a = ws.deserializeAttachment();
    if (a?.host) { for (const g of this.guests()) { try { g.close(4000, "host saiu"); } catch {} } return; }
    const h = this.host();
    if (h) h.send(JSON.stringify({ t: "leave", pid: a?.pid }));
  }
  webSocketError(ws) { this.webSocketClose(ws); }
}

// ===== Matchmaking: uma sala aberta por modo; ao começar, outra é criada sozinha =====
const MODES = ["ffa", "team", "zombie"], MAXP = 8, WAIT = 40000, FULL = 5000;
export class Matchmaker extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.rooms = this.ctx.storage.kv.get("rooms") || {};
    for (const m of MODES) if (!this.rooms[m]) this.rooms[m] = this.fresh();
  }
  fresh() { return { code: Math.random().toString(36).slice(2, 7).toUpperCase(), startAt: 0 }; }
  save() { this.ctx.storage.kv.put("rooms", this.rooms); }
  members(m) {
    const code = this.rooms[m].code;
    return this.ctx.getWebSockets().map((ws) => ({ ws, a: ws.deserializeAttachment() || {} }))
      .filter((x) => x.a.mode === m && x.a.code === code).sort((x, y) => x.a.t - y.a.t);
  }
  async rearm() {
    const t = MODES.map((m) => this.rooms[m].startAt).filter(Boolean);
    if (t.length) await this.ctx.storage.setAlarm(Math.min(...t));
  }
  broadcast() {
    const rooms = {};
    for (const m of MODES) rooms[m] = { code: this.rooms[m].code, startAt: this.rooms[m].startAt, n: this.members(m).length, max: MAXP };
    const msg = JSON.stringify({ t: "state", now: Date.now(), rooms });
    for (const ws of this.ctx.getWebSockets()) { try { ws.send(msg); } catch {} }
  }
  async fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({});
    this.broadcast();
    return new Response(null, { status: 101, webSocket: client });
  }
  async webSocketMessage(ws, raw) {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const a = ws.deserializeAttachment() || {};
    if (m.a === "join" && !a.mode && MODES.includes(m.mode)) {
      const room = this.rooms[m.mode];
      if (this.members(m.mode).length >= MAXP) return;
      ws.serializeAttachment({ mode: m.mode, code: room.code, t: Date.now(), name: String(m.name || "Jogador").slice(0, 12) });
      const now = Date.now(), n = this.members(m.mode).length;
      if (!room.startAt) room.startAt = now + WAIT;
      if (n >= MAXP && room.startAt - now > FULL) room.startAt = now + FULL;
      this.save(); await this.rearm();
      ws.send(JSON.stringify({ t: "joined", mode: m.mode, code: room.code }));
      this.broadcast();
    } else if (m.a === "leave" && a.mode) await this.leave(ws);
  }
  async leave(ws) {
    const a = ws.deserializeAttachment() || {};
    ws.serializeAttachment({});
    if (a.mode && this.rooms[a.mode].code === a.code && this.members(a.mode).filter((x) => x.ws !== ws).length === 0) {
      this.rooms[a.mode].startAt = 0; this.save();
    }
    this.broadcast();
  }
  async webSocketClose(ws) { await this.leave(ws); }
  async webSocketError(ws) { await this.leave(ws); }
  async alarm() {
    const now = Date.now();
    for (const m of MODES) {
      const r = this.rooms[m];
      if (r.startAt && r.startAt <= now) {
        this.members(m).forEach((x, k) => {
          try { x.ws.send(JSON.stringify({ t: "go", mode: m, code: r.code, role: k ? "join" : "host" })); } catch {}
          x.ws.serializeAttachment({});
        });
        this.rooms[m] = this.fresh();
      }
    }
    this.save(); await this.rearm(); this.broadcast();
  }
}
