import { DurableObject } from "cloudflare:workers";

// Rota /ws?room=CODIGO&role=host|join  -> encaminha para a sala (Durable Object)
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
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
