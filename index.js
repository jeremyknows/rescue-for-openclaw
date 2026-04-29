#!/usr/bin/env node
/**
 * Rescue — Admin Bot for OpenClaw (Discord + Telegram)
 *
 * Standalone bot (independent of the OpenClaw gateway) that provides
 * admin commands for managing agent sessions from Discord or Telegram.
 * Designed as a "rescue" tool — works even when the gateway is down.
 *
 * Commands:
 *   !reset              — Reset the agent session bound to this channel/chat
 *   !reset <agent>      — Reset agent in this channel (supports aliases)
 *   !reset <agent> all  — Reset ALL sessions for an agent everywhere
 *   !restart gateway    — Restart the OpenClaw gateway (clears provider cooldowns)
 *   !model <alias>      — Override model for this session
 *   !model show         — Show current model override
 *   !model default      — Clear model override
 *   !mute [agent]       — Require @mention for agent (Discord only)
 *   !unmute [agent]     — Let agent respond to all messages (Discord only)
 *   !swap               — Show current Anthropic auth profile order
 *   !swap swap          — Flip primary/fallback Anthropic profile
 *   !swap <name>        — Set a specific profile as primary (watson, jeremy)
 *   !wcc                — Show all wcc sessions (uptime, status)
 *   !wcc start          — Start Terminal (Claude Code + Discord channels)
 *   !wcc start <name>   — Start plain session (watson-cc-<name>)
 *   !wcc stop [name]    — Stop a session (default: terminal)
 *   !wcc stop all       — Stop all wcc sessions
 *   !wcc kill [name|all] — Force kill session + cleanup orphans + MCP cache
 *   !wcc restart        — Restart Terminal with fresh context (verified)
 *   !backup             — Snapshot the current gateway config
 *   !rollback           — Restore last known good config + restart gateway
 *   !rollback list      — Show recent config backups
 *   !handoff [agent]    — Write handoff → reset → breadcrumb (preserves context)
 *   !mc [start|stop|status] — Manage Mission Control dashboard
 *   !ki [start|stop|status] — Manage Knowledge Intake server (localhost:7420)
 *   !watson-graph [start|stop|status] — Manage Watson Knowledge Graph (localhost:4444)
 *   !gc                 — Session GC status (RSS, prune stats, archive stats)
 *   !gc run             — Trigger manual garbage collection now
 *   !help               — Display command help
 *
 * Background monitors (always running, emit to bus.jsonl):
 *   - Stall detector (gateway sessions + CC tmux)
 *   - Gateway watchdog (PID changes, crash-loop detection)
 *   - Session GC (prune, archive, 30-day TTL, RSS-based auto-restart)
 *
 * Environment (at least one platform required):
 *   DISCORD_BOT_TOKEN         — Discord bot token (also accepts DISCORD_ADMIN_BOT_TOKEN)
 *   DISCORD_ADMIN_USER_ID     — Discord user ID for auth
 *   TELEGRAM_BOT_TOKEN        — Telegram bot token from @BotFather
 *   TELEGRAM_ADMIN_USER_ID    — Telegram user ID for auth
 *
 * Environment (optional):
 *   OPENCLAW_DIR               — OpenClaw base directory (default: ~/.openclaw)
 *   RESCUE_PREFIX              — Command prefix for Discord (default: !)
 *   RESCUE_TELEGRAM_PREFIX     — Command prefix for Telegram (default: /)
 *   RESCUE_AGENT_ALIASES       — JSON map of aliases, e.g. {"watson":"main"}
 *   RESCUE_OPS_CHANNEL_ID      — Discord channel ID for system alerts
 *   RESCUE_OPS_TELEGRAM_CHAT   — Telegram chat ID for system alerts
 *   RESCUE_STALL_MINUTES       — Minutes before stall alert (default: 15)
 *   RESCUE_MAX_BACKUPS         — Max config backups to keep (default: 20)
 *   RESCUE_GATEWAY_PROCESS     — Gateway process name (default: openclaw-gateway)
 */

const { Client, GatewayIntentBits } = require("discord.js");
const { Bot } = require("grammy");
const fsp = require("fs/promises");
const path = require("path");
const os = require("os");
const { exec, execFile, spawn } = require("child_process");
const { promisify } = require("util");
const execPromise = promisify(exec);

// Hard guard: rescue is NOT an LLM. If @anthropic-ai/sdk ever lands in this
// process tree, fail loud at startup rather than wake up to a swap-triggered
// API call from a bot that's supposed to be substrate-only.
try {
  require.resolve("@anthropic-ai/sdk");
  throw new Error(
    "[rescue] @anthropic-ai/sdk resolved in module path — rescue must remain substrate-only. Aborting."
  );
} catch (err) {
  if (err.code !== "MODULE_NOT_FOUND") throw err;
}

// ---------------------------------------------------------------------------
// Config — all customizable via environment variables
// ---------------------------------------------------------------------------

const DISCORD_TOKEN =
  process.env.DISCORD_BOT_TOKEN || process.env.DISCORD_ADMIN_BOT_TOKEN;
const DISCORD_TERMINAL_TOKEN = process.env.DISCORD_TERMINAL_BOT_TOKEN;
const DISCORD_ADMIN_ID = process.env.DISCORD_ADMIN_USER_ID;
const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_ADMIN_ID = process.env.TELEGRAM_ADMIN_USER_ID;

const OPENCLAW_DIR =
  process.env.OPENCLAW_DIR || path.join(os.homedir(), ".openclaw");
const AGENTS_DIR = path.join(OPENCLAW_DIR, "agents");
const LOGS_DIR = path.join(OPENCLAW_DIR, "logs");
const AUDIT_LOG = path.join(LOGS_DIR, "rescue-bot-audit.jsonl");
const CONFIG_PATH = path.join(OPENCLAW_DIR, "openclaw.json");
const BACKUP_DIR = path.join(OPENCLAW_DIR, "backups");

// Substrate-readable websocket-health heartbeat. Touched whenever the Discord
// gateway is in READY state. External monitors (rescue-bot-roundtrip.sh) read
// the mtime to detect process death, event-loop hang, OR websocket drop —
// all three failure modes that "process running" alone cannot distinguish.
const WS_HEARTBEAT_FILE =
  process.env.RESCUE_WS_HEARTBEAT_FILE ||
  path.join(os.homedir(), "atlas/shared/state/rescue-bot-ws.heartbeat");
const WS_HEARTBEAT_INTERVAL_MS = 60 * 1000;
const DISCORD_PREFIX = process.env.RESCUE_PREFIX || "!";
const TELEGRAM_PREFIX = process.env.RESCUE_TELEGRAM_PREFIX || "/";
const OPS_CHANNEL_ID = process.env.RESCUE_OPS_CHANNEL_ID || null;
const OPS_TELEGRAM_CHAT = process.env.RESCUE_OPS_TELEGRAM_CHAT || null;
const MAX_BACKUPS = parseInt(process.env.RESCUE_MAX_BACKUPS) || 20;

// Agent aliases
let AGENT_ALIASES = {};
try {
  if (process.env.RESCUE_AGENT_ALIASES) {
    AGENT_ALIASES = JSON.parse(process.env.RESCUE_AGENT_ALIASES);
  }
} catch (err) {
  console.error("[rescue] Failed to parse RESCUE_AGENT_ALIASES:", err.message);
}

// Stall detection
const STALL_THRESHOLD_MS =
  (parseInt(process.env.RESCUE_STALL_MINUTES) || 15) * 60 * 1000;
const STALL_ACTIVE_WINDOW = 30 * 60 * 1000;
const STALL_CHECK_INTERVAL = 60 * 1000;
const STALL_ALERT_COOLDOWN = 60 * 60 * 1000;
const staleAlertTimes = new Map();

// Gateway watchdog
const WATCHDOG_INTERVAL = 60 * 1000; // 60s — halves subprocess cost; crash-loop detection over 5min windows doesn't need finer granularity
const WATCHDOG_CRASH_THRESHOLD = 3;
const WATCHDOG_CRASH_WINDOW = 5 * 60 * 1000;
const WATCHDOG_ALERT_COOLDOWN = 30 * 60 * 1000;
const GATEWAY_PROCESS_NAME =
  process.env.RESCUE_GATEWAY_PROCESS || "openclaw-gateway";
const watchdogState = {
  lastPid: null,
  restarts: [],
  lastAlertTime: 0,
  lastCheckTime: 0,
  status: "starting",
};

// Session GC (garbage collector)
const GC_INTERVAL = 30 * 60 * 1000; // 30 minutes
const GC_CRON_RUN_MAX_AGE_MS = 6 * 3600 * 1000; // 6h for ephemeral :run: sessions
const GC_ORPHAN_MIN_AGE_S = 3600; // 1h before archiving orphans
const GC_GATEWAY_RSS_RESTART_MB = 5000; // restart gateway if RSS exceeds this (raised from 2500 — was causing restarts every 30min on 24GB M4)
const GC_CONTEXT_ALERT_PCT = 85; // alert when session context usage >= this %
const GC_SESSION_COUNT_ALERT = 100; // alert when main agent has more sessions than this
const GC_ALERT_COOLDOWN_MS = 4 * 3600 * 1000; // 4h cooldown per session/count alert
const GC_HEALTH_DRY_RUN = false; // set true to log-only (no Discord alerts) for first 2 weeks
const GC_STATUS_FILE = "/tmp/openclaw-ops-status.json";
const CRON_JOBS_PATH = path.join(OPENCLAW_DIR, "cron", "jobs.json");
const gcState = {
  lastRun: 0,
  lastResult: null,
  totalPruned: 0,
  totalArchived: 0,
  gatewayRestarts: 0,
  consecutiveDeferrals: 0, // tracks how many GC cycles we've deferred restart due to in-flight jobs
  contextAlertTimes: {}, // sessionId → last alert timestamp (cooldown tracking)
  lastCountAlert: 0, // last session-count alert timestamp
};

// Require at least one platform
if (!DISCORD_TOKEN && !TELEGRAM_TOKEN) {
  console.error(
    "At least one platform is required: set DISCORD_BOT_TOKEN or TELEGRAM_BOT_TOKEN"
  );
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Messaging abstraction — unified interface for Discord and Telegram
// ---------------------------------------------------------------------------

/**
 * Unified message context that wraps both Discord messages and Telegram contexts.
 * All command handlers receive this instead of platform-specific objects.
 */
class MessageContext {
  /**
   * @param {object} opts
   * @param {string} opts.userId - Sender's user ID
   * @param {string} opts.chatId - Channel/chat ID
   * @param {"discord"|"telegram"} opts.platform
   * @param {string} opts.prefix - Command prefix for this platform
   * @param {function(string): Promise} opts.replyFn - Reply to the triggering message
   * @param {function(string): Promise} opts.sendFn - Send a new message to the same chat
   * @param {object} [opts.raw] - Original platform object (for platform-specific commands)
   */
  constructor({ userId, chatId, platform, prefix, replyFn, sendFn, raw }) {
    this.userId = userId;
    this.chatId = chatId;
    this.platform = platform;
    this.prefix = prefix;
    this._reply = replyFn;
    this._send = sendFn;
    this.raw = raw;
    this.maxMessageLength = platform === "telegram" ? 4096 : 2000;
  }

  /** Reply to the triggering message (auto-splits if too long). */
  async reply(text) {
    return safeSend(this, text, true);
  }

  /** Send a new message to the same chat (auto-splits if too long). */
  async send(text) {
    return safeSend(this, text, false);
  }
}

/** Send or reply with auto-splitting for message length limits. */
async function safeSend(ctx, text, isReply) {
  const max = ctx.maxMessageLength;

  if (text.length <= max) {
    return isReply ? ctx._reply(text) : ctx._send(text);
  }

  const chunks = [];
  let current = "";
  for (const line of text.split("\n")) {
    if (current.length + line.length + 1 > max) {
      if (current) chunks.push(current);
      // If a single line exceeds max, split it
      if (line.length > max) {
        for (let i = 0; i < line.length; i += max) {
          chunks.push(line.slice(i, i + max));
        }
        current = "";
        continue;
      }
      current = line;
    } else {
      current = current ? current + "\n" + line : line;
    }
  }
  if (current) chunks.push(current);

  // First chunk as reply, rest as follow-up sends
  if (isReply) {
    await ctx._reply(chunks[0]);
  } else {
    await ctx._send(chunks[0]);
  }
  for (let i = 1; i < chunks.length; i++) {
    await ctx._send(chunks[i]);
  }
}

/** Create a MessageContext from a Discord message. */
function fromDiscord(message) {
  return new MessageContext({
    userId: message.author.id,
    chatId: message.channel.id,
    platform: "discord",
    prefix: DISCORD_PREFIX,
    replyFn: (text) => message.reply(text),
    sendFn: (text) => message.channel.send(text),
    raw: message,
  });
}

/** Create a MessageContext from a grammY context. */
function fromTelegram(tgCtx) {
  return new MessageContext({
    userId: String(tgCtx.from?.id),
    chatId: String(tgCtx.chat?.id),
    platform: "telegram",
    prefix: TELEGRAM_PREFIX,
    replyFn: (text) => tgCtx.reply(text, { parse_mode: undefined }),
    sendFn: (text) =>
      tgCtx.api.sendMessage(tgCtx.chat.id, text, { parse_mode: undefined }),
    raw: tgCtx,
  });
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

const COOLDOWNS = {
  reset: 5000,
  status: 2000,
  help: 1000,
  restart: 15000,
  start: 10000,
  model: 3000,
  keys: 5000,
  mute: 10000,
  unmute: 10000,
  backup: 5000,
  rollback: 10000,
  watchdog: 2000,
  cron: 3000,
  handoff: 30000,
  mc: 5000,
  ki: 5000,
  gc: 5000,
  swap: 3000,
  ping: 1000,
  panic: 5000,
  resume: 2000,
  atlas: 3000,
  remote: 5000,
};
const lastCommandTime = new Map();

function checkCooldown(command) {
  const cooldown = COOLDOWNS[command] || 2000;
  const now = Date.now();
  const last = lastCommandTime.get(command) || 0;
  if (now - last < cooldown) return false;
  lastCommandTime.set(command, now);
  return true;
}

// ---------------------------------------------------------------------------
// Audit logging
// ---------------------------------------------------------------------------

async function auditLog(userId, command, args, channelId, result) {
  const entry = {
    timestamp: new Date().toISOString(),
    userId,
    command,
    args,
    channelId,
    result,
  };
  try {
    await fsp.mkdir(LOGS_DIR, { recursive: true });
    await fsp.appendFile(AUDIT_LOG, JSON.stringify(entry) + "\n");
  } catch (err) {
    console.error("[rescue] Failed to write audit log:", err.message);
  }
}

/** Send an alert to configured ops channels (Discord + Telegram). */
async function sendOpsAlert(text) {
  if (OPS_CHANNEL_ID && discordClient) {
    try {
      const channel = await discordClient.channels.fetch(OPS_CHANNEL_ID);
      if (channel?.isTextBased()) await channel.send(text);
    } catch (err) {
      console.error("[rescue] Ops alert (Discord) failed:", err.message);
    }
  }
  if (OPS_TELEGRAM_CHAT && telegramBot) {
    try {
      await telegramBot.api.sendMessage(OPS_TELEGRAM_CHAT, text);
    } catch (err) {
      console.error("[rescue] Ops alert (Telegram) failed:", err.message);
    }
  }
}

/** Emit an event to the OpenClaw bus via emit-event.sh.
 *  Non-blocking fire-and-forget — fails silently if bus is unavailable. */
function emitBusEvent(agent, type, message, data = {}, topic = "rescue") {
  const emitScript = path.join(process.env.HOME, ".openclaw/scripts/emit-event.sh");
  try {
    const proc = spawn(
      "bash",
      [emitScript, agent, type, message, JSON.stringify(data), topic],
      { stdio: "ignore", detached: true }
    );
    proc.on("error", (err) => {
      console.error("[rescue] Bus emit failed:", err.message);
    });
    proc.unref();
  } catch (err) {
    console.error("[rescue] Bus emit spawn failed:", err.message);
  }
}

// ---------------------------------------------------------------------------
// File locking (mkdir-based, no extra dependencies)
// ---------------------------------------------------------------------------

const LOCK_TIMEOUT = 5000;
const LOCK_RETRY_MS = 50;

async function acquireLock(sessionsFile) {
  const lockDir = sessionsFile + ".lock";
  const deadline = Date.now() + LOCK_TIMEOUT;
  while (Date.now() < deadline) {
    try {
      await fsp.mkdir(lockDir);
      return lockDir;
    } catch {
      await new Promise((r) => setTimeout(r, LOCK_RETRY_MS));
    }
  }
  try {
    const stat = await fsp.stat(lockDir);
    if (Date.now() - stat.mtimeMs > 30000) {
      await fsp.rmdir(lockDir);
      await fsp.mkdir(lockDir);
      return lockDir;
    }
  } catch {
    // Lock removed by another process
  }
  throw new Error("Could not acquire lock on sessions.json (timeout)");
}

async function releaseLock(lockDir) {
  try {
    await fsp.rmdir(lockDir);
  } catch {
    // Already released
  }
}

// ---------------------------------------------------------------------------
// Input validation
// ---------------------------------------------------------------------------

function isValidAgentId(id) {
  return (
    /^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$/.test(id) && id.length <= 64
  );
}

async function listAgentIds() {
  try {
    const entries = await fsp.readdir(AGENTS_DIR, { withFileTypes: true });
    return entries.filter((e) => e.isDirectory()).map((e) => e.name);
  } catch {
    return [];
  }
}

// ---------------------------------------------------------------------------
// Agent name helpers
// ---------------------------------------------------------------------------

function agentName(agentId) {
  return agentId
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

async function resolveAgentId(input) {
  const lower = input.toLowerCase();
  if (AGENT_ALIASES[lower]) return AGENT_ALIASES[lower];
  const agents = await listAgentIds();
  if (agents.includes(lower)) return lower;
  return null;
}

function agentDisplayName(agentId) {
  for (const [alias, id] of Object.entries(AGENT_ALIASES)) {
    if (id === agentId) return `${agentName(agentId)} (${alias})`;
  }
  return agentName(agentId);
}

async function formatAgentList() {
  const agents = await listAgentIds();
  const parts = [];
  for (const id of agents.sort()) {
    const aliases = Object.entries(AGENT_ALIASES)
      .filter(([, v]) => v === id)
      .map(([k]) => k);
    if (aliases.length > 0) {
      parts.push(`\`${id}\` (or \`${aliases.join("`, `")}\`)`);
    } else {
      parts.push(`\`${id}\``);
    }
  }
  return parts.join(", ");
}

// ---------------------------------------------------------------------------
// Session helpers
// ---------------------------------------------------------------------------

async function readSessionsJson(agentId) {
  if (!isValidAgentId(agentId)) return null;
  const filePath = path.join(AGENTS_DIR, agentId, "sessions", "sessions.json");
  try {
    const raw = await fsp.readFile(filePath, "utf-8");
    return { data: JSON.parse(raw), filePath };
  } catch {
    return null;
  }
}

/** Find the session bound to a chat — searches for both discord and telegram patterns. */
async function findSessionByChat(chatId, platform) {
  const agents = await listAgentIds();

  // Build search patterns for the platform
  const patterns = [];
  if (platform === "discord") {
    patterns.push(`discord:channel:${chatId}`);
  } else if (platform === "telegram") {
    patterns.push(`telegram:chat:${chatId}`);
    patterns.push(`telegram:channel:${chatId}`);
    // Some setups may use just the numeric chat ID
    patterns.push(`:${chatId}`);
  } else {
    // Unknown platform — search broadly
    patterns.push(chatId);
  }

  for (const agentId of agents) {
    const result = await readSessionsJson(agentId);
    if (!result) continue;
    for (const [key, entry] of Object.entries(result.data)) {
      for (const pattern of patterns) {
        if (key.endsWith(pattern) || key.includes(pattern + ":")) {
          return { agentId, key, entry, sessionsFile: result.filePath };
        }
      }
    }
  }
  return null;
}

async function findSessionsByAgent(agentId) {
  const result = await readSessionsJson(agentId);
  if (!result) return [];
  return Object.entries(result.data).map(([key, entry]) => ({
    agentId,
    key,
    entry,
    sessionsFile: result.filePath,
  }));
}

async function resetSession({ agentId, key, entry, sessionsFile }) {
  const sessionsDir = path.dirname(sessionsFile);
  const timestamp = Date.now();
  const lockDir = await acquireLock(sessionsFile);

  try {
    if (entry.sessionId) {
      const jsonlPath = path.join(sessionsDir, `${entry.sessionId}.jsonl`);
      const backupPath = path.join(
        sessionsDir,
        `${entry.sessionId}.deleted.${timestamp}.jsonl`
      );
      try {
        await fsp.rename(jsonlPath, backupPath);
      } catch {
        // Transcript may not exist
      }
    }

    const freshRaw = await fsp.readFile(sessionsFile, "utf-8");
    const data = JSON.parse(freshRaw);
    delete data[key];
    const tmpFile = sessionsFile + ".tmp";
    await fsp.writeFile(tmpFile, JSON.stringify(data, null, 2));
    await fsp.rename(tmpFile, sessionsFile);
  } finally {
    await releaseLock(lockDir);
  }

  return true;
}

// Known context windows per model — prevents false alerts when gateway
// doesn't report contextTokens (e.g., GPT-5.4 showed 116% without this).
const KNOWN_CONTEXT_WINDOWS = {
  "anthropic/claude-sonnet-4-6": 1000000,
  "anthropic/claude-opus-4-6": 1000000,
  "anthropic/claude-haiku-4-5": 200000,
  "openai-codex/gpt-5.4": 128000,
  "ollama/qwen3.5:9b": 131072,
};

function getUsageInfo(entry) {
  const total = entry.totalTokens || 0;
  const modelKey = entry.modelProvider
    ? `${entry.modelProvider}/${entry.model}`
    : entry.model;
  const context =
    KNOWN_CONTEXT_WINDOWS[modelKey] || entry.contextTokens || 200000;
  const pct = context > 0 ? Math.round((total / context) * 100) : 0;

  let status = "healthy";
  if (pct >= 100) status = "full";
  else if (pct >= 90) status = "critical";
  else if (pct >= 70) status = "warning";

  const emoji = {
    healthy: "\u{1F7E2}",
    warning: "\u{1F7E1}",
    critical: "\u{1F7E0}",
    full: "\u{1F534}",
  };
  return { total, context, pct, status, emoji: emoji[status] };
}

// ---------------------------------------------------------------------------
// !ping — round-trip self-test (Tier 1 read-only safety)
// ---------------------------------------------------------------------------

function formatUptimeShort(seconds) {
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

async function handlePing(ctx) {
  const startedAt = Date.now();
  const uptimeStr = formatUptimeShort(process.uptime());
  let version = "unknown";
  try {
    version = require("./package.json").version || "unknown";
  } catch {}

  let wsLabel = "n/a";
  if (ctx.platform === "discord" && discordClient) {
    // discord.js Status enum: READY = 0
    const status = discordClient.ws?.status;
    const ping = discordClient.ws?.ping;
    wsLabel =
      status === 0
        ? `READY (${typeof ping === "number" && ping >= 0 ? Math.round(ping) + "ms" : "—"})`
        : `status=${status}`;
  }

  const reply = `🟢 rescue alive · v${version} · uptime ${uptimeStr} · ws ${wsLabel}`;
  await ctx.reply(reply);
  await auditLog(
    ctx.userId,
    "ping",
    [],
    ctx.chatId,
    `ws=${discordClient?.ws?.status ?? "na"} handler_ms=${Date.now() - startedAt}`
  );
}

// ---------------------------------------------------------------------------
// React-✅-to-confirm helper (Tier 2 destructive ops)
// ---------------------------------------------------------------------------
//
// Reused by !panic (T1.3), !restart (T1.4), !swap (T1.5). Never type-to-confirm
// — iOS autocorrect kills typed confirmations. Reactions are immune to that
// failure mode and are one tap from any device.
//
// Returns one of: { outcome: "confirmed"|"cancelled"|"timeout"|"react-failed" }.

async function awaitReactConfirm(replyMessage, opts = {}) {
  const { adminId, timeoutMs = 60_000 } = opts;

  if (!replyMessage || typeof replyMessage.react !== "function") {
    return { outcome: "react-failed", error: "reply message is not reactable" };
  }

  try {
    await replyMessage.react("✅");
    await replyMessage.react("❌");
  } catch (err) {
    return { outcome: "react-failed", error: err.message };
  }

  const filter = (reaction, user) =>
    user.id === adminId && ["✅", "❌"].includes(reaction.emoji.name);

  try {
    const collected = await replyMessage.awaitReactions({
      filter,
      max: 1,
      time: timeoutMs,
      errors: ["time"],
    });
    const reaction = collected.first();
    return {
      outcome: reaction?.emoji?.name === "✅" ? "confirmed" : "cancelled",
    };
  } catch {
    return { outcome: "timeout" };
  }
}

// ---------------------------------------------------------------------------
// !panic / !resume — silence all autonomous Discord posting (Tier 1.3)
// ---------------------------------------------------------------------------
//
// Tier 2 react-✅ for !panic (destructive: silences cron-driven posters).
// Tier 1 no-confirm for !resume (recovery path must be friction-free).
//
// Mechanism: presence of ~/atlas/shared/state/discord-quiet.flag (TTL 24h).
// All cron-driven Discord posters call discord-quiet-check.sh and exit silent
// if the flag is set. Direct replies bypass via DISCORD_QUIET_BYPASS=1.

const DISCORD_QUIET_FLAG = path.join(
  os.homedir(),
  "atlas/shared/state/discord-quiet.flag"
);

async function handlePanic(ctx) {
  if (ctx.platform !== "discord") {
    return ctx.reply(
      "`!panic` is Discord-only per security spec. Touch the flag manually if needed: `touch ~/atlas/shared/state/discord-quiet.flag`"
    );
  }

  const replyText = [
    "🔇 **!panic** — silence ALL autonomous Discord posting",
    "",
    "Affected:",
    "• `discord-send-message.js` (gates every bot-token cron poster: cc-send, sub-agent-complete, workspace-watcher, …)",
    "• `overload-alert.js` (5-min API-overload alerter)",
    "",
    "Untouched: direct replies, rescue-bot itself, anything setting `DISCORD_QUIET_BYPASS=1`.",
    "Auto-recovery: flag expires 24h after set if `!resume` is missed.",
    "",
    "**React ✅ within 60s to confirm. React ❌ to cancel.**",
  ].join("\n");

  const sent = await ctx.reply(replyText);
  const result = await awaitReactConfirm(sent, {
    adminId: DISCORD_ADMIN_ID,
    timeoutMs: 60_000,
  });

  if (result.outcome === "confirmed") {
    try {
      await fsp.mkdir(path.dirname(DISCORD_QUIET_FLAG), { recursive: true });
      await fsp.writeFile(
        DISCORD_QUIET_FLAG,
        `panic set by ${ctx.userId} at ${new Date().toISOString()}\n`
      );
      await ctx.send(
        "✅ **panic active** — autonomous Discord posting silenced. `!resume` to lift."
      );
      await auditLog(ctx.userId, "panic", [], ctx.chatId, "set");
    } catch (err) {
      await ctx.send(`❌ failed to set panic flag: ${err.message}`);
      await auditLog(
        ctx.userId,
        "panic",
        [],
        ctx.chatId,
        `error: ${err.message}`
      );
    }
  } else if (result.outcome === "cancelled") {
    await ctx.send("cancelled — no flag set");
    await auditLog(ctx.userId, "panic", [], ctx.chatId, "cancelled");
  } else if (result.outcome === "timeout") {
    await ctx.send("⏱️ timed out (60s) — no flag set");
    await auditLog(ctx.userId, "panic", [], ctx.chatId, "timeout");
  } else {
    await ctx.send(
      `⚠️ react-confirm failed: ${result.error || result.outcome}. Touch the flag manually if needed.`
    );
    await auditLog(
      ctx.userId,
      "panic",
      [],
      ctx.chatId,
      `react-failed: ${result.error || ""}`
    );
  }
}

// ---------------------------------------------------------------------------
// !atlas — off-network snapshot of 5 atlas metrics (Tier 1 read-only)
// ---------------------------------------------------------------------------
//
// Designed for "Jeremy's on his phone, off the Mac Mini's network." Pure text
// reply, no UI dependency. Shells out to atlas-snapshot.sh which probes
// localhost atlas-os endpoints + bus.jsonl. Each metric is independent so a
// single slow source can't block the others. Substrate-only fallbacks for
// cron-error count + bus event flow keep the read partially useful even when
// atlas-os itself is down.

async function handleAtlas(ctx) {
  const t0 = Date.now();
  const SCRIPT = path.join(
    os.homedir(),
    "atlas/shared/scripts/dashboards/atlas-snapshot.sh"
  );
  try {
    const { stdout } = await execPromise(SCRIPT, { timeout: 8_000 });
    let body = stdout.trimEnd();
    if (body.length > 1900) body = body.slice(0, 1900) + "\n…(truncated)";
    await ctx.reply(body);
    await auditLog(
      ctx.userId,
      "atlas",
      [],
      ctx.chatId,
      `handler_ms=${Date.now() - t0}`
    );
  } catch (err) {
    await ctx.reply(`\u{274C} atlas-snapshot failed: ${err.message}`);
    await auditLog(
      ctx.userId,
      "atlas",
      [],
      ctx.chatId,
      `error: ${err.message}`
    );
  }
}

async function handleResume(ctx) {
  try {
    let removed = false;
    try {
      await fsp.unlink(DISCORD_QUIET_FLAG);
      removed = true;
    } catch (err) {
      if (err.code !== "ENOENT") throw err;
    }
    await ctx.reply(
      removed
        ? "✅ **resumed** — autonomous Discord posting unblocked"
        : "ℹ️ no panic flag was set — already resumed"
    );
    await auditLog(
      ctx.userId,
      "resume",
      [],
      ctx.chatId,
      removed ? "removed" : "noop"
    );
  } catch (err) {
    await ctx.reply(`❌ resume failed: ${err.message}`);
    await auditLog(
      ctx.userId,
      "resume",
      [],
      ctx.chatId,
      `error: ${err.message}`
    );
  }
}

// ---------------------------------------------------------------------------
// !status — substrate-only health snapshot (Tier 1 read-only)
// ---------------------------------------------------------------------------
//
// Per the Rescue Homecoming PRISM consensus (R2): produces useful output using
// ONLY launchctl + tmux + filesystem reads + the Discord client's own
// ws.status field. NEVER calls bus, NEVER calls gateway HTTP. The single HTTP
// call is a 1s-timeout localhost curl to atlas-os, which is itself the
// substrate this command is designed to report on.

async function handleStatus(ctx) {
  const t0 = Date.now();
  const sections = [];

  // --- launchd: com.watson.* + ai.* labels ---
  try {
    const { stdout } = await execPromise("launchctl list");
    const rows = stdout
      .split("\n")
      .slice(1)
      .filter(Boolean)
      .map((l) => {
        const parts = l.split(/\s+/);
        return {
          pid: parts[0],
          status: parseInt(parts[1], 10),
          label: parts.slice(2).join(" "),
        };
      })
      .filter((r) => /^(com\.watson\.|ai\.)/.test(r.label))
      .sort((a, b) => a.label.localeCompare(b.label));

    const running = rows.filter((r) => r.pid !== "-");
    const stopped = rows.filter((r) => r.pid === "-" && r.status === 0);
    const crashed = rows.filter((r) => r.pid === "-" && r.status !== 0);

    const fmt = (r) => {
      if (r.pid !== "-") return `🟢 ${r.label} (pid ${r.pid})`;
      if (r.status === 0) return `⚪ ${r.label} (stopped)`;
      if (r.status < 0) return `🔴 ${r.label} (sig ${-r.status})`;
      return `🔴 ${r.label} (exit ${r.status})`;
    };

    const lines = [
      `**launchd** ${running.length} up · ${stopped.length} stopped · ${crashed.length} crashed`,
      ...crashed.map(fmt),
      ...running.map(fmt),
      ...stopped.map(fmt),
    ];
    sections.push(lines.join("\n"));
  } catch (err) {
    sections.push(`**launchd** ❌ error: ${err.message}`);
  }

  // --- tmux watson-cc-* sessions ---
  try {
    const { stdout } = await execPromise(
      'tmux ls 2>/dev/null | grep "^watson-cc" || true'
    );
    const lines = stdout.split("\n").filter(Boolean);
    const out = [`**tmux** ${lines.length} watson-cc session(s)`];
    for (const l of lines) {
      const m = l.match(/^(\S+):\s+\d+\s+windows\s+\(created\s+([^)]+)\)(.*)$/);
      if (m) {
        const [, name, created, rest] = m;
        const attached = /attached/.test(rest) ? "attached" : "detached";
        const createdMs = new Date(created).getTime();
        const ageMin = isFinite(createdMs)
          ? Math.floor((Date.now() - createdMs) / 60000)
          : null;
        const ageStr =
          ageMin == null
            ? ""
            : ageMin >= 1440
              ? ` · ${Math.floor(ageMin / 1440)}d`
              : ageMin >= 60
                ? ` · ${Math.floor(ageMin / 60)}h ${ageMin % 60}m`
                : ` · ${ageMin}m`;
        out.push(`• ${name} · ${attached}${ageStr}`);
      } else {
        out.push(`• ${l}`);
      }
    }
    sections.push(out.join("\n"));
  } catch (err) {
    sections.push(`**tmux** ❌ error: ${err.message}`);
  }

  // --- Anthropic account marker + snapshot expiresAt hint ---
  try {
    const currentPath = path.join(os.homedir(), ".claude/accounts/.current");
    const current = (await fsp.readFile(currentPath, "utf8")).trim();
    let hint = "";
    try {
      const acct = JSON.parse(
        await fsp.readFile(
          path.join(os.homedir(), `.claude/accounts/${current}.json`),
          "utf8"
        )
      );
      const expiresAt = acct?.claudeAiOauth?.expiresAt;
      if (typeof expiresAt === "number") {
        const days = (expiresAt - Date.now()) / 86_400_000;
        const iso = new Date(expiresAt).toISOString().slice(0, 10);
        hint =
          days < 0
            ? ` · snapshot expiresAt=${iso} (${Math.abs(days).toFixed(1)}d ago — likely refreshed in-place)`
            : ` · snapshot expiresAt=${iso} (${days.toFixed(1)}d ahead)`;
      }
    } catch {
      // snapshot unreadable — fall through with current marker only
    }
    sections.push(`**cc-account** ${current}${hint}`);
  } catch (err) {
    sections.push(`**cc-account** ❌ error: ${err.message}`);
  }

  // --- atlas-os reachability (1s timeout, only meaningful from Mac Mini) ---
  try {
    await execPromise("curl -sf -o /dev/null -m 1 http://localhost:3000");
    sections.push("**atlas-os** UP (localhost:3000)");
  } catch {
    sections.push(
      "**atlas-os** DOWN (localhost:3000) — only valid from Mac Mini itself"
    );
  }

  // --- this rescue-bot's gateway state (parity with !ping) ---
  if (ctx.platform === "discord" && discordClient) {
    const status = discordClient.ws?.status;
    const ping = discordClient.ws?.ping;
    const pingStr =
      typeof ping === "number" && ping >= 0 ? ` (${Math.round(ping)}ms)` : "";
    sections.push(
      `**rescue ws** ${status === 0 ? "READY" : "status=" + status}${pingStr}`
    );
  }

  let reply = sections.join("\n\n");
  if (reply.length > 1900) reply = reply.slice(0, 1900) + "\n…(truncated)";
  await ctx.reply(reply);
  await auditLog(
    ctx.userId,
    "status",
    [],
    ctx.chatId,
    `handler_ms=${Date.now() - t0}`
  );
}

// ---------------------------------------------------------------------------
// !reset
// ---------------------------------------------------------------------------

async function handleReset(ctx, args) {
  const target = args[0];
  const allFlag = args[1]?.toLowerCase() === "all";

  if (target) {
    const agentId = await resolveAgentId(target);
    if (!agentId) {
      const list = await formatAgentList();
      return ctx.reply(`Agent \`${target}\` not found. Available: ${list}`);
    }

    if (allFlag) {
      const sessions = await findSessionsByAgent(agentId);
      if (sessions.length === 0) {
        await auditLog(ctx.userId, "reset", [agentId, "all"], ctx.chatId, "no_sessions");
        return ctx.reply(
          `No sessions found for **${agentDisplayName(agentId)}**.`
        );
      }
      for (const session of sessions) {
        await resetSession(session);
      }
      await auditLog(ctx.userId, "reset", [agentId, "all"], ctx.chatId, `reset_all_${sessions.length}`);
      return ctx.reply(
        `\u{1F504} Reset **all ${sessions.length}** sessions for **${agentDisplayName(agentId)}**. Send a message to start fresh.`
      );
    }

    const session = await findSessionByChat(ctx.chatId, ctx.platform);
    if (!session || session.agentId !== agentId) {
      await auditLog(ctx.userId, "reset", [agentId], ctx.chatId, "not_in_channel");
      return ctx.reply(
        `**${agentDisplayName(agentId)}** doesn't have a session in this channel.\n` +
          `Use \`${ctx.prefix}reset ${target} all\` to reset all their sessions everywhere.`
      );
    }
    const usage = getUsageInfo(session.entry);
    await resetSession(session);
    await auditLog(ctx.userId, "reset", [agentId], ctx.chatId, `reset_${usage.pct}pct`);
    return ctx.reply(
      `\u{1F504} **${agentDisplayName(agentId)}**'s session in this channel has been reset (was at ${usage.pct}% context). Send a message to start fresh.`
    );
  }

  const session = await findSessionByChat(ctx.chatId, ctx.platform);
  if (!session) {
    await auditLog(ctx.userId, "reset", [], ctx.chatId, "no_session");
    return ctx.reply(
      "No OpenClaw session found for this channel. Is an agent active here?"
    );
  }

  const name = agentDisplayName(session.agentId);
  const usage = getUsageInfo(session.entry);
  await resetSession(session);
  await auditLog(ctx.userId, "reset", [], ctx.chatId, `reset_${name}_${usage.pct}pct`);

  return ctx.reply(
    `\u{1F504} **${name}**'s session in this channel has been reset (was at ${usage.pct}% context). Send a message to start fresh.`
  );
}
// Per-agent restart dispatch table — unified model 2026-04-29:
//   method: "atlas"     — CC tmux agents. Kill tmux + run start-X.sh
//                         directly. Works regardless of plist load state.
//   method: "launchctl" — pure launchd daemons (no tmux session). The
//                         supervised process IS the program.
// rescue is HARD-REJECTED (R3 §6 self-suicide block).
const RESTART_DISPATCH = {
  terminal: {
    method: "atlas",
    cmdPath:
      "/Users/watson/projects/system-pipes/scripts/atlas/start-terminal.sh",
    cmdArgs: ["--detached"],
    tmuxSession: "watson-cc",
    label: "Terminal (CC tmux)",
  },
  dodo: {
    method: "atlas",
    cmdPath: "/Users/watson/projects/system-pipes/scripts/atlas/start-dodo.sh",
    cmdArgs: ["--detached"],
    tmuxSession: "watson-cc-dodo",
    label: "Dodo (CC tmux)",
  },
  librarian: {
    method: "atlas",
    cmdPath:
      "/Users/watson/projects/system-pipes/scripts/atlas/start-librarian.sh",
    cmdArgs: ["--detached"],
    tmuxSession: "watson-cc-librarian",
    label: "Librarian (CC tmux)",
  },
  dispatch: {
    method: "atlas",
    cmdPath:
      "/Users/watson/projects/system-pipes/scripts/atlas/start-dispatch.sh",
    cmdArgs: ["--detached"],
    tmuxSession: "watson-cc-dispatch",
    label: "Dispatch (CC tmux)",
  },
  producer: {
    method: "atlas",
    cmdPath:
      "/Users/watson/projects/system-pipes/scripts/atlas/start-producer.sh",
    cmdArgs: ["--detached"],
    tmuxSession: "watson-cc-producer",
    label: "Producer (CC tmux)",
  },
  augur: {
    method: "atlas",
    cmdPath:
      "/Users/watson/projects/system-pipes/scripts/atlas/start-augur.sh",
    cmdArgs: ["--detached"],
    tmuxSession: "watson-cc-augur",
    label: "Augur (CC tmux)",
  },
  "watson-bridge": {
    method: "launchctl",
    plist: "com.watson.watson-bridge",
    tmuxSession: null,
    label: "Atlas bridge (Watson session router; legacy plist label)",
  },
  "builder-bridge": {
    method: "launchctl",
    plist: "com.watson.builder-bridge",
    tmuxSession: null,
    label: "Builder bridge daemon",
  },
};

// Two captures 1.5s apart; if pane content changed -> active.
async function tmuxSessionActivity(session) {
  if (!session) return null;
  try {
    const cap1 = (
      await execPromise(
        `tmux capture-pane -p -t ${session} 2>/dev/null || true`
      )
    ).stdout;
    if (!cap1) return null;
    await new Promise((r) => setTimeout(r, 1500));
    const cap2 = (
      await execPromise(
        `tmux capture-pane -p -t ${session} 2>/dev/null || true`
      )
    ).stdout;
    return { active: cap1 !== cap2, exists: true };
  } catch {
    return null;
  }
}

async function handleRestart(ctx, args) {
  const target = args[0]?.toLowerCase();

  // Hard-reject: rescue can't kill itself via the surface that depends on it.
  if (target === "rescue" || target === "rescue-bot") {
    await auditLog(ctx.userId, "restart", [target], ctx.chatId, "self-blocked");
    return ctx.reply(
      "\u{274C} rescue cannot restart itself via Discord. The external watchdog (Tier 3.5) covers this failure mode.\n" +
        "Last resort: ssh in and run `launchctl kickstart -k gui/$UID/com.watson.rescue-bot`."
    );
  }

  // Agent dispatch path (T1.4) — handled BEFORE the gateway path so the table
  // takes precedence for agent names.
  if (target && target !== "gateway") {
    const entry = RESTART_DISPATCH[target];
    if (!entry) {
      return ctx.reply(
        `Usage: \`${ctx.prefix}restart <target>\`\n` +
          `Agents: ${Object.keys(RESTART_DISPATCH).join(", ")}\n` +
          `Also: gateway\n` +
          `Hard-rejected: rescue (self-restart blocked)`
      );
    }

    if (ctx.platform !== "discord") {
      return ctx.reply(
        `\`!restart ${target}\` requires react-confirm — Discord-only.`
      );
    }

    let activityLine = "";
    if (entry.tmuxSession) {
      const probe = await tmuxSessionActivity(entry.tmuxSession);
      if (probe == null) {
        activityLine = `Tmux \`${entry.tmuxSession}\`: not currently running (clean restart, no live context to lose)`;
      } else if (probe.active) {
        activityLine = `\u{26A0}️ Tmux \`${entry.tmuxSession}\`: **ACTIVE** (pane changed within 1.5s) -- restart will destroy live context.`;
      } else {
        activityLine = `Tmux \`${entry.tmuxSession}\`: idle (pane unchanged for 1.5s).`;
      }
    } else {
      activityLine = "Daemon (no tmux pane).";
    }

    const methodLine =
      entry.method === "atlas"
        ? `Method: \`${entry.cmdPath} ${entry.cmdArgs.join(" ")}\``
        : `Method: \`launchctl kickstart -k gui/$UID/${entry.plist}\``;

    const replyText = [
      `\u{1F504} **!restart ${target}**`,
      "",
      `Target: ${entry.label}`,
      methodLine,
      activityLine,
      "",
      "**React \u{2705} within 60s to confirm. React \u{274C} to cancel.**",
    ].join("\n");

    const sent = await ctx.reply(replyText);
    const result = await awaitReactConfirm(sent, {
      adminId: DISCORD_ADMIN_ID,
      timeoutMs: 60_000,
    });

    if (result.outcome !== "confirmed") {
      const labels = {
        cancelled: "cancelled",
        timeout: "timed out (60s)",
        "react-failed": `react-confirm failed (${result.error || "unknown"})`,
      };
      await ctx.send(
        `${labels[result.outcome] || result.outcome} -- no restart`
      );
      await auditLog(
        ctx.userId,
        "restart",
        [target],
        ctx.chatId,
        result.outcome
      );
      return;
    }

    await auditLog(ctx.userId, "restart", [target], ctx.chatId, "executing");
    try {
      if (entry.method === "atlas") {
        // start-*.sh scripts are idempotent: if the tmux session already
        // exists, they exit 0 without doing anything. For an actual restart
        // we must kill the existing session first so the script creates a
        // fresh one. The tmux kill is part of what the user already
        // confirmed via react-✅.
        if (entry.tmuxSession) {
          await execPromise(
            `tmux kill-session -t "${entry.tmuxSession}" 2>/dev/null || true`,
            { timeout: 5_000 }
          );
        }
        await execPromise(
          `"${entry.cmdPath}" ${entry.cmdArgs.map((a) => `"${a}"`).join(" ")}`,
          { timeout: 30_000 }
        );
      } else if (entry.method === "launchctl") {
        // Same idempotency issue as the atlas method: launchd's tracked
        // process is start-*.sh which exits 0 after spawning the tmux
        // session. kickstart -k re-runs the script which sees the existing
        // session and exits 0 — the actual Claude inside the tmux session
        // is never killed. Kill the tmux first so the re-spawned script
        // creates a fresh session. Bridges have tmuxSession: null and
        // skip this — they're real launchd-supervised daemons.
        // War story: 2026-04-29 !restart dodo silently no-op'd because
        // this branch only ran kickstart -k.
        if (entry.tmuxSession) {
          await execPromise(
            `tmux kill-session -t "${entry.tmuxSession}" 2>/dev/null || true`,
            { timeout: 5_000 }
          );
        }
        const uid = process.getuid();
        await execPromise(
          `launchctl kickstart -k "gui/${uid}/${entry.plist}"`,
          { timeout: 15_000 }
        );
      }
      await ctx.send(
        `\u{2705} **${target}** restart issued. Verify with \`${ctx.prefix}status\` (give it ~5s to come up).`
      );
      await auditLog(ctx.userId, "restart", [target], ctx.chatId, "executed");
    } catch (err) {
      await ctx.send(`\u{274C} restart failed: ${err.message}`);
      await auditLog(
        ctx.userId,
        "restart",
        [target],
        ctx.chatId,
        `error: ${err.message}`
      );
    }
    return;
  }

  // Preserved gateway path (existing behavior).
  if (target !== "gateway") {
    return ctx.reply(`Usage: \`${ctx.prefix}restart gateway\``);
  }

  await ctx.reply(
    "\u{1F504} Restarting OpenClaw gateway (clears provider cooldowns)..."
  );
  await auditLog(ctx.userId, "restart", ["gateway"], ctx.chatId, "initiated");

  try {
    await stopGateway();
  } catch (err) {
    await ctx.send(`\u{274C} Could not kill gateway process: ${err.message}`);
    await auditLog(ctx.userId, "restart", ["gateway"], ctx.chatId, `error: ${err.message}`);
    return;
  }

  setTimeout(async () => {
    try {
      await fsp.access(CONFIG_PATH);
      await ctx.send(
        `\u{2705} Gateway killed \u2014 launchd should auto-restart it. Use \`${ctx.prefix}status all\` to verify.`
      );
      await auditLog(ctx.userId, "restart", ["gateway"], ctx.chatId, "success");
    } catch {
      await ctx.send(
        "\u{26A0}\uFE0F Gateway killed but config not found. Check your OpenClaw installation."
      );
      await auditLog(ctx.userId, "restart", ["gateway"], ctx.chatId, "config_missing");
    }
  }, 5000);
}

// ---------------------------------------------------------------------------
// !model — override the model for a session
// ---------------------------------------------------------------------------

async function loadModelAliases() {
  try {
    const raw = await fsp.readFile(CONFIG_PATH, "utf-8");
    const config = JSON.parse(raw);
    const models = config.agents?.defaults?.models || {};
    const aliases = {};
    for (const [fullName, modelConfig] of Object.entries(models)) {
      if (modelConfig.alias) {
        aliases[modelConfig.alias] = fullName;
      }
      const shorthand = fullName.split("/").pop();
      if (shorthand && !aliases[shorthand]) {
        aliases[shorthand] = fullName;
      }
    }
    return { aliases, models };
  } catch (err) {
    console.error("[rescue] Failed to load model aliases:", err.message);
    return { aliases: {}, models: {} };
  }
}

function splitModelName(fullName) {
  const slash = fullName.indexOf("/");
  if (slash === -1) return { provider: null, model: fullName };
  return {
    provider: fullName.slice(0, slash),
    model: fullName.slice(slash + 1),
  };
}

async function resolveModelAlias(fullName) {
  const { aliases } = await loadModelAliases();
  for (const [alias, name] of Object.entries(aliases)) {
    if (name === fullName) return alias;
  }
  return null;
}

async function handleModel(ctx, args) {
  const { aliases, models } = await loadModelAliases();

  if (args.length === 0) {
    const aliasLines = [];
    for (const [fullName, modelConfig] of Object.entries(models)) {
      if (!modelConfig.alias) continue;
      aliasLines.push(`  \`${modelConfig.alias}\` \u2192 \`${fullName}\``);
    }
    const list =
      aliasLines.length > 0
        ? `\n\n**Available models:**\n${aliasLines.join("\n")}`
        : "";
    return ctx.reply(
      `**Usage:** \`${ctx.prefix}model <alias|name>\` or \`${ctx.prefix}model show\` or \`${ctx.prefix}model default\`` +
        list
    );
  }

  const input = args[0].toLowerCase();

  // !model show
  if (input === "show" || input === "current") {
    const session = await findSessionByChat(ctx.chatId, ctx.platform);
    if (!session) {
      return ctx.reply("No OpenClaw session found for this channel.");
    }
    const override = session.entry.modelOverride;
    const providerOv = session.entry.providerOverride;
    if (!override) {
      return ctx.reply("No model override set \u2014 using agent default.");
    }
    const fullName = providerOv ? `${providerOv}/${override}` : override;
    const resolvedAlias = await resolveModelAlias(fullName);
    return ctx.reply(
      `Current override: \`${fullName}\`${resolvedAlias ? ` (${resolvedAlias})` : ""}\n` +
        `Use \`${ctx.prefix}model default\` to clear.`
    );
  }

  // !model default
  if (input === "default" || input === "clear" || input === "reset") {
    const session = await findSessionByChat(ctx.chatId, ctx.platform);
    if (!session) {
      return ctx.reply("No OpenClaw session found for this channel.");
    }
    try {
      const preCheck = await readSessionsJson(session.agentId);
      if (!preCheck || !preCheck.data[session.key]) {
        return ctx.reply("Session entry not found.");
      }
      const { filePath } = preCheck;
      const lock = await acquireLock(filePath);
      try {
        const freshRaw = await fsp.readFile(filePath, "utf-8");
        const data = JSON.parse(freshRaw);
        if (!data[session.key]) return ctx.reply("Session disappeared.");
        const old = data[session.key].modelOverride || "(none)";
        delete data[session.key].modelOverride;
        delete data[session.key].providerOverride;
        const tmpFile = filePath + ".tmp";
        await fsp.writeFile(tmpFile, JSON.stringify(data, null, 2));
        await fsp.rename(tmpFile, filePath);
        await ctx.reply(
          `Model override cleared (was: \`${old}\`). Using agent default on next message.`
        );
        await auditLog(ctx.userId, "model", ["default"], ctx.chatId, `${old} -> default`);
      } finally {
        await releaseLock(lock);
      }
    } catch (err) {
      await ctx.reply(`Error clearing override: ${err.message}`);
    }
    return;
  }

  // !model <name>
  const session = await findSessionByChat(ctx.chatId, ctx.platform);
  if (!session) {
    return ctx.reply(
      "No OpenClaw session found for this channel. Is an agent active here?"
    );
  }

  let modelName = aliases[input] || input;

  if (
    !models[modelName] &&
    !/^[a-z0-9][a-z0-9/_.-]*[a-z0-9]$/.test(modelName)
  ) {
    const suggestions = Object.entries(aliases)
      .filter(([alias]) => alias.includes(input.split("-")[0]))
      .slice(0, 3)
      .map(([alias]) => `\`${alias}\``)
      .join(", ");
    return ctx.reply(
      `Unknown model: \`${input}\`${suggestions ? `\nDid you mean: ${suggestions}?` : ""}`
    );
  }

  const { provider, model } = splitModelName(modelName);

  try {
    const preCheck = await readSessionsJson(session.agentId);
    if (!preCheck) {
      return ctx.reply(
        "Could not read session data. Try again in a moment."
      );
    }
    if (!preCheck.data[session.key]) {
      return ctx.reply(
        "Session entry not found. Try sending a message first to initialize the session."
      );
    }

    const { filePath } = preCheck;
    let oldModel = "(using default)";
    const lock = await acquireLock(filePath);
    try {
      const freshRaw = await fsp.readFile(filePath, "utf-8");
      const data = JSON.parse(freshRaw);
      if (!data[session.key]) {
        return ctx.reply("Session disappeared. Try again.");
      }

      const oldOverride = data[session.key].modelOverride;
      const oldProvider = data[session.key].providerOverride;
      oldModel = oldOverride
        ? oldProvider
          ? `${oldProvider}/${oldOverride}`
          : oldOverride
        : "(using default)";

      data[session.key].modelOverride = model;
      if (provider) {
        data[session.key].providerOverride = provider;
      } else {
        delete data[session.key].providerOverride;
      }

      const tmpFile = filePath + ".tmp";
      await fsp.writeFile(tmpFile, JSON.stringify(data, null, 2));
      await fsp.rename(tmpFile, filePath);
    } finally {
      await releaseLock(lock);
    }

    const resolvedAlias = await resolveModelAlias(modelName);
    const displayName = resolvedAlias
      ? `\`${modelName}\` (${resolvedAlias})`
      : `\`${modelName}\``;
    await ctx.reply(
      `\u{1F504} Model override set to ${displayName}\n` +
        `Previous: ${oldModel}\n` +
        `This will take effect on the next message.`
    );
    await auditLog(ctx.userId, "model", [modelName], ctx.chatId, `${oldModel} -> ${modelName}`);
  } catch (err) {
    console.error(`[rescue] !model error:`, err.message);
    await ctx.reply(`\u{274C} Error setting model: ${err.message}`);
    await auditLog(ctx.userId, "model", args, ctx.chatId, `error: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// !mute / !unmute (Discord only — modifies Discord-specific gateway config)
// ---------------------------------------------------------------------------

async function readConfig() {
  const raw = await fsp.readFile(CONFIG_PATH, "utf-8");
  return JSON.parse(raw);
}

async function writeConfig(config) {
  const tmpFile = CONFIG_PATH + ".tmp";
  await fsp.writeFile(tmpFile, JSON.stringify(config, null, 2));
  await fsp.rename(tmpFile, CONFIG_PATH);
}

function findDiscordAccount(config, agentId) {
  const defaultAgentId = config.agents?.defaults?.agentId || "main";
  if (agentId === defaultAgentId) return "default";
  const binding = (config.bindings || []).find(
    (b) => b.agentId === agentId && b.match?.channel === "discord"
  );
  return binding?.match?.accountId || null;
}

async function handleMute(ctx, args, mute) {
  // Discord only
  if (ctx.platform !== "discord") {
    return ctx.reply(
      `\`${ctx.prefix}${mute ? "mute" : "unmute"}\` is only available on Discord.`
    );
  }

  const channelId = ctx.chatId;
  const verb = mute ? "Mute" : "Unmute";
  const pastVerb = mute ? "Muted" : "Unmuted";

  let agentId;
  if (args[0]) {
    agentId = await resolveAgentId(args[0]);
    if (!agentId) {
      const list = await formatAgentList();
      return ctx.reply(
        `Agent \`${args[0]}\` not found. Available: ${list}`
      );
    }
  } else {
    const session = await findSessionByChat(channelId, ctx.platform);
    agentId = session?.agentId || "main";
  }

  const accountId = findDiscordAccount(await readConfig(), agentId);
  if (!accountId) {
    return ctx.reply(
      `Could not find Discord account for **${agentDisplayName(agentId)}**.`
    );
  }

  await ctx.reply(
    `\u{1F504} ${verb === "Mute" ? "Muting" : "Unmuting"} **${agentDisplayName(agentId)}** in this channel...`
  );
  await auditLog(ctx.userId, mute ? "mute" : "unmute", [agentId], channelId, "initiated");

  try {
    await createBackup("pre-" + (mute ? "mute" : "unmute"));

    await stopGateway();
    await new Promise((r) => setTimeout(r, 2000));

    const config = await readConfig();
    const account = config.channels?.discord?.accounts?.[accountId];
    if (!account) {
      return ctx.send(
        `\u{274C} Discord account \`${accountId}\` not found in config.`
      );
    }

    let found = false;
    for (const [, guild] of Object.entries(account.guilds || {})) {
      const channelConfig = guild.channels?.[channelId];
      if (channelConfig) {
        channelConfig.requireMention = mute;
        found = true;
        break;
      }
    }

    if (!found) {
      return ctx.send(
        `\u{274C} Channel \`${channelId}\` not configured for **${agentDisplayName(agentId)}**'s Discord account.`
      );
    }

    await writeConfig(config);

    setTimeout(async () => {
      try {
        await ctx.send(
          `\u{2705} **${pastVerb}** ${agentDisplayName(agentId)} in this channel. ` +
            `${mute ? "They now require an @mention to respond." : "They will respond to all messages."}`
        );
        await auditLog(ctx.userId, mute ? "mute" : "unmute", [agentId], channelId, "success");
      } catch {
        /* channel unavailable */
      }
    }, 6000);
  } catch (err) {
    console.error(
      `[rescue] !${mute ? "mute" : "unmute"} error:`,
      err.message
    );
    await ctx.send(`\u{274C} Error: ${err.message}`);
    await auditLog(ctx.userId, mute ? "mute" : "unmute", [agentId], channelId, `error: ${err.message}`);
  }
}
async function createBackup(label) {
  await fsp.mkdir(BACKUP_DIR, { recursive: true });
  const timestamp = new Date()
    .toISOString()
    .replace(/[:.]/g, "-")
    .replace("T", "_")
    .slice(0, 19);
  const filename = label
    ? `openclaw.json.${timestamp}.${label}`
    : `openclaw.json.${timestamp}`;
  const dest = path.join(BACKUP_DIR, filename);
  await fsp.copyFile(CONFIG_PATH, dest);

  const files = await fsp.readdir(BACKUP_DIR);
  const backups = files
    .filter((f) => f.startsWith("openclaw.json."))
    .sort()
    .reverse();
  for (const old of backups.slice(MAX_BACKUPS)) {
    await fsp.unlink(path.join(BACKUP_DIR, old)).catch(() => {});
  }

  return { filename, dest, total: Math.min(backups.length, MAX_BACKUPS) };
}

async function handleBackup(ctx) {
  try {
    await fsp.access(CONFIG_PATH);
  } catch {
    return ctx.reply(
      `\u{274C} Config not found at \`${CONFIG_PATH}\`. Is OpenClaw installed?`
    );
  }

  try {
    const raw = await fsp.readFile(CONFIG_PATH, "utf-8");
    JSON.parse(raw);

    const { filename, total } = await createBackup("manual");

    await ctx.reply(
      `\u{2705} Config backed up as \`${filename}\`\n` +
        `${total} backup${total !== 1 ? "s" : ""} stored in \`${BACKUP_DIR}/\``
    );
    await auditLog(ctx.userId, "backup", [], ctx.chatId, filename);
  } catch (err) {
    if (err instanceof SyntaxError) {
      return ctx.reply(
        `\u{26A0}\uFE0F Current config has invalid JSON \u2014 backing up anyway as a record.`
      );
    }
    await ctx.reply(`\u{274C} Backup failed: ${err.message}`);
    await auditLog(ctx.userId, "backup", [], ctx.chatId, `error: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// !rollback
// ---------------------------------------------------------------------------

async function listBackups() {
  try {
    const files = await fsp.readdir(BACKUP_DIR);
    return files
      .filter((f) => f.startsWith("openclaw.json."))
      .sort()
      .reverse();
  } catch {
    return [];
  }
}

async function handleRollback(ctx, args) {
  const sub = args[0]?.toLowerCase();
  const backups = await listBackups();

  if (backups.length === 0) {
    return ctx.reply(
      `No backups found in \`${BACKUP_DIR}/\`.\n` +
        `Use \`${ctx.prefix}backup\` to create one first.`
    );
  }

  if (sub === "list") {
    const recent = backups.slice(0, 10);
    const lines = recent.map((f, i) => {
      const label = i === 0 ? " **(latest)**" : "";
      return `  \`${f}\`${label}`;
    });
    await auditLog(ctx.userId, "rollback", ["list"], ctx.chatId, `${backups.length}_backups`);
    return ctx.reply(
      `\u{1F4CB} **${backups.length} backups available:**\n${lines.join("\n")}\n\n` +
        `Use \`${ctx.prefix}rollback\` to restore the latest, or \`${ctx.prefix}rollback <filename>\` for a specific one.`
    );
  }

  let targetFile;
  if (!sub) {
    targetFile = backups[0];
  } else {
    targetFile = backups.find((f) => f === sub || f.includes(sub));
    if (!targetFile) {
      return ctx.reply(
        `Backup \`${sub}\` not found. Use \`${ctx.prefix}rollback list\` to see available backups.`
      );
    }
  }

  const backupPath = path.join(BACKUP_DIR, targetFile);

  // Schema validation: parse JSON and verify required top-level structure
  try {
    const raw = await fsp.readFile(backupPath, "utf-8");
    const parsed = JSON.parse(raw);
    const missing = [];
    if (!parsed.agents || !Array.isArray(parsed.agents.list)) missing.push("agents.list");
    if (!parsed.auth?.order?.anthropic) missing.push("auth.order.anthropic");
    if (!parsed.gateway) missing.push("gateway");
    if (missing.length > 0) {
      return ctx.reply(
        `\u{274C} Backup \`${targetFile}\` is structurally invalid. Missing: \`${missing.join(", ")}\`.\n` +
        `This backup may be from an incompatible config version. Try a different backup.`
      );
    }
  } catch (err) {
    return ctx.reply(
      `\u{274C} Backup \`${targetFile}\` contains invalid JSON: ${err.message}`
    );
  }

  await ctx.reply(
    `\u{1F504} Rolling back to \`${targetFile}\`...\n` +
      `1. Backing up current config\n` +
      `2. Stopping gateway\n` +
      `3. Restoring backup\n` +
      `4. Restarting gateway`
  );

  try {
    await createBackup("pre-rollback");

    await stopGateway();
    await new Promise((r) => setTimeout(r, 2000));

    const tmpFile = CONFIG_PATH + ".tmp";
    await fsp.copyFile(backupPath, tmpFile);
    await fsp.rename(tmpFile, CONFIG_PATH);

    setTimeout(async () => {
      try {
        const raw = await fsp.readFile(CONFIG_PATH, "utf-8");
        const config = JSON.parse(raw);
        const agentCount = Array.isArray(config.agents?.list) ? config.agents.list.length : 0;
        await ctx.send(
          `\u{2705} **Rollback complete!** Restored \`${targetFile}\`\n` +
            `Config has ${agentCount} agent${agentCount !== 1 ? "s" : ""} configured. ` +
            `Gateway should auto-restart via launchd.`
        );
        await auditLog(ctx.userId, "rollback", [targetFile], ctx.chatId, "success");
      } catch (err) {
        await ctx.send(
          `\u{26A0}\uFE0F Config restored but verification failed: ${err.message}\n` +
            `The gateway may need manual attention.`
        );
        await auditLog(ctx.userId, "rollback", [targetFile], ctx.chatId, `verify_error: ${err.message}`);
      }
    }, 6000);
  } catch (err) {
    console.error("[rescue] !rollback error:", err.message);
    await ctx.send(`\u{274C} Rollback failed: ${err.message}`);
    await auditLog(ctx.userId, "rollback", [targetFile], ctx.chatId, `error: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// !swap — Swap Anthropic auth profile order
// ---------------------------------------------------------------------------

// !swap — switch the active cc-account snapshot (Tier 2 react-confirm).
//
// The phased plan's auto-restart-bridges + per-daemon probe + auto-rollback
// machinery is not built here. An audit found every long-lived consumer on
// this Mac Mini is env-pinned to CLAUDE_CODE_OAUTH_TOKEN — bridges via plist
// EnvironmentVariables, CC tmux sessions via per-agent claude-<agent>-oauth.env
// files. Bridges spawn `claude` with `...process.env`, so the child inherits
// the plist token and never reads credentials.json. Same for tmux daemons
// via `tmux -e`. !swap therefore only changes which account a fresh
// interactive `claude` (no env override) authenticates as. The rollback
// machinery solves a problem this fleet doesn't have. If the bridges ever
// move to CLAUDE_CONFIG_DIR (Tier 3.2), revisit.

const CC_ACCOUNTS_DIR = path.join(os.homedir(), ".claude/accounts");
const CC_ACCOUNT_BIN = path.join(os.homedir(), ".local/bin/cc-account");
const BUS_EMIT = path.join(
  os.homedir(),
  "atlas/shared/scripts/bus/emit-event.sh"
);

async function readCurrentAccount() {
  try {
    return (
      await fsp.readFile(path.join(CC_ACCOUNTS_DIR, ".current"), "utf8")
    ).trim();
  } catch {
    return null;
  }
}

async function listAccountSnapshots() {
  try {
    const entries = await fsp.readdir(CC_ACCOUNTS_DIR);
    return entries
      .filter((f) => f.endsWith(".json") && !f.startsWith("."))
      .map((f) => f.replace(/\.json$/, ""))
      .sort();
  } catch {
    return [];
  }
}

const SWAP_IMPACT_LINES = [
  "• watson-bridge / builder-bridge: env-pinned via plist `CLAUDE_CODE_OAUTH_TOKEN` — **UNAFFECTED**",
  "• terminal / augur / dispatch / dodo / librarian / producer (CC tmux): env-pinned via `claude-<agent>-oauth.env` — **UNAFFECTED**",
  "• Interactive `claude` (no env override) reading `~/.claude/.credentials.json` — **SWAPPED**",
  "• cc-account auto-saves the freshly-refreshed live creds back to the OUTGOING account snapshot first (refresh tokens stay current across swaps)",
];

async function handleSwap(ctx, args) {
  const target = args[0];
  const current = await readCurrentAccount();
  const snapshots = await listAccountSnapshots();

  // Status mode (no args)
  if (!target) {
    return ctx.reply(
      [
        "\u{1F511} **cc-account**",
        `Current: \`${current || "(unknown — marker missing)"}\``,
        `Snapshots: ${snapshots.length ? snapshots.map((s) => `\`${s}\``).join(", ") : "(none)"}`,
        "",
        "**Impact map:**",
        ...SWAP_IMPACT_LINES,
        "",
        `Usage: \`${ctx.prefix}swap <account>\``,
      ].join("\n")
    );
  }

  if (target === current) {
    return ctx.reply(`\u{1F7E2} Already on \`${target}\`.`);
  }

  if (!snapshots.includes(target)) {
    return ctx.reply(
      `\u{274C} No snapshot for \`${target}\`. Available: ${snapshots.map((s) => `\`${s}\``).join(", ") || "(none)"}.\n` +
        "First-time setup: `/login` in CC, then `cp -p ~/.claude/.credentials.json ~/.claude/accounts/<name>.json && chmod 600 ~/.claude/accounts/<name>.json`."
    );
  }

  // Pre-flight: validate snapshot JSON + read expiresAt as info-only hint.
  // expiresAt being in the past is normal here because credentials.json is
  // refreshed in place; the snapshot file only updates when cc-account swaps
  // away from this account. claude auto-refreshes on first use via refreshToken.
  const snapshotPath = path.join(CC_ACCOUNTS_DIR, `${target}.json`);
  let expiresHint = "";
  try {
    const raw = await fsp.readFile(snapshotPath, "utf8");
    const snap = JSON.parse(raw);
    const expiresAt = snap?.claudeAiOauth?.expiresAt;
    if (typeof expiresAt === "number") {
      const days = (expiresAt - Date.now()) / 86_400_000;
      const iso = new Date(expiresAt).toISOString().slice(0, 10);
      expiresHint =
        days < 0
          ? `Snapshot accessToken expiresAt ${iso} (${Math.abs(days).toFixed(1)}d ago — claude auto-refreshes via refreshToken on first use, not a blocker).`
          : `Snapshot accessToken expiresAt ${iso} (${days.toFixed(1)}d ahead).`;
    }
  } catch (err) {
    return ctx.reply(
      `\u{274C} Snapshot for \`${target}\` is unreadable / invalid JSON: ${err.message}`
    );
  }

  if (ctx.platform !== "discord") {
    return ctx.reply(
      "`!swap <acct>` requires react-confirm — Discord-only."
    );
  }

  const replyLines = [
    `\u{1F511} **!swap ${target}**`,
    "",
    `Current: \`${current || "(unknown)"}\` → Target: \`${target}\``,
    expiresHint,
    "",
    "**Impact:**",
    ...SWAP_IMPACT_LINES,
    "",
    `**React \u{2705} within 60s to swap. React \u{274C} to cancel.**`,
  ].filter(Boolean);

  const sent = await ctx.reply(replyLines.join("\n"));
  const result = await awaitReactConfirm(sent, {
    adminId: DISCORD_ADMIN_ID,
    timeoutMs: 60_000,
  });

  if (result.outcome !== "confirmed") {
    const labels = {
      cancelled: "cancelled",
      timeout: "⏱️ timed out (60s)",
      "react-failed": `\u{26A0}️ react-confirm failed (${result.error || "unknown"})`,
    };
    await ctx.send(
      `${labels[result.outcome] || result.outcome} — no swap`
    );
    await auditLog(ctx.userId, "swap", [target], ctx.chatId, result.outcome);
    return;
  }

  // Execute the swap
  await auditLog(ctx.userId, "swap", [target], ctx.chatId, "executing");
  try {
    await execPromise(`"${CC_ACCOUNT_BIN}" "${target}"`, { timeout: 10_000 });

    const newCurrent = await readCurrentAccount();
    if (newCurrent !== target) {
      throw new Error(
        `marker mismatch: cc-account exited 0 but .current = ${newCurrent || "(empty)"}`
      );
    }

    // Emit swap.complete to bus (best-effort).
    try {
      const fromAcct = (current || "unknown").replace(/"/g, '\\"');
      const data = `{"from":"${fromAcct}","to":"${target}","triggered_by":"${ctx.userId}"}`;
      await execPromise(
        `"${BUS_EMIT}" terminal swap.complete "cc-account swapped ${fromAcct} -> ${target}" '${data}' rescue`,
        { timeout: 5_000 }
      );
    } catch (busErr) {
      console.error("[rescue] swap bus emit failed:", busErr.message);
    }

    await ctx.send(
      [
        `\u{2705} **Swapped: \`${current || "?"}\` → \`${target}\`**`,
        "Daemons unchanged (all env-pinned). The next interactive `claude` (no env override) will use the new account.",
        current
          ? `\`${ctx.prefix}swap ${current}\` to revert.`
          : "Use `!swap <name>` to switch back when needed.",
      ].join("\n")
    );
    await auditLog(
      ctx.userId,
      "swap",
      [target],
      ctx.chatId,
      `${current || "?"} -> ${target}`
    );
  } catch (err) {
    await ctx.send(`\u{274C} swap failed: ${err.message}`);
    await auditLog(
      ctx.userId,
      "swap",
      [target],
      ctx.chatId,
      `error: ${err.message}`
    );
  }
}

// --- legacy gateway-provider-order swap (kept reachable as !swap-order) ---
async function handleSwapOrder(ctx, args) {
  const sub = args[0]?.toLowerCase();

  try {
    const raw = await fsp.readFile(CONFIG_PATH, "utf-8");
    const cfg = JSON.parse(raw);
    const order = cfg.auth?.order?.anthropic;

    if (!order || !Array.isArray(order) || order.length < 2) {
      return ctx.reply(
        "\u274C No Anthropic auth order configured (need at least 2 profiles)."
      );
    }

    const current = order[0];
    const fallback = order[1];
    const label = (id) => id.replace("anthropic:", "");

    if (sub === "status" || !sub) {
      return ctx.reply(
        [
          "\u{1F511} **Anthropic Auth Order**",
          `Primary: **${label(current)}** | Fallback: **${label(fallback)}**`,
        ].join("\n")
      );
    }

    let newOrder;
    if (sub === "swap" || sub === "flip") {
      newOrder = [fallback, current];
    } else {
      // Try to match a profile name: !swap watson, !swap jeremy
      const target = order.find(
        (id) => label(id).toLowerCase() === sub
      );
      if (!target) {
        return ctx.reply(
          `Unknown profile \`${sub}\`. Available: ${order.map((id) => `\`${label(id)}\``).join(", ")}`
        );
      }
      if (target === current) {
        return ctx.reply(
          `\u{1F7E2} **${label(target)}** is already primary.`
        );
      }
      newOrder = [target, ...order.filter((id) => id !== target)];
    }

    // Write updated config
    cfg.auth.order.anthropic = newOrder;
    const tmp = CONFIG_PATH + `.swap-${process.pid}.tmp`;
    await fsp.writeFile(tmp, JSON.stringify(cfg, null, 2), "utf-8");
    await fsp.rename(tmp, CONFIG_PATH);

    await auditLog(
      ctx.userId,
      "swap",
      args,
      ctx.chatId,
      `${label(newOrder[0])}_primary`
    );

    return ctx.reply(
      [
        `\u{1F504} **Swapped** — Primary: **${label(newOrder[0])}** | Fallback: **${label(newOrder[1])}**`,
        "_Config hot-reloads \u2014 no gateway restart needed._",
      ].join("\n")
    );
  } catch (err) {
    console.error("[rescue] !swap error:", err.message);
    return ctx.reply(`\u274C Swap failed: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// !wcc — Watson Code Client (Claude Code tmux session management)
// ---------------------------------------------------------------------------

const WCC_TMUX_PREFIX = "watson-cc";
const WCC_START_SCRIPT = path.join(os.homedir(), ".openclaw/scripts/start-terminal.sh");

async function listWccSessions() {
  try {
    const { stdout } = await execPromise(`tmux list-sessions -F '#{session_name}:#{session_created}:#{session_attached}' 2>/dev/null`);
    return stdout
      .trim()
      .split("\n")
      .filter((s) => s.startsWith(WCC_TMUX_PREFIX))
      .map((line) => {
        const [name, created, attached] = line.split(":");
        const suffix = name === WCC_TMUX_PREFIX ? "" : name.slice(WCC_TMUX_PREFIX.length + 1);
        const uptime = created ? Math.floor((Date.now() / 1000 - parseInt(created)) / 60) : 0;
        return { name, suffix, uptime, attached: attached === "1" };
      });
  } catch {
    return [];
  }
}

async function handleWcc(ctx, args) {
  const sub = args[0]?.toLowerCase() || "status";

  if (sub === "start") {
    const rawSuffix = args[1] || "";
    const suffix = rawSuffix.toLowerCase().replace(/[^a-z0-9-]/g, "");

    // Reject empty-after-sanitization input
    if (rawSuffix && !suffix) {
      return ctx.reply(`\u{274C} Invalid session name: \`${rawSuffix}\``);
    }

    const tmuxSession = suffix ? `${WCC_TMUX_PREFIX}-${suffix}` : WCC_TMUX_PREFIX;
    const displayName = suffix || "terminal";

    // Check if already running
    try {
      const { stdout } = await execPromise(`tmux has-session -t =${tmuxSession} 2>&1 && echo RUNNING || echo STOPPED`);
      if (stdout.trim() === "RUNNING") {
        return ctx.reply(`\u{1F7E2} **${displayName}** already running.`);
      }
    } catch {}

    try {
      const startArgs = suffix ? `--detached ${suffix}` : "--detached";
      const terminalToken = DISCORD_TERMINAL_TOKEN || DISCORD_TOKEN;
      // Pass token via env (not shell string) to avoid ps leakage
      const env = { ...process.env };
      if (!suffix && terminalToken) env.DISCORD_PLUGIN_BOT_TOKEN = terminalToken;
      await execPromise(`"${WCC_START_SCRIPT}" ${startArgs}`, { env });

      await auditLog(ctx.userId, "wcc", ["start", displayName], ctx.chatId, "started");
      const isDefault = !suffix;
      return ctx.reply(
        [
          `\u{1F4BB} **WCC started** \u2014 \`${displayName}\``,
          isDefault ? "\u{1F4E1} Discord channels active" : "",
          "",
          `_tmux: \`${tmuxSession}\` | bridge: \`cc-tmux-send.sh${suffix ? ` -s ${suffix}` : ""}\`_`,
        ]
          .filter(Boolean)
          .join("\n")
      );
    } catch (err) {
      return ctx.reply(`\u{274C} Failed to start wcc: ${err.message}`);
    }
  }

  if (sub === "stop") {
    const target = args[1]?.toLowerCase().replace(/[^a-z0-9-]/g, "") || "";

    if (target === "all") {
      const sessions = await listWccSessions();
      if (sessions.length === 0) return ctx.reply("No wcc sessions running.");
      for (const s of sessions) {
        try { await execPromise(`tmux kill-session -t =${s.name} 2>&1`); } catch {}
      }
      await auditLog(ctx.userId, "wcc", ["stop", "all"], ctx.chatId, `stopped_${sessions.length}`);
      return ctx.reply(`\u{1F534} Stopped ${sessions.length} wcc session${sessions.length !== 1 ? "s" : ""}.`);
    }

    const tmuxSession = target ? `${WCC_TMUX_PREFIX}-${target}` : WCC_TMUX_PREFIX;
    try {
      await execPromise(`tmux kill-session -t =${tmuxSession} 2>&1`);
      await auditLog(ctx.userId, "wcc", ["stop", target || "terminal"], ctx.chatId, "stopped");
      return ctx.reply(`\u{1F534} Stopped wcc session \`${target || "terminal"}\`.`);
    } catch {
      return ctx.reply(`No wcc session \`${target || "terminal"}\` running.`);
    }
  }

  if (sub === "kill") {
    const target = args[1]?.toLowerCase().replace(/[^a-z0-9-]/g, "") || "";
    const mcpCachePath = path.join(os.homedir(), ".claude", "mcp-needs-auth-cache.json");

    async function killWccSession(sessionName, displayName) {
      let panePid = null;
      let orphanCleanup = "none";
      let cacheCleanup = "unchanged";

      try {
        const { stdout } = await execPromise(`tmux list-panes -t =${sessionName} -F '#{pane_pid}' 2>&1`);
        const rawPid = stdout.trim().split(/\s+/)[0];
        if (/^\d+$/.test(rawPid)) panePid = rawPid;
      } catch {}

      try {
        await execPromise(`tmux kill-session -t =${sessionName} 2>&1`);
      } catch {
        throw new Error(`No wcc session \`${displayName}\` running.`);
      }

      if (panePid) {
        try {
          const { stdout } = await execPromise(`pkill -P ${panePid} 2>&1 || true`);
          orphanCleanup = stdout.trim() || `pkill -P ${panePid}`;
        } catch {
          orphanCleanup = `pkill -P ${panePid}`;
        }
      }

      try {
        await fsp.writeFile(mcpCachePath, "{}\n", "utf8");
        cacheCleanup = "cleared";
      } catch (err) {
        cacheCleanup = `error: ${err.message}`;
      }

      return { panePid: panePid || "unknown", orphanCleanup, cacheCleanup };
    }

    if (target === "all") {
      const sessions = await listWccSessions();
      if (sessions.length === 0) return ctx.reply("No wcc sessions running.");

      const cleaned = [];
      for (const s of sessions) {
        try {
          const result = await killWccSession(s.name, s.suffix || "terminal");
          cleaned.push(`- \`${s.suffix || "terminal"}\`: pane ${result.panePid}, children ${result.orphanCleanup}, cache ${result.cacheCleanup}`);
        } catch (err) {
          cleaned.push(`- \`${s.suffix || "terminal"}\`: ${err.message}`);
        }
      }

      await auditLog(ctx.userId, "wcc", ["kill", "all"], ctx.chatId, "killed");
      return ctx.reply([
        `\u{2620}\u{FE0F} Killed ${sessions.length} wcc session${sessions.length !== 1 ? "s" : ""}.`,
        "",
        "Cleanup:",
        ...cleaned,
      ].join("\n"));
    }

    const tmuxSession = target ? `${WCC_TMUX_PREFIX}-${target}` : WCC_TMUX_PREFIX;
    try {
      const result = await killWccSession(tmuxSession, target || "terminal");
      await auditLog(ctx.userId, "wcc", ["kill", target || "terminal"], ctx.chatId, "killed");
      return ctx.reply([
        `\u{2620}\u{FE0F} Killed wcc session \`${target || "terminal"}\`.`,
        `- pane pid: ${result.panePid}`,
        `- orphan cleanup: ${result.orphanCleanup}`,
        `- MCP cache: ${result.cacheCleanup}`,
      ].join("\n"));
    } catch (err) {
      return ctx.reply(err.message);
    }
  }

  if (sub === "restart") {
    const tmuxSession = WCC_TMUX_PREFIX;

    // Pre-flight: verify the start script exists
    try {
      await fsp.access(WCC_START_SCRIPT);
    } catch {
      await auditLog(ctx.userId, "wcc", ["restart"], ctx.chatId, "script_missing");
      return ctx.reply(
        `\u{274C} Cannot restart: start script not found at \`${WCC_START_SCRIPT}\``
      );
    }

    try { await execPromise(`tmux kill-session -t =${tmuxSession} 2>&1`); } catch {}
    await new Promise((r) => setTimeout(r, 3000));

    try {
      const terminalToken = DISCORD_TERMINAL_TOKEN || DISCORD_TOKEN;
      // Pass token via env (not shell string) to avoid ps leakage
      const env = { ...process.env };
      if (terminalToken) env.DISCORD_PLUGIN_BOT_TOKEN = terminalToken;
      await execPromise(`"${WCC_START_SCRIPT}" --detached`, { env });

      // Startup verification: poll tmux for up to 10s
      const deadline = Date.now() + 10000;
      let started = false;
      while (Date.now() < deadline) {
        try {
          await execPromise(`tmux has-session -t =${tmuxSession} 2>/dev/null`);
          started = true;
          break;
        } catch {
          await new Promise((r) => setTimeout(r, 500));
        }
      }

      if (!started) {
        await auditLog(ctx.userId, "wcc", ["restart"], ctx.chatId, "startup_timeout");
        return ctx.reply(
          `\u{26A0}\u{FE0F} Start script ran but tmux session \`${tmuxSession}\` didn't appear within 10s.\n` +
          `Check \`~/.openclaw/logs/terminal.log\` for errors.`
        );
      }

      await auditLog(ctx.userId, "wcc", ["restart"], ctx.chatId, "restarted");
      return ctx.reply(`\u{1F504} Terminal restarted — \`${tmuxSession}\` is live with Discord channels.`);
    } catch (err) {
      await auditLog(ctx.userId, "wcc", ["restart"], ctx.chatId, `failed: ${err.message}`);
      return ctx.reply(`\u{274C} Failed to restart: ${err.message}`);
    }
  }

  // Default: status — list all wcc sessions
  const sessions = await listWccSessions();
  if (sessions.length === 0) {
    return ctx.reply(
      `\u{26AA} No wcc sessions running.\n\`${ctx.prefix}wcc start\` \u2014 start Terminal (Discord channels)\n\`${ctx.prefix}wcc start research\` \u2014 start a plain session`
    );
  }

  const lines = [`\u{1F4BB} **WCC Sessions** (${sessions.length})\n`];
  for (const s of sessions) {
    const label = s.suffix || "terminal";
    const hours = Math.floor(s.uptime / 60);
    const mins = s.uptime % 60;
    const uptimeStr = hours > 0 ? `${hours}h${mins}m` : `${mins}m`;
    const status = s.attached ? "attached" : "detached";
    const channels = !s.suffix ? " \u{1F4E1}" : "";
    lines.push(`\u{1F7E2} **${label}**${channels} \u2014 ${uptimeStr}, ${status}`);
  }

  return ctx.reply(lines.join("\n"));
}

// ---------------------------------------------------------------------------
// !help
// ---------------------------------------------------------------------------

async function handleHelp(ctx) {
  const p = ctx.prefix;
  const aliasEntries = Object.entries(AGENT_ALIASES);
  const aliasLine =
    aliasEntries.length > 0
      ? `\n_Agent aliases: ${aliasEntries.map(([a, id]) => `\`${a}\`\u2192\`${id}\``).join(", ")}_`
      : "";

  const discordOnly =
    ctx.platform === "discord"
      ? [
          `\`${p}mute [agent]\` \u2014 Silence agent in this channel (require @mention)`,
          `\`${p}unmute [agent]\` \u2014 Let agent respond to all messages`,
        ]
      : [];

  return ctx.reply(
    [
      "**Rescue \u2014 Atlas Admin Bot**",
      "",
      "**Quick checks (read-only)**",
      `\`${p}ping\` \u2014 Is rescue alive? version + uptime + ws latency`,
      `\`${p}status\` \u2014 Substrate-only state of every launchd + tmux + cc-account`,
      `\`${p}atlas\` \u2014 Off-network atlas-os snapshot (5 metrics)`,
      "",
      "**Fix a single agent** (react \u2705 to confirm)",
      `\`${p}restart <agent>\` \u2014 Restart with idle-aware tmux probe`,
      `   targets: terminal, augur, dodo, librarian, dispatch, producer, watson-bridge, builder-bridge`,
      `\`${p}restart gateway\` \u2014 Restart the gateway`,
      "",
      "**A cron is spamming Discord**",
      `\`${p}panic\` \u2014 Stop ALL autonomous Discord posting (react \u2705)`,
      `\`${p}resume\` \u2014 Lift the panic flag (no confirm \u2014 friction-free recovery)`,
      "",
      "**Account swap (5h quota hit)**",
      `\`${p}swap [acct]\` \u2014 Swap cc-account; shows inventory + react \u2705`,
      `\`${p}swap\` (no arg) \u2014 List available account snapshots`,
      `\`${p}swap-order [legacy]\` \u2014 Flip gateway provider order (legacy)`,
      "",
      "**Sessions**",
      `\`${p}reset [agent] [all]\` \u2014 Reset agent session(s)`,
      `\`${p}handoff [agent]\` \u2014 Write handoff \u2192 reset \u2192 breadcrumb`,
      `\`${p}handoff --dry-run\` \u2014 Preview only`,
      `\`${p}gc [run]\` \u2014 Session GC status / trigger`,
      "",
      "**Services**",
      `\`${p}mc [start|stop|status]\` \u2014 Mission Control dashboard`,
      `\`${p}ki [start|stop|status]\` \u2014 Knowledge Intake (localhost:7420)`,
      `\`${p}watson-graph [start|stop|status]\` \u2014 Watson Knowledge Graph (localhost:4444)`,
      "",
      "**Terminal (Claude Code via Discord)**",
      `\`${p}wcc [start|stop|kill|restart] [name|all]\` \u2014 Manage watson-cc-* tmux sessions`,
      `\`${p}wcc\` (no args) \u2014 List all sessions`,
      "",
      "**Config**",
      `\`${p}model [alias|show|default]\` \u2014 Override / inspect / clear model`,
      `\`${p}backup\` / \`${p}rollback [list]\` \u2014 Snapshot + restore gateway config`,
      ...discordOnly,
      "",
      "**Hard-rejected**",
      `\`${p}restart rescue\` \u2014 Self-restart blocked (R3 self-suicide)`,
      "",
      `\`${p}help\` \u2014 This message`,
      aliasLine,
    ]
      .filter(Boolean)
      .join("\n")
  );
}

// ---------------------------------------------------------------------------
// Stall detector
// ---------------------------------------------------------------------------

async function readLastMessageRole(agentId, entry) {
  if (!entry.sessionId || !isValidAgentId(agentId)) return null;
  if (!/^[a-zA-Z0-9_-]+$/.test(entry.sessionId)) return null;
  const jsonlPath = path.join(
    AGENTS_DIR,
    agentId,
    "sessions",
    `${entry.sessionId}.jsonl`
  );
  try {
    const stat = await fsp.stat(jsonlPath);
    if (stat.size === 0) return null;
    const readSize = Math.min(2048, stat.size);
    const buf = Buffer.alloc(readSize);
    const fd = await fsp.open(jsonlPath, "r");
    try {
      await fd.read(buf, 0, readSize, stat.size - readSize);
    } finally {
      await fd.close();
    }
    const lines = buf
      .toString("utf-8")
      .split("\n")
      .filter((l) => l.trim());
    for (let i = lines.length - 1; i >= 0; i--) {
      try {
        const msg = JSON.parse(lines[i]);
        if (msg.role) return msg.role;
      } catch {
        // Partial line at start of buffer
      }
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Check CC tmux sessions (watson-cc, watson-cc-*) for stalls.
 * CC sessions don't write sessions.json, so we use tmux pane inspection instead.
 *
 * Heuristic: capture the pane output and compare against the last capture.
 * If content hasn't changed in STALL_THRESHOLD_MS AND the last visible prompt
 * isn't a bare ready state, the session is probably stuck.
 */
const ccPaneSnapshots = new Map(); // sessionName -> { content, lastChange }

async function checkForStalledCcSessions() {
  const now = Date.now();
  try {
    // List all watson-cc* tmux sessions
    const { stdout: lsOut } = await execPromise("tmux ls -F '#{session_name}' 2>/dev/null || true");
    const sessions = lsOut
      .split("\n")
      .map((s) => s.trim())
      .filter((s) => s.startsWith("watson-cc"));

    for (const sessionName of sessions) {
      let paneContent;
      try {
        const { stdout } = await execPromise(
          `tmux capture-pane -t =${sessionName} -p -S -20 2>/dev/null`
        );
        paneContent = stdout;
      } catch {
        continue; // Session disappeared between ls and capture
      }

      // Normalize — strip trailing whitespace and count-like indicators
      const normalized = paneContent.trim();
      const prev = ccPaneSnapshots.get(sessionName);

      if (!prev || prev.content !== normalized) {
        ccPaneSnapshots.set(sessionName, { content: normalized, lastChange: now });
        continue;
      }

      const staleFor = now - prev.lastChange;
      if (staleFor < STALL_THRESHOLD_MS) continue;

      // Cooldown check
      const key = `cc:${sessionName}`;
      const lastAlert = staleAlertTimes.get(key) || 0;
      if (now - lastAlert < STALL_ALERT_COOLDOWN) continue;

      // Check last few lines for a bare ready-state prompt (not actually stalled)
      const tail = normalized.split("\n").slice(-3).join("\n");
      const looksReady =
        /bypass permissions on/.test(tail) ||
        /\? for shortcuts/.test(tail) ||
        /^\s*❯\s*$/m.test(tail);
      if (looksReady) continue;

      const minutes = Math.round(staleFor / 60000);
      const alertMsg = `\u{1F6A8} **CC Stall:** \`${sessionName}\` hasn't changed in ${minutes} minutes. Check with \`tmux attach -t ${sessionName}\` or use \`!wcc restart\`.`;
      await sendOpsAlert(alertMsg);
      staleAlertTimes.set(key, now);
      await auditLog("system", "cc_stall_alert", [sessionName], "ops", `${minutes}min`);
      emitBusEvent(
        "rescue-bot",
        "session_stalled",
        `CC session ${sessionName}: no output change in ${minutes}min`,
        { session: sessionName, minutes, kind: "cc" },
        "session"
      );
    }

    // Prune snapshots for sessions that no longer exist
    for (const name of ccPaneSnapshots.keys()) {
      if (!sessions.includes(name)) ccPaneSnapshots.delete(name);
    }
  } catch (err) {
    console.error("[rescue] CC stall check error:", err.message);
  }
}

async function checkForStalledSessions() {
  const agents = await listAgentIds();
  const now = Date.now();

  for (const [key, time] of staleAlertTimes) {
    if (now - time > STALL_ALERT_COOLDOWN * 2) staleAlertTimes.delete(key);
  }

  for (const agentId of agents) {
    const result = await readSessionsJson(agentId);
    if (!result) continue;

    for (const [key, entry] of Object.entries(result.data)) {
      // Support both Discord and Telegram session keys
      const discordMatch = key.match(/discord:channel:(\d+)/);
      const telegramMatch = key.match(/telegram:chat:(-?\d+)/);
      if (!discordMatch && !telegramMatch) continue;

      const updatedAt = entry.updatedAt || 0;
      const timeSinceUpdate = now - updatedAt;

      if (timeSinceUpdate > STALL_ACTIVE_WINDOW) continue;
      if (timeSinceUpdate < STALL_THRESHOLD_MS) continue;

      const lastAlert = staleAlertTimes.get(key) || 0;
      if (now - lastAlert < STALL_ALERT_COOLDOWN) continue;

      const lastRole = await readLastMessageRole(agentId, entry);
      if (lastRole !== "user") continue;

      const name = agentName(agentId);
      const minutes = Math.round(timeSinceUpdate / 60000);
      const usage = getUsageInfo(entry);

      const alertText =
        `\u{1F6A8} **Stall detected:** ${name} hasn't responded in ${minutes} minutes (context: ${usage.pct}%). Use \`!reset\` to clear the session.`;

      // Alert via Discord
      if (discordMatch && discordClient) {
        const channelId = discordMatch[1];
        try {
          const channel = await discordClient.channels.fetch(channelId);
          if (channel?.isTextBased()) {
            await channel.send(alertText);
          }
        } catch (err) {
          console.error(
            `[rescue] Stall alert failed for ${key}:`,
            err.message
          );
        }
      }

      // Alert via Telegram
      if (telegramMatch && telegramBot) {
        const chatId = telegramMatch[1];
        try {
          await telegramBot.api.sendMessage(chatId, alertText);
        } catch (err) {
          console.error(
            `[rescue] Telegram stall alert failed for ${key}:`,
            err.message
          );
        }
      }

      staleAlertTimes.set(key, now);
      await auditLog(
        "system",
        "stall_alert",
        [agentId, key],
        discordMatch?.[1] || telegramMatch?.[1] || "unknown",
        `${minutes}min_${usage.pct}pct`
      );
      // Emit to bus
      emitBusEvent(
        "rescue-bot",
        "session_stalled",
        `${name}: stalled ${minutes}min (context ${usage.pct}%)`,
        { agent: agentId, key, minutes, context_pct: usage.pct },
        "session"
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Gateway watchdog
// ---------------------------------------------------------------------------

function getGatewayPid() {
  return new Promise((resolve) => {
    exec(
      `pgrep -foU $(whoami) "${GATEWAY_PROCESS_NAME}"`,
      { timeout: 5000 },
      (err, stdout) => {
        if (err) return resolve(null);
        const pids = stdout.trim().split("\n").filter(Boolean);
        resolve(pids.length > 0 ? parseInt(pids[0], 10) : null);
      }
    );
  });
}

/**
 * Stop the gateway by targeted PID kill. Falls back to user-scoped pkill
 * if PID lookup fails (e.g. during system stress). Returns true if a kill
 * signal was sent, false if gateway was not found.
 */
async function stopGateway() {
  const pid = await getGatewayPid();
  if (pid) {
    try {
      process.kill(pid, "SIGTERM");
      return true;
    } catch (e) {
      // ESRCH = process already gone — that's fine
      if (e.code === "ESRCH") return false;
      throw e;
    }
  }
  // PID lookup failed — fall back to user-scoped pkill as safety net
  return new Promise((resolve) => {
    exec(`pkill -fU $(whoami) ${GATEWAY_PROCESS_NAME}`, (err) => {
      // pkill exit 0 = signal sent, exit 1 = no match, other = error
      resolve(!err);
    });
  });
}

async function checkGatewayHealth() {
  const now = Date.now();
  watchdogState.lastCheckTime = now;

  const pid = await getGatewayPid();

  if (!pid) {
    if (watchdogState.lastPid !== null) {
      watchdogState.restarts.push(now);
      if (watchdogState.restarts.length > 20) {
        watchdogState.restarts = watchdogState.restarts.slice(-20);
      }
    }
    watchdogState.lastPid = null;
    watchdogState.status = "down";
    return;
  }

  if (watchdogState.lastPid !== null && pid !== watchdogState.lastPid) {
    watchdogState.restarts.push(now);
    if (watchdogState.restarts.length > 20) {
      watchdogState.restarts = watchdogState.restarts.slice(-20);
    }
    // Emit to bus — informational, pulse will surface via health_check
    emitBusEvent(
      "rescue-bot",
      "health_check",
      `Gateway PID change detected: ${watchdogState.lastPid} → ${pid}`,
      { old_pid: watchdogState.lastPid, new_pid: pid, total_restarts: watchdogState.restarts.length },
      "gateway"
    );
  }

  watchdogState.lastPid = pid;

  const recentRestarts = watchdogState.restarts.filter(
    (t) => now - t < WATCHDOG_CRASH_WINDOW
  );

  if (recentRestarts.length >= WATCHDOG_CRASH_THRESHOLD) {
    if (now - watchdogState.lastAlertTime < WATCHDOG_ALERT_COOLDOWN) {
      watchdogState.status = "cooldown";
      return;
    }

    watchdogState.status = "crash-loop";
    watchdogState.lastAlertTime = now;

    const alertMessage =
      `\u{1F6A8} **Gateway Crash-Loop Detected**\n` +
      `${recentRestarts.length} restarts in the last ${Math.round(WATCHDOG_CRASH_WINDOW / 60000)} minutes\n` +
      `Current PID: ${pid}\n\n` +
      `**Recommended actions:**\n` +
      `\u2022 \`!rollback\` \u2014 restore last known good config\n` +
      `\u2022 \`!rollback list\` \u2014 see available backups\n` +
      `\u2022 Check gateway logs for the root cause`;

    // Alert via Discord ops channel
    if (OPS_CHANNEL_ID && discordClient) {
      try {
        const channel = await discordClient.channels.fetch(OPS_CHANNEL_ID);
        if (channel?.isTextBased()) {
          await channel.send(alertMessage);
        }
      } catch (err) {
        console.error(
          "[rescue] Watchdog Discord alert failed:",
          err.message
        );
      }
    }

    // Alert via Telegram ops chat
    if (OPS_TELEGRAM_CHAT && telegramBot) {
      try {
        await telegramBot.api.sendMessage(OPS_TELEGRAM_CHAT, alertMessage);
      } catch (err) {
        console.error(
          "[rescue] Watchdog Telegram alert failed:",
          err.message
        );
      }
    }

    await auditLog(
      "watchdog",
      "crash_loop_alert",
      [String(recentRestarts.length), String(pid)],
      OPS_CHANNEL_ID || OPS_TELEGRAM_CHAT || "none",
      `${recentRestarts.length}_restarts_in_${WATCHDOG_CRASH_WINDOW / 60000}min`
    );

    // Emit to bus — this is a critical escalation, bus-tail will forward to ops
    emitBusEvent(
      "rescue-bot",
      "escalation",
      `Gateway crash-loop: ${recentRestarts.length} restarts in ${WATCHDOG_CRASH_WINDOW / 60000}min (pid ${pid})`,
      { restart_count: recentRestarts.length, window_min: WATCHDOG_CRASH_WINDOW / 60000, pid },
      "gateway"
    );

    console.log(
      `[rescue] Watchdog: crash-loop detected (${recentRestarts.length} restarts in ${WATCHDOG_CRASH_WINDOW / 60000}min)`
    );
    return;
  }

  watchdogState.status = "healthy";
}

// ---------------------------------------------------------------------------
// !handoff — context-preserving reset (write handoff → reset → breadcrumb)
// ---------------------------------------------------------------------------

const HANDOFF_MEMORY_DIR = path.join(
  AGENTS_DIR,
  "main",
  "workspace",
  "memory"
);

async function handleHandoff(ctx, args) {
  const dryRun = args.includes("--dry-run");
  const agentArg = args.find((a) => !a.startsWith("--"));

  // Resolve agent — default to this channel's agent
  let agentId;
  if (agentArg) {
    agentId = await resolveAgentId(agentArg);
    if (!agentId) {
      const list = await formatAgentList();
      return ctx.reply(`Agent \`${agentArg}\` not found. Available: ${list}`);
    }
  } else {
    const session = await findSessionByChat(ctx.chatId, ctx.platform);
    if (!session) {
      return ctx.reply(
        `No agent session in this channel. Specify one: \`${ctx.prefix}handoff <agent>\``
      );
    }
    agentId = session.agentId;
  }

  // Find the session
  const session = await findSessionByChat(ctx.chatId, ctx.platform);
  if (!session || session.agentId !== agentId) {
    const sessions = await findSessionsByAgent(agentId);
    if (sessions.length === 0) {
      return ctx.reply(
        `No sessions found for **${agentDisplayName(agentId)}**.`
      );
    }
  }

  const usage = session ? getUsageInfo(session.entry) : null;
  const now = new Date();
  const timestamp = now.toISOString().replace(/[:.]/g, "-").slice(0, 16);
  const handoffFile = path.join(
    AGENTS_DIR,
    agentId,
    "workspace",
    "memory",
    `${timestamp}-context-handoff.md`
  );

  if (dryRun) {
    const lines = [
      `\u{1F50D} **Handoff dry-run for ${agentDisplayName(agentId)}**`,
      "",
      `**Session:** ${usage ? `${usage.pct}% context used` : "no session in this channel"}`,
      `**Handoff file:** \`${path.basename(handoffFile)}\``,
      "",
      "**Would do:**",
      "1. Ask agent to write context handoff to memory",
      "2. Reset the session",
      "3. Leave breadcrumb in new session",
      "",
      `_Run \`${ctx.prefix}handoff${agentArg ? " " + agentArg : ""}\` to execute._`,
    ];
    await auditLog(ctx.userId, "handoff", ["--dry-run", agentId], ctx.chatId, "dry_run");
    return ctx.reply(lines.join("\n"));
  }

  await ctx.reply(
    `\u{1F4DD} Starting handoff for **${agentDisplayName(agentId)}**...`
  );

  // Step 1: Write a handoff prompt to the agent's session via the gateway
  // We send a message to the agent asking it to write its handoff
  const handoffPrompt =
    `URGENT: Your session is about to be reset. ` +
    `Write a concise context handoff to ${handoffFile} covering: ` +
    `what you were working on, current state, and what the next session needs to know. ` +
    `Keep it under 500 words. Write the file NOW.`;

  // Send the handoff prompt through the gateway API
  try {
    await new Promise((resolve, reject) => {
      const payload = JSON.stringify({
        agentId,
        message: handoffPrompt,
      });
      const req = require("http").request(
        {
          hostname: "127.0.0.1",
          port: 18789,
          path: "/api/message",
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(payload),
          },
          timeout: 30000,
        },
        (res) => {
          let body = "";
          res.on("data", (chunk) => (body += chunk));
          res.on("end", () => {
            if (res.statusCode >= 200 && res.statusCode < 300) {
              resolve(body);
            } else {
              reject(new Error(`Gateway responded ${res.statusCode}: ${body.slice(0, 200)}`));
            }
          });
        }
      );
      req.on("error", reject);
      req.on("timeout", () => {
        req.destroy();
        reject(new Error("Gateway request timed out (30s)"));
      });
      req.write(payload);
      req.end();
    });

    await ctx.send("\u2705 Handoff message sent to agent. Waiting for response...");

    // Give agent time to write the handoff file
    await new Promise((r) => setTimeout(r, 15000));

    // Check if the handoff file was written
    let handoffWritten = false;
    try {
      await fsp.access(handoffFile);
      handoffWritten = true;
    } catch {
      // Check for any recent handoff file (agent may have used a slightly different name)
      try {
        const memDir = path.join(AGENTS_DIR, agentId, "workspace", "memory");
        const files = await fsp.readdir(memDir);
        const recent = files
          .filter((f) => f.includes("handoff") && f.endsWith(".md"))
          .sort()
          .reverse();
        if (recent.length > 0) {
          const stat = await fsp.stat(path.join(memDir, recent[0]));
          if (Date.now() - stat.mtimeMs < 60000) {
            handoffWritten = true;
          }
        }
      } catch {
        // No memory dir
      }
    }

    if (handoffWritten) {
      await ctx.send("\u{1F4BE} Handoff file written.");
    } else {
      await ctx.send(
        "\u{26A0}\uFE0F No handoff file detected (agent may not have written one). Continuing with reset..."
      );
    }
  } catch (err) {
    await ctx.send(
      `\u{26A0}\uFE0F Could not reach gateway for handoff prompt: ${err.message}\nContinuing with reset anyway...`
    );
  }

  // Step 2: Reset the session
  if (session) {
    await resetSession(session);
    await ctx.send(
      `\u{1F504} Session reset (was at ${usage ? usage.pct + "%" : "unknown"} context).`
    );
  }

  // Step 3: Leave a breadcrumb — write a note so the next session picks up the handoff
  const breadcrumbFile = path.join(
    AGENTS_DIR,
    agentId,
    "workspace",
    "memory",
    "HANDOFF_BREADCRUMB.md"
  );
  try {
    const breadcrumb =
      `# Session Handoff — ${now.toISOString()}\n\n` +
      `A handoff was performed. Check the latest handoff file in this directory:\n` +
      `\`ls -t *handoff*.md | head -1\`\n\n` +
      `Previous session was at ${usage ? usage.pct + "%" : "unknown"} context.\n` +
      `Triggered by: rescue-bot !handoff command\n`;
    await fsp.mkdir(path.dirname(breadcrumbFile), { recursive: true });
    await fsp.writeFile(breadcrumbFile, breadcrumb);
  } catch (err) {
    console.error("[rescue] Failed to write breadcrumb:", err.message);
  }

  await auditLog(
    ctx.userId,
    "handoff",
    [agentId],
    ctx.chatId,
    `reset_${usage ? usage.pct + "pct" : "unknown"}`
  );

  return ctx.send(
    `\u{2705} **Handoff complete for ${agentDisplayName(agentId)}.**\n` +
      `Context has been preserved. The next message will start a fresh session with breadcrumb.`
  );
}

// ---------------------------------------------------------------------------
// !mc — Manage Mission Control dashboard
// ---------------------------------------------------------------------------

const MC_DIR = path.join(os.homedir(), "jeremy-hq");
const MC_PLIST = path.join(
  os.homedir(),
  "Library",
  "LaunchAgents",
  "com.openclaw.mission-control-restart.plist"
);
const MC_PORT = 3000;

function mcGetPid() {
  return new Promise((resolve) => {
    exec(
      `lsof -ti :${MC_PORT} 2>/dev/null | head -1`,
      { timeout: 5000 },
      (err, stdout) => {
        if (err) return resolve(null);
        const pid = parseInt(stdout.trim(), 10);
        resolve(isNaN(pid) ? null : pid);
      }
    );
  });
}

async function handleMc(ctx, args) {
  const sub = (args[0] || "status").toLowerCase();

  if (sub === "start") {
    const existing = await mcGetPid();
    if (existing) {
      return ctx.reply(
        `Mission Control is already running (PID ${existing}). http://127.0.0.1:${MC_PORT}/`
      );
    }

    await ctx.reply("\u{1F680} Starting Mission Control...");

    exec(
      `cd ${MC_DIR} && npm run dev > /dev/null 2>&1 &`,
      { timeout: 10000, cwd: MC_DIR },
      () => {}
    );

    // Wait a few seconds for the server to start
    await new Promise((r) => setTimeout(r, 5000));

    const pid = await mcGetPid();
    if (pid) {
      await auditLog(ctx.userId, "mc", ["start"], ctx.chatId, `started_pid_${pid}`);
      return ctx.send(
        `\u{2705} Mission Control started (PID ${pid}). http://127.0.0.1:${MC_PORT}/`
      );
    } else {
      await auditLog(ctx.userId, "mc", ["start"], ctx.chatId, "start_failed");
      return ctx.send(
        "\u{274C} Mission Control may not have started. Check logs at `~/.openclaw/logs/mc-restart.log`."
      );
    }
  }

  if (sub === "stop") {
    const pid = await mcGetPid();
    if (!pid) {
      return ctx.reply("Mission Control is not running.");
    }

    await new Promise((resolve) => {
      exec(
        `kill ${pid} 2>/dev/null; pkill -f "next-server" 2>/dev/null; pkill -f "next dev" 2>/dev/null`,
        { timeout: 5000 },
        () => resolve()
      );
    });

    await new Promise((r) => setTimeout(r, 2000));

    const check = await mcGetPid();
    if (check) {
      await auditLog(ctx.userId, "mc", ["stop"], ctx.chatId, "stop_failed");
      return ctx.reply(`\u{26A0}\uFE0F Process still running (PID ${check}). May need \`kill -9 ${check}\`.`);
    }

    await auditLog(ctx.userId, "mc", ["stop"], ctx.chatId, "stopped");
    return ctx.reply("\u{2705} Mission Control stopped.");
  }

  if (sub === "status") {
    const pid = await mcGetPid();
    const lines = [
      `\u{1F4CA} **Mission Control**`,
      "",
      `**Status:** ${pid ? `\u{1F7E2} Running (PID ${pid})` : "\u{1F534} Stopped"}`,
      `**URL:** http://127.0.0.1:${MC_PORT}/`,
      `**Directory:** \`${MC_DIR}\``,
    ];

    await auditLog(ctx.userId, "mc", ["status"], ctx.chatId, pid ? `running_${pid}` : "stopped");
    return ctx.reply(lines.join("\n"));
  }

  return ctx.reply(
    `Usage: \`${ctx.prefix}mc [start|stop|status]\``
  );
}

// ---------------------------------------------------------------------------
// !ki — Manage Knowledge Intake server
// ---------------------------------------------------------------------------

const KI_PORT = 7420;
const KI_SERVER_PLIST = path.join(
  os.homedir(),
  "Library",
  "LaunchAgents",
  "ai.watson.knowledge-intake-server.plist"
);
const KI_POLL_PLIST = path.join(
  os.homedir(),
  "Library",
  "LaunchAgents",
  "ai.watson.knowledge-intake-poll.plist"
);

function kiGetPid() {
  return new Promise((resolve) => {
    exec(
      `lsof -ti :${KI_PORT} 2>/dev/null | head -1`,
      { timeout: 5000 },
      (err, stdout) => {
        if (err) return resolve(null);
        const pid = parseInt(stdout.trim(), 10);
        resolve(isNaN(pid) ? null : pid);
      }
    );
  });
}

function kiLaunchdLoaded() {
  return new Promise((resolve) => {
    exec(
      `launchctl list ai.watson.knowledge-intake-server 2>/dev/null`,
      { timeout: 5000 },
      (err, stdout) => {
        resolve(!err && stdout.includes("ai.watson.knowledge-intake-server"));
      }
    );
  });
}

async function handleKi(ctx, args) {
  const sub = (args[0] || "status").toLowerCase();
  const uid = process.getuid ? process.getuid() : 501;

  if (sub === "start") {
    const loaded = await kiLaunchdLoaded();
    if (loaded) {
      const pid = await kiGetPid();
      if (pid) {
        return ctx.reply(
          `Knowledge Intake is already running (PID ${pid}). http://localhost:${KI_PORT}/`
        );
      }
    }

    await ctx.reply("\u{1F680} Starting Knowledge Intake...");

    await new Promise((resolve) => {
      exec(
        `launchctl bootstrap gui/${uid} ${KI_SERVER_PLIST} 2>/dev/null; launchctl kickstart gui/${uid}/ai.watson.knowledge-intake-server 2>/dev/null; launchctl bootstrap gui/${uid} ${KI_POLL_PLIST} 2>/dev/null; launchctl kickstart gui/${uid}/ai.watson.knowledge-intake-poll 2>/dev/null`,
        { timeout: 10000 },
        () => resolve()
      );
    });

    await new Promise((r) => setTimeout(r, 3000));

    const pid = await kiGetPid();
    const loaded2 = await kiLaunchdLoaded();

    if (pid) {
      await auditLog(ctx.userId, "ki", ["start"], ctx.chatId, `started_pid_${pid}`);
      return ctx.send(
        `\u{2705} Knowledge Intake started (PID ${pid}). http://localhost:${KI_PORT}/`
      );
    } else if (loaded2) {
      await auditLog(ctx.userId, "ki", ["start"], ctx.chatId, "loaded_no_pid");
      return ctx.send(
        `\u{2705} Knowledge Intake launchd job loaded (polling service, may not have a persistent listener on :${KI_PORT}).`
      );
    } else {
      await auditLog(ctx.userId, "ki", ["start"], ctx.chatId, "start_failed");
      return ctx.send(
        `\u{274C} Could not start Knowledge Intake. Check plist at \`${KI_SERVER_PLIST}\`.`
      );
    }
  }

  if (sub === "stop") {
    const loaded = await kiLaunchdLoaded();
    const pid = await kiGetPid();

    if (!loaded && !pid) {
      return ctx.reply("Knowledge Intake is not running.");
    }

    await new Promise((resolve) => {
      exec(
        `launchctl bootout gui/${uid}/ai.watson.knowledge-intake-server 2>/dev/null; launchctl bootout gui/${uid}/ai.watson.knowledge-intake-poll 2>/dev/null`,
        { timeout: 10000 },
        () => resolve()
      );
    });

    if (pid) {
      await new Promise((resolve) => {
        exec(`kill ${pid} 2>/dev/null`, { timeout: 5000 }, () => resolve());
      });
    }

    await new Promise((r) => setTimeout(r, 2000));

    const check = await kiGetPid();
    if (check) {
      await auditLog(ctx.userId, "ki", ["stop"], ctx.chatId, "stop_failed");
      return ctx.reply(`\u{26A0}\uFE0F Process still running (PID ${check}). May need manual kill.`);
    }

    await auditLog(ctx.userId, "ki", ["stop"], ctx.chatId, "stopped");
    return ctx.reply("\u{2705} Knowledge Intake stopped.");
  }

  if (sub === "status") {
    const pid = await kiGetPid();
    const loaded = await kiLaunchdLoaded();

    const lines = [
      `\u{1F4CA} **Knowledge Intake**`,
      "",
      `**Status:** ${pid ? `\u{1F7E2} Running (PID ${pid})` : loaded ? "\u{1F7E1} Loaded (no listener)" : "\u{1F534} Stopped"}`,
      `**URL:** http://localhost:${KI_PORT}/`,
      `**Server:** ${loaded ? "loaded" : "not loaded"}`,
      `**Plist:** \`${KI_SERVER_PLIST}\``,
    ];

    await auditLog(ctx.userId, "ki", ["status"], ctx.chatId, pid ? `running_${pid}` : loaded ? "loaded" : "stopped");
    return ctx.reply(lines.join("\n"));
  }

  return ctx.reply(
    `Usage: \`${ctx.prefix}ki [start|stop|status]\``
  );
}

// ---------------------------------------------------------------------------
// Watson Knowledge Graph (localhost:4444)
// ---------------------------------------------------------------------------

const WATSON_GRAPH_PORT = 4444;
const WATSON_GRAPH_TOKEN = process.env.WATSON_GRAPH_TOKEN || null;
const WATSON_GRAPH_SCRIPT = path.join(
  OPENCLAW_DIR,
  "agents/main/workspace/scripts/memory-graph-server.js",
);

async function wgGetPid() {
  try {
    const { stdout } = await execPromise(`lsof -ti :${WATSON_GRAPH_PORT} 2>/dev/null`);
    const pid = stdout.trim().split("\n")[0]?.trim();
    return pid && /^\d+$/.test(pid) ? pid : null;
  } catch {
    return null;
  }
}

async function handleWatsonGraph(ctx, args) {
  const sub = (args[0] || "status").toLowerCase();

  if (sub === "start") {
    // Security: require explicit token, no hardcoded default
    if (!WATSON_GRAPH_TOKEN) {
      await auditLog(ctx.userId, "watson-graph", ["start"], ctx.chatId, "missing_token");
      return ctx.reply(
        "\u{274C} Cannot start Watson Knowledge Graph: `WATSON_GRAPH_TOKEN` env var is not set.\n" +
        "Set it in the launchd plist or environment before starting."
      );
    }

    const existing = await wgGetPid();
    if (existing) {
      return ctx.reply(
        `Watson Knowledge Graph is already running (PID ${existing}).\nhttp://localhost:${WATSON_GRAPH_PORT}/`
      );
    }

    await ctx.reply("\u{1F680} Starting Watson Knowledge Graph...");

    const child = require("child_process").spawn(
      "node",
      [WATSON_GRAPH_SCRIPT],
      {
        detached: true,
        stdio: ["ignore", "pipe", "pipe"],
        env: { ...process.env, HOME: process.env.HOME, WATSON_GRAPH_TOKEN },
      }
    );

    child.unref();
    // Wait for server to start
    await new Promise((r) => setTimeout(r, 3000));

    const pid = await wgGetPid();
    if (pid) {
      await auditLog(ctx.userId, "watson-graph", ["start"], ctx.chatId, `started_pid_${pid}`);
      return ctx.send(`\u{2705} Watson Knowledge Graph started (PID ${pid}).\nhttp://localhost:${WATSON_GRAPH_PORT}/`);
    } else {
      await auditLog(ctx.userId, "watson-graph", ["start"], ctx.chatId, "start_failed");
      return ctx.send(
        "\u{274C} Watson Knowledge Graph may not have started. Check the script at `scripts/memory-graph-server.js`."
      );
    }
  }

  if (sub === "stop") {
    const pid = await wgGetPid();
    if (!pid) {
      return ctx.reply("Watson Knowledge Graph is not running.");
    }

    await new Promise((resolve) => {
      exec(`kill ${pid} 2>/dev/null`, { timeout: 5000 }, () => resolve());
    });

    await new Promise((r) => setTimeout(r, 2000));

    const check = await wgGetPid();
    if (check) {
      await auditLog(ctx.userId, "watson-graph", ["stop"], ctx.chatId, "stop_failed");
      return ctx.reply(`\u{26A0}\uFE0F Process still running (PID ${check}). May need \`kill -9 ${check}\`.`);
    }

    await auditLog(ctx.userId, "watson-graph", ["stop"], ctx.chatId, "stopped");
    return ctx.reply("\u{2705} Watson Knowledge Graph stopped.");
  }

  if (sub === "status") {
    const pid = await wgGetPid();
    const lines = [
      `\u{1F9E0} **Watson Knowledge Graph**`,
      "",
      `**Status:** ${pid ? `\u{1F7E2} Running (PID ${pid})` : "\u{1F534} Stopped"}`,
      `**URL:** http://localhost:${WATSON_GRAPH_PORT}/`,
      `**Script:** \`${WATSON_GRAPH_SCRIPT}\``,
    ];

    await auditLog(ctx.userId, "watson-graph", ["status"], ctx.chatId, pid ? `running_${pid}` : "stopped");
    return ctx.reply(lines.join("\n"));
  }

  return ctx.reply(
    `Usage: \`${ctx.prefix}watson-graph [start|stop|status]\``
  );
}

// ---------------------------------------------------------------------------
// Session GC — automated garbage collection for sessions + gateway memory
// ---------------------------------------------------------------------------

/**
 * Get the RSS (resident set size) of the gateway process in MB.
 * Returns null if gateway is not running.
 */
function getGatewayRssMb() {
  return new Promise((resolve) => {
    exec(
      `ps -p $(pgrep -f openclaw-gateway | head -1) -o rss= 2>/dev/null`,
      { timeout: 5000 },
      (err, stdout) => {
        if (err) return resolve(null);
        const kb = parseInt(stdout.trim(), 10);
        resolve(isNaN(kb) ? null : Math.round(kb / 1024));
      }
    );
  });
}

/**
 * Get the set of active cron job IDs from the gateway's jobs.json.
 */
async function getActiveCronIds() {
  try {
    const raw = await fsp.readFile(CRON_JOBS_PATH, "utf-8");
    const data = JSON.parse(raw);
    const jobs = data.jobs || [];
    return new Set(jobs.filter((j) => j.enabled !== false).map((j) => j.id));
  } catch {
    return new Set();
  }
}

/**
 * Run session garbage collection for one agent.
 * Returns { prunedEntries, archivedFiles }.
 */
async function gcAgent(agentId, activeCronIds) {
  const sessionsDir = path.join(AGENTS_DIR, agentId, "sessions");
  const sessionsFile = path.join(sessionsDir, "sessions.json");
  let prunedEntries = 0;
  let archivedFiles = 0;

  // --- Phase 1: Prune sessions.json entries ---
  let data;
  try {
    const raw = await fsp.readFile(sessionsFile, "utf-8");
    data = JSON.parse(raw);
    if (typeof data !== "object" || Array.isArray(data)) return { prunedEntries: 0, archivedFiles: 0 };
  } catch {
    return { prunedEntries: 0, archivedFiles: 0 };
  }

  const now = Date.now();
  const cleaned = {};
  const keptSessionIds = new Set();

  for (const [key, entry] of Object.entries(data)) {
    let keep = true;

    if (key.includes(":cron:") && key.includes(":run:")) {
      // Ephemeral cron run — remove if older than threshold
      const updated = entry.updatedAt || entry.createdAt || 0;
      if (now - updated > GC_CRON_RUN_MAX_AGE_MS) keep = false;
    } else if (key.includes(":cron:") && !key.includes(":run:")) {
      // Cron base session — remove if cron job no longer exists
      const cronIdMatch = key.match(/:cron:([a-f0-9-]+)/);
      if (cronIdMatch && !activeCronIds.has(cronIdMatch[1])) keep = false;
    } else if (key.includes(":subagent:")) {
      // Subagent sessions — remove if older than 24h
      const updated = entry.updatedAt || entry.createdAt || 0;
      if (now - updated > 24 * 3600 * 1000) keep = false;
    }

    // Also prune entries whose transcript file is missing
    if (keep && entry.sessionId) {
      const jsonlPath = path.join(sessionsDir, `${entry.sessionId}.jsonl`);
      try {
        await fsp.access(jsonlPath);
      } catch {
        // Transcript gone — prune the entry unless it's a Discord/Telegram session
        // (those auto-recreate and the missing file is normal after reset)
        if (!key.includes("discord:") && !key.includes("telegram:")) {
          keep = false;
        }
      }
    }

    if (keep) {
      cleaned[key] = entry;
      if (entry.sessionId) keptSessionIds.add(entry.sessionId);
      // Also track sessionFile basenames
      if (entry.sessionFile) keptSessionIds.add(path.basename(entry.sessionFile, ".jsonl"));
    } else {
      prunedEntries++;
    }
  }

  // Write cleaned sessions.json if anything changed
  if (prunedEntries > 0) {
    const lockDir = await acquireLock(sessionsFile).catch((err) => {
      console.warn(`[rescue] GC: Lock acquisition failed for ${path.basename(path.dirname(sessionsFile))}, skipping prune: ${err.message || err}`);
      return null;
    });
    if (lockDir) {
      try {
        const tmpFile = sessionsFile + ".tmp";
        await fsp.writeFile(tmpFile, JSON.stringify(cleaned, null, 2));
        await fsp.rename(tmpFile, sessionsFile);
      } finally {
        await releaseLock(lockDir);
      }
    }
  }

  // --- Phase 2: Archive orphaned .jsonl files ---
  const archiveDir = path.join(sessionsDir, `_gc-${new Date().toISOString().slice(0, 10)}`);
  let entries;
  try {
    entries = await fsp.readdir(sessionsDir);
  } catch {
    return { prunedEntries, archivedFiles: 0 };
  }

  for (const fname of entries) {
    if (!fname.endsWith(".jsonl")) continue;
    const sessionId = fname.replace(".jsonl", "");
    if (keptSessionIds.has(sessionId)) continue;
    if (keptSessionIds.has(fname)) continue;

    // Check file age — don't archive recent files
    const filePath = path.join(sessionsDir, fname);
    try {
      const stat = await fsp.stat(filePath);
      const ageS = (Date.now() - stat.mtimeMs) / 1000;
      if (ageS < GC_ORPHAN_MIN_AGE_S) continue;

      await fsp.mkdir(archiveDir, { recursive: true });
      await fsp.rename(filePath, path.join(archiveDir, fname));
      archivedFiles++;
    } catch {
      // Skip files we can't stat/move
    }
  }

  // --- Phase 3: Clean up old .deleted.* files (>7 days) ---
  for (const fname of entries) {
    if (!fname.includes(".deleted.")) continue;
    const filePath = path.join(sessionsDir, fname);
    try {
      const stat = await fsp.stat(filePath);
      if (Date.now() - stat.mtimeMs > 7 * 24 * 3600 * 1000) {
        await fsp.unlink(filePath);
      }
    } catch {
      // Skip
    }
  }

  // --- Phase 4: Delete archived .jsonl files >30 days, then remove empty dirs ---
  const thirtyDaysMs = 30 * 24 * 3600 * 1000;
  const cutoff = Date.now() - thirtyDaysMs;
  for (const fname of entries) {
    if (!fname.startsWith("_gc-") && !fname.startsWith("_orphaned-")) continue;
    const dirPath = path.join(sessionsDir, fname);
    try {
      const stat = await fsp.stat(dirPath);
      if (!stat.isDirectory()) continue;

      // First pass: delete .jsonl files older than 30 days inside the archive dir
      const contents = await fsp.readdir(dirPath);
      for (const file of contents) {
        if (!file.endsWith(".jsonl")) continue;
        const filePath = path.join(dirPath, file);
        try {
          const fileStat = await fsp.stat(filePath);
          if (fileStat.mtimeMs < cutoff) {
            await fsp.unlink(filePath);
          }
        } catch {
          // Skip
        }
      }

      // Second pass: remove the dir if it's now empty
      const remaining = await fsp.readdir(dirPath);
      if (remaining.length === 0) {
        await fsp.rmdir(dirPath);
      }
    } catch {
      // Skip
    }
  }

  return { prunedEntries, archivedFiles };
}

/**
 * Write a machine-readable status file for dashboards.
 */
async function writeStatusFile(result) {
  const status = {
    timestamp: new Date().toISOString(),
    gateway: {
      pid: watchdogState.lastPid,
      status: watchdogState.status,
      rssMb: result.rssMb,
    },
    gc: {
      lastRun: new Date(gcState.lastRun).toISOString(),
      prunedEntries: result.totalPruned,
      archivedFiles: result.totalArchived,
      lifetimePruned: gcState.totalPruned,
      lifetimeArchived: gcState.totalArchived,
      lifetimeGatewayRestarts: gcState.gatewayRestarts,
    },
    agents: result.agentSummaries,
  };
  try {
    await fsp.writeFile(GC_STATUS_FILE, JSON.stringify(status, null, 2));
  } catch (err) {
    console.error("[rescue] Failed to write status file:", err.message);
  }
}

/**
 * Main GC loop — runs every GC_INTERVAL.
 */
async function runSessionGC() {
  const startTime = Date.now();
  const agents = await listAgentIds();
  const activeCronIds = await getActiveCronIds();
  let totalPruned = 0;
  let totalArchived = 0;
  const agentSummaries = {};

  for (const agentId of agents) {
    try {
      const { prunedEntries, archivedFiles } = await gcAgent(agentId, activeCronIds);
      totalPruned += prunedEntries;
      totalArchived += archivedFiles;

      // Count remaining sessions for status
      const sessionsFile = path.join(AGENTS_DIR, agentId, "sessions", "sessions.json");
      try {
        const raw = await fsp.readFile(sessionsFile, "utf-8");
        const data = JSON.parse(raw);
        agentSummaries[agentId] = Object.keys(data).length;
      } catch {
        agentSummaries[agentId] = 0;
      }
    } catch (err) {
      console.error(`[rescue] GC error for ${agentId}:`, err.message);
    }
  }

  gcState.totalPruned += totalPruned;
  gcState.totalArchived += totalArchived;

  // --- Session health monitoring (main agent only) ---
  try {
    const mainSessionsFile = path.join(AGENTS_DIR, "main", "sessions", "sessions.json");
    const mainRaw = await fsp.readFile(mainSessionsFile, "utf-8");
    const mainData = JSON.parse(mainRaw);
    const mainEntries = Object.entries(mainData);
    const now = Date.now();
    const hotSessions = [];

    for (const [key, entry] of mainEntries) {
      const usage = getUsageInfo(entry);
      if (usage.pct >= GC_CONTEXT_ALERT_PCT) {
        const lastAlert = gcState.contextAlertTimes[entry.sessionId] || 0;
        if (now - lastAlert > GC_ALERT_COOLDOWN_MS) {
          const label = entry.displayName || entry.origin?.label || key.split(":").slice(-1)[0];
          hotSessions.push({ key, sessionId: entry.sessionId, label, pct: usage.pct, status: usage.status, emoji: usage.emoji, total: usage.total });
          gcState.contextAlertTimes[entry.sessionId] = now;
        }
      }
    }

    // Session count alert
    const mainCount = mainEntries.length;
    let countAlert = false;
    if (mainCount > GC_SESSION_COUNT_ALERT && now - gcState.lastCountAlert > GC_ALERT_COOLDOWN_MS) {
      countAlert = true;
      gcState.lastCountAlert = now;
    }

    // Emit alerts
    if (hotSessions.length > 0 || countAlert) {
      const lines = [];
      if (hotSessions.length > 0) {
        lines.push(`\u{1F6A8} **Session Health Alert** (Watson/main)`);
        for (const s of hotSessions) {
          lines.push(`${s.emoji} **${s.label}** — ${s.pct}% context (${Math.round(s.total / 1000)}K tokens)`);
          console.log(`[rescue] Health: session ${s.sessionId} at ${s.pct}% — ${s.label}`);
        }
        lines.push(`_Run \`!handoff watson\` then \`!reset watson\` for bloated sessions._`);
      }
      if (countAlert) {
        lines.push(`\u{26A0}\u{FE0F} **Session count: ${mainCount}** (threshold: ${GC_SESSION_COUNT_ALERT})`);
        console.log(`[rescue] Health: main agent has ${mainCount} sessions (threshold: ${GC_SESSION_COUNT_ALERT})`);
      }

      if (GC_HEALTH_DRY_RUN) {
        console.log(`[rescue] Health (DRY RUN — would alert): ${lines.join(" | ")}`);
      } else {
        await sendOpsAlert(lines.join("\n"));
        // Emit context bloat alerts to bus
        for (const s of hotSessions) {
          emitBusEvent(
            "rescue-bot",
            "context_bloat_alert",
            `${s.label}: ${s.pct}% context (${Math.round(s.total / 1000)}K tokens)`,
            { session_id: s.sessionId, label: s.label, context_pct: s.pct, total_tokens: s.total },
            "session"
          );
        }
        if (countAlert) {
          emitBusEvent(
            "rescue-bot",
            "escalation",
            `Main agent session count: ${mainCount} (threshold: ${GC_SESSION_COUNT_ALERT})`,
            { session_count: mainCount, threshold: GC_SESSION_COUNT_ALERT },
            "session"
          );
        }
      }
    }

    // Prune stale cooldown entries (sessions that no longer exist)
    for (const sid of Object.keys(gcState.contextAlertTimes)) {
      if (!mainEntries.some(([, e]) => e.sessionId === sid)) {
        delete gcState.contextAlertTimes[sid];
      }
    }
  } catch (err) {
    console.error("[rescue] Health check error:", err.message);
  }

  // Check gateway RSS and restart if bloated
  const rssMb = await getGatewayRssMb();
  let gatewayRestarted = false;

  if (rssMb !== null && rssMb > GC_GATEWAY_RSS_RESTART_MB) {
    // Check for in-flight work before restarting
    let inFlightCount = 0;
    let inFlightNames = [];
    try {
      const cronData = JSON.parse(require("fs").readFileSync(
        require("path").join(require("os").homedir(), ".openclaw/cron/jobs.json"), "utf8"
      ));
      const runningJobs = (cronData.jobs || []).filter(j => j.state && j.state.runningAtMs);
      inFlightCount += runningJobs.length;
      inFlightNames.push(...runningJobs.map(j => j.name));
    } catch (_) {}

    if (inFlightCount > 0 && gcState.consecutiveDeferrals < 2) {
      // Defer only up to 2 cycles — if RSS is still high after that,
      // the in-flight job is likely the cause (runaway/stuck), so force restart.
      gcState.consecutiveDeferrals = (gcState.consecutiveDeferrals || 0) + 1;
      const names = inFlightNames.join(", ");
      console.log(`[rescue] GC: RSS ${rssMb}MB exceeds threshold but ${inFlightCount} job(s) in flight (${names}) — deferring restart (deferral #${gcState.consecutiveDeferrals})`);
      await sendOpsAlert(
        `⚠️ **Session GC: Restart deferred (${gcState.consecutiveDeferrals}/2)**\n` +
        `RSS ${rssMb}MB exceeds ${GC_GATEWAY_RSS_RESTART_MB}MB threshold, but ${inFlightCount} cron job(s) are running:\n` +
        `\`${names}\`\n` +
        `_Will force-restart on next cycle if RSS is still high._`
      );
    } else {
      if (gcState.consecutiveDeferrals >= 2 && inFlightCount > 0) {
        const names = inFlightNames.join(", ");
        console.log(`[rescue] GC: RSS ${rssMb}MB still high after ${gcState.consecutiveDeferrals} deferrals — forcing restart (in-flight: ${names})`);
        await sendOpsAlert(
          `🚨 **Session GC: Forced restart** (RSS still high after ${gcState.consecutiveDeferrals} deferrals)\n` +
          `RSS ${rssMb}MB — in-flight jobs may be the cause:\n\`${names}\`\n` +
          `_Restarting anyway. Check these jobs for runaway behavior._`
        );
      }
      gcState.consecutiveDeferrals = 0;
      console.log(`[rescue] GC: Gateway RSS ${rssMb}MB exceeds ${GC_GATEWAY_RSS_RESTART_MB}MB — restarting`);
      gcState.gatewayRestarts++;
      gatewayRestarted = true;

      await stopGateway();

      const alertMsg =
        `\u267B\uFE0F **Session GC: Gateway auto-restarted**\n` +
        `RSS was ${rssMb}MB (threshold: ${GC_GATEWAY_RSS_RESTART_MB}MB)\n` +
        `Pruned ${totalPruned} sessions, archived ${totalArchived} files\n` +
        `_Launchd will auto-restart the gateway._`;
      await sendOpsAlert(alertMsg);
      // Emit escalation — bus-tail will forward this
      emitBusEvent(
        "rescue-bot",
        "escalation",
        `Gateway auto-restarted: RSS ${rssMb}MB exceeded ${GC_GATEWAY_RSS_RESTART_MB}MB`,
        { rss_mb: rssMb, threshold_mb: GC_GATEWAY_RSS_RESTART_MB, pruned: totalPruned, archived: totalArchived },
        "gateway"
      );
    } // end else (no in-flight jobs)
  }

  const result = {
    totalPruned,
    totalArchived,
    rssMb,
    gatewayRestarted,
    durationMs: Date.now() - startTime,
    agentSummaries,
  };

  gcState.lastRun = Date.now();
  gcState.lastResult = result;

  await writeStatusFile(result);

  if (totalPruned > 0 || totalArchived > 0 || gatewayRestarted) {
    console.log(
      `[rescue] GC: pruned=${totalPruned} archived=${totalArchived} rss=${rssMb || "?"}MB restarted=${gatewayRestarted} (${result.durationMs}ms)`
    );
    // Emit GC completion to bus (only when something happened)
    emitBusEvent(
      "rescue-bot",
      "health_check",
      `GC cycle: pruned ${totalPruned}, archived ${totalArchived}, RSS ${rssMb || "?"}MB`,
      { pruned: totalPruned, archived: totalArchived, rss_mb: rssMb, gateway_restarted: gatewayRestarted, duration_ms: result.durationMs },
      "gc"
    );
  }

  return result;
}

async function handleGc(ctx, args) {
  const sub = args[0]?.toLowerCase();

  if (sub === "run") {
    await ctx.reply("\u267B\uFE0F Running session GC...");
    try {
      const result = await runSessionGC();
      const lines = [
        `\u2705 **Session GC complete** (${result.durationMs}ms)`,
        "",
        `**Pruned:** ${result.totalPruned} session entries`,
        `**Archived:** ${result.totalArchived} orphaned files`,
        `**Gateway RSS:** ${result.rssMb || "unknown"}MB${result.gatewayRestarted ? " \u2192 **restarted**" : ""}`,
      ];

      if (Object.keys(result.agentSummaries).length > 0) {
        lines.push("", "**Remaining sessions per agent:**");
        for (const [agent, count] of Object.entries(result.agentSummaries)) {
          if (count > 0) lines.push(`  ${agent}: ${count}`);
        }
      }

      await auditLog(ctx.userId, "gc", ["run"], ctx.chatId, `pruned=${result.totalPruned} archived=${result.totalArchived}`);
      return ctx.reply(lines.join("\n"));
    } catch (err) {
      await auditLog(ctx.userId, "gc", ["run"], ctx.chatId, `error: ${err.message}`);
      return ctx.reply(`\u274C GC failed: ${err.message}`);
    }
  }

  // Default: show GC status
  const rssMb = await getGatewayRssMb();
  const lines = [
    `\u267B\uFE0F **Session GC Status**`,
    "",
    `**Gateway RSS:** ${rssMb || "unknown"}MB ${rssMb && rssMb > GC_GATEWAY_RSS_RESTART_MB ? "\u26A0\uFE0F above threshold" : rssMb ? "\u2705" : ""}`,
    `**Auto-restart threshold:** ${GC_GATEWAY_RSS_RESTART_MB}MB`,
    `**GC interval:** every ${GC_INTERVAL / 60000} min`,
    `**Last GC:** ${gcState.lastRun ? `${Math.round((Date.now() - gcState.lastRun) / 60000)} min ago` : "never"}`,
  ];

  if (gcState.lastResult) {
    const r = gcState.lastResult;
    lines.push(
      "",
      "**Last run:**",
      `  Pruned: ${r.totalPruned} entries, Archived: ${r.totalArchived} files`,
      `  Duration: ${r.durationMs}ms${r.gatewayRestarted ? ", gateway restarted" : ""}`
    );
  }

  lines.push(
    "",
    "**Lifetime:**",
    `  Pruned: ${gcState.totalPruned} entries`,
    `  Archived: ${gcState.totalArchived} files`,
    `  Gateway restarts: ${gcState.gatewayRestarts}`,
    "",
    `_Use \`${ctx.prefix}gc run\` to trigger manually._`
  );

  await auditLog(ctx.userId, "gc", [], ctx.chatId, `rss=${rssMb}`);
  return ctx.reply(lines.join("\n"));
}

// ---------------------------------------------------------------------------
// Command router — shared by both platforms
// ---------------------------------------------------------------------------

async function routeCommand(ctx, cmd, args) {
  if (!checkCooldown(cmd)) return;

  try {
    switch (cmd) {
      case "ping":
        await handlePing(ctx);
        break;
      case "status":
        await handleStatus(ctx);
        break;
      case "panic":
        await handlePanic(ctx);
        break;
      case "resume":
        await handleResume(ctx);
        break;
      case "atlas":
        await handleAtlas(ctx);
        break;
      case "reset":
        await handleReset(ctx, args);
        break;
      case "restart":
        await handleRestart(ctx, args);
        break;
      case "model":
        await handleModel(ctx, args);
        break;
      case "mute":
        await handleMute(ctx, args, true);
        break;
      case "unmute":
        await handleMute(ctx, args, false);
        break;
      case "backup":
        await handleBackup(ctx);
        break;
      case "rollback":
        await handleRollback(ctx, args);
        break;
      case "handoff":
        await handleHandoff(ctx, args);
        break;
      case "mc":
        await handleMc(ctx, args);
        break;
      case "ki":
        await handleKi(ctx, args);
        break;
      case "watson-graph":
        await handleWatsonGraph(ctx, args);
        break;
      case "gc":
        await handleGc(ctx, args);
        break;
      case "swap":
        await handleSwap(ctx, args);
        break;
      case "swap-order":
        await handleSwapOrder(ctx, args);
        break;
      case "wcc":
        await handleWcc(ctx, args);
        break;
      case "help":
        await handleHelp(ctx);
        break;
      // Silently ignore unknown commands
    }
  } catch (err) {
    console.error(`[rescue] Error handling ${ctx.prefix}${cmd}:`, err);
    await auditLog(ctx.userId, cmd, args, ctx.chatId, `error: ${err.message}`);
    try {
      await ctx.reply(
        "An error occurred processing that command. Check logs for details."
      );
    } catch {
      // Can't reply
    }
  }
}

// ---------------------------------------------------------------------------
// Platform clients
// ---------------------------------------------------------------------------

let discordClient = null;
let telegramBot = null;

/** Start background monitors (called once when first platform connects). */
let monitorsStarted = false;
function startMonitors() {
  if (monitorsStarted) return;
  monitorsStarted = true;

  // Stall detector — gateway sessions + CC tmux sessions
  setInterval(() => {
    checkForStalledSessions().catch((err) => {
      console.error("[rescue] Stall check error:", err.message);
    });
    checkForStalledCcSessions().catch((err) => {
      console.error("[rescue] CC stall check error:", err.message);
    });
  }, STALL_CHECK_INTERVAL);
  console.log(
    `[rescue] Stall detector active (${STALL_CHECK_INTERVAL / 1000}s interval, ${STALL_THRESHOLD_MS / 60000}min threshold, gateway + CC)`
  );

  // Gateway watchdog
  setInterval(() => {
    checkGatewayHealth().catch((err) => {
      console.error("[rescue] Watchdog check error:", err.message);
    });
  }, WATCHDOG_INTERVAL);
  checkGatewayHealth().catch(() => {});
  console.log(
    `[rescue] Watchdog active (${WATCHDOG_INTERVAL / 1000}s interval, alert on ${WATCHDOG_CRASH_THRESHOLD}+ restarts in ${WATCHDOG_CRASH_WINDOW / 60000}min)`
  );

  // Websocket heartbeat — touch a file when the Discord gateway is READY so
  // an external substrate-only monitor can detect drops without a round-trip
  // dependency on the bot it's checking.
  const writeWsHeartbeat = async () => {
    if (!discordClient || discordClient.ws?.status !== 0) return;
    try {
      await fsp.mkdir(path.dirname(WS_HEARTBEAT_FILE), { recursive: true });
      await fsp.writeFile(
        WS_HEARTBEAT_FILE,
        `${new Date().toISOString()} pid=${process.pid} ws=READY\n`
      );
    } catch (err) {
      console.error("[rescue] WS heartbeat write failed:", err.message);
    }
  };
  writeWsHeartbeat();
  setInterval(writeWsHeartbeat, WS_HEARTBEAT_INTERVAL_MS);
  console.log(
    `[rescue] WS heartbeat active (${WS_HEARTBEAT_INTERVAL_MS / 1000}s interval → ${WS_HEARTBEAT_FILE})`
  );

  // Session GC — runs every 30min, prunes dead sessions, archives orphans, checks gateway RSS
  setInterval(() => {
    runSessionGC().catch((err) => {
      console.error("[rescue] Session GC error:", err.message);
    });
  }, GC_INTERVAL);
  // Run initial GC after 60s (let gateway stabilize first)
  setTimeout(() => {
    runSessionGC().catch((err) => {
      console.error("[rescue] Initial GC error:", err.message);
    });
  }, 60 * 1000);
  console.log(
    `[rescue] Session GC active (${GC_INTERVAL / 60000}min interval, RSS restart at ${GC_GATEWAY_RSS_RESTART_MB}MB)`
  );
}

// --- Discord ---

if (DISCORD_TOKEN) {
  discordClient = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.GuildMessageReactions,
      GatewayIntentBits.MessageContent,
      GatewayIntentBits.DirectMessages,
      GatewayIntentBits.DirectMessageReactions,
    ],
  });

  discordClient.once("clientReady", () => {
    console.log(`[rescue] Discord: logged in as ${discordClient.user.tag}`);
    console.log(`[rescue] Discord: authorized user ${DISCORD_ADMIN_ID}`);
    if (Object.keys(AGENT_ALIASES).length > 0) {
      console.log(
        `[rescue] Agent aliases: ${JSON.stringify(AGENT_ALIASES)}`
      );
    }
    startMonitors();
  });

  discordClient.on("messageCreate", async (message) => {
    if (message.author.bot) return;
    if (!message.content.startsWith(DISCORD_PREFIX)) return;
    if (!DISCORD_ADMIN_ID || message.author.id !== DISCORD_ADMIN_ID) return;

    const [command, ...args] = message.content
      .slice(DISCORD_PREFIX.length)
      .trim()
      .split(/\s+/);

    const ctx = fromDiscord(message);
    await routeCommand(ctx, command.toLowerCase(), args);
  });

  discordClient.login(DISCORD_TOKEN).catch((err) => {
    console.error("[rescue] Discord login failed:", err.message);
  });
} else {
  console.log("[rescue] Discord: disabled (no DISCORD_BOT_TOKEN)");
}

// --- Telegram ---

if (TELEGRAM_TOKEN) {
  telegramBot = new Bot(TELEGRAM_TOKEN);

  telegramBot.on("message:text", async (tgCtx) => {
    const text = tgCtx.message.text;
    if (!text.startsWith(TELEGRAM_PREFIX)) return;
    if (
      !TELEGRAM_ADMIN_ID ||
      String(tgCtx.from?.id) !== String(TELEGRAM_ADMIN_ID)
    )
      return;

    // Strip bot mention from commands (Telegram appends @botname)
    let rawCmd = text.slice(TELEGRAM_PREFIX.length).trim();
    const [commandPart, ...args] = rawCmd.split(/\s+/);
    const cmd = commandPart.replace(/@\S+$/, "").toLowerCase();

    const ctx = fromTelegram(tgCtx);
    await routeCommand(ctx, cmd, args);
  });

  telegramBot
    .start({
      onStart: (botInfo) => {
        console.log(`[rescue] Telegram: logged in as @${botInfo.username}`);
        console.log(`[rescue] Telegram: authorized user ${TELEGRAM_ADMIN_ID}`);
        startMonitors();
      },
    })
    .catch((err) => {
      console.error("[rescue] Telegram start failed:", err.message);
    });
} else {
  console.log("[rescue] Telegram: disabled (no TELEGRAM_BOT_TOKEN)");
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

function shutdown(signal) {
  console.log(`[rescue] Received ${signal}, shutting down...`);
  if (discordClient) discordClient.destroy();
  if (telegramBot) telegramBot.stop();
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
