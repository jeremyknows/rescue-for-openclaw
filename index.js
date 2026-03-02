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
 *   !status             — Show session health for this channel's agent
 *   !status all         — Show all sessions across all agents
 *   !restart gateway    — Restart the OpenClaw gateway (clears provider cooldowns)
 *   !start <message>    — Kick off a conversation with the agent in this channel
 *   !model <alias>      — Override model for this session
 *   !model show         — Show current model override
 *   !model default      — Clear model override
 *   !mute [agent]       — Require @mention for agent (Discord only)
 *   !unmute [agent]     — Let agent respond to all messages (Discord only)
 *   !keys status        — Show auth-profile health across all agents
 *   !backup             — Snapshot the current gateway config
 *   !rollback           — Restore last known good config + restart gateway
 *   !rollback list      — Show recent config backups
 *   !watchdog           — Show gateway health and restart history
 *   !help               — Display command help
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
const { exec, execFile } = require("child_process");

// ---------------------------------------------------------------------------
// Config — all customizable via environment variables
// ---------------------------------------------------------------------------

const DISCORD_TOKEN =
  process.env.DISCORD_BOT_TOKEN || process.env.DISCORD_ADMIN_BOT_TOKEN;
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
const WATCHDOG_INTERVAL = 30 * 1000;
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

function getUsageInfo(entry) {
  const total = entry.totalTokens || 0;
  const context = entry.contextTokens || 200000;
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

// ---------------------------------------------------------------------------
// !status
// ---------------------------------------------------------------------------

async function handleStatus(ctx, args) {
  if (args[0] === "all") {
    const agents = await listAgentIds();
    if (agents.length === 0) {
      return ctx.reply("Could not read agents directory.");
    }

    const lines = [];
    let totalSessions = 0;

    for (const agentId of agents.sort()) {
      const result = await readSessionsJson(agentId);
      if (!result) continue;
      const entries = Object.entries(result.data);
      if (entries.length === 0) continue;
      totalSessions += entries.length;

      lines.push(`**${agentName(agentId)}** (${entries.length} sessions)`);

      const sorted = entries
        .map(([key, entry]) => ({ key, ...getUsageInfo(entry) }))
        .sort((a, b) => b.pct - a.pct)
        .slice(0, 3);

      for (const s of sorted) {
        const shortKey =
          s.key.length > 50 ? "..." + s.key.slice(-47) : s.key;
        lines.push(`  ${s.emoji} ${s.pct}% \u2014 \`${shortKey}\``);
      }
      if (entries.length > 3) {
        lines.push(`  _...and ${entries.length - 3} more_`);
      }
    }

    lines.unshift(
      `\u{1F4CA} **${totalSessions} sessions across ${agents.length} agents**\n`
    );
    await auditLog(ctx.userId, "status", ["all"], ctx.chatId, `${totalSessions}_sessions`);
    return ctx.reply(lines.join("\n"));
  }

  const session = await findSessionByChat(ctx.chatId, ctx.platform);
  if (!session) {
    return ctx.reply(
      `No OpenClaw session found for this channel. Use \`${ctx.prefix}status all\` to see everything.`
    );
  }

  const name = agentName(session.agentId);
  const usage = getUsageInfo(session.entry);
  const model = session.entry.model || "unknown";
  const modelProvider = session.entry.modelProvider || "";
  const fullModel = modelProvider ? `${modelProvider}/${model}` : model;
  const alias = await resolveModelAlias(fullModel);
  const modelDisplay = alias
    ? `\`${fullModel}\` (${alias})`
    : `\`${fullModel}\``;
  const override = session.entry.modelOverride;
  const overrideNote = override ? ` *(override active)*` : "";
  const updatedAt = session.entry.updatedAt
    ? new Date(session.entry.updatedAt).toLocaleString("en-US", {
        dateStyle: "short",
        timeStyle: "short",
      })
    : "unknown";

  await auditLog(ctx.userId, "status", [], ctx.chatId, `${name}_${usage.pct}pct`);
  return ctx.reply(
    [
      `${usage.emoji} **${name}** in this channel`,
      `Context: **${usage.pct}%** (${(usage.total / 1000).toFixed(0)}k / ${(usage.context / 1000).toFixed(0)}k tokens)`,
      `Model: ${modelDisplay}${overrideNote}`,
      `Last active: ${updatedAt}`,
      usage.pct >= 70
        ? `\n\u26A0\uFE0F Context is getting full. Use \`${ctx.prefix}reset\` to clear it.`
        : "",
    ]
      .filter(Boolean)
      .join("\n")
  );
}

// ---------------------------------------------------------------------------
// !start
// ---------------------------------------------------------------------------

async function handleStart(ctx, args) {
  if (args.length === 0) {
    return ctx.reply(
      `Usage: \`${ctx.prefix}start <message>\` \u2014 sends a prompt to the agent in this channel.`
    );
  }

  const session = await findSessionByChat(ctx.chatId, ctx.platform);
  if (!session) {
    return ctx.reply(
      "No OpenClaw session found for this channel. Is an agent active here?"
    );
  }

  const prompt = args.join(" ");
  const name = agentName(session.agentId);

  await ctx.reply(`\u{1F4AC} Sending to **${name}**...`);
  await auditLog(ctx.userId, "start", args, ctx.chatId, `session:${session.key}`);

  const bridgeScript = path.join(
    os.homedir(),
    ".claude",
    "skills",
    "ask-watson",
    "scripts",
    "ask-watson.js"
  );

  try {
    await fsp.access(bridgeScript);
  } catch {
    return ctx.send(
      `\u{274C} Bridge script not found at \`${bridgeScript}\`. ` +
        `Set up the OpenClaw bridge to use \`${ctx.prefix}start\`.`
    );
  }

  execFile(
    "node",
    [bridgeScript, "send", session.key, prompt],
    { timeout: 120000 },
    async (err) => {
      if (err && err.killed) return;
      if (err) {
        console.error(`[rescue] !start send error:`, err.message);
        try {
          await ctx.send(
            `\u{274C} Failed to reach ${name} \u2014 gateway may be down.`
          );
        } catch {
          /* channel unavailable */
        }
      }
    }
  );
}

// ---------------------------------------------------------------------------
// !restart gateway
// ---------------------------------------------------------------------------

async function handleRestart(ctx, args) {
  const target = args[0]?.toLowerCase();

  if (target !== "gateway") {
    return ctx.reply(`Usage: \`${ctx.prefix}restart gateway\``);
  }

  await ctx.reply(
    "\u{1F504} Restarting OpenClaw gateway (clears provider cooldowns)..."
  );
  await auditLog(ctx.userId, "restart", ["gateway"], ctx.chatId, "initiated");

  exec("pkill -f openclaw-gateway", async (err) => {
    if (err && err.code !== 1) {
      await ctx.send(
        `\u{274C} Could not kill gateway process: ${err.message}`
      );
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
  });
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

    await new Promise((resolve) =>
      exec("pkill -f openclaw-gateway", () => resolve())
    );
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

// ---------------------------------------------------------------------------
// !keys status
// ---------------------------------------------------------------------------

async function handleKeys(ctx, args) {
  const sub = args[0]?.toLowerCase();

  if (sub !== "status") {
    return ctx.reply(
      `Usage: \`${ctx.prefix}keys status\` \u2014 Show auth-profile health`
    );
  }

  const agents = await listAgentIds();
  const lines = [];
  let totalIssues = 0;

  for (const agentId of agents.sort()) {
    if (!isValidAgentId(agentId)) continue;
    const authPath = path.join(
      AGENTS_DIR,
      agentId,
      "agent",
      "auth-profiles.json"
    );
    try {
      const raw = await fsp.readFile(authPath, "utf-8");
      const data = JSON.parse(raw);
      const profiles = data.profiles || {};
      const stats = data.usageStats || {};
      const issues = [];

      for (const [profileId, profileStats] of Object.entries(stats)) {
        const errors = profileStats.errorCount || 0;
        const cooldown = profileStats.cooldownUntil;
        const disabled = profileStats.disabledUntil;
        if (errors > 0) issues.push(`${profileId}: ${errors} errors`);
        if (cooldown && cooldown > Date.now())
          issues.push(`${profileId}: in cooldown`);
        if (disabled && disabled > Date.now())
          issues.push(`${profileId}: disabled`);
      }

      const profileCount = Object.keys(profiles).length;
      if (issues.length > 0) {
        totalIssues += issues.length;
        lines.push(
          `\u{1F7E0} **${agentName(agentId)}** (${profileCount} profiles) \u2014 ${issues.join("; ")}`
        );
      } else {
        lines.push(
          `\u{1F7E2} **${agentName(agentId)}** (${profileCount} profiles) \u2014 OK`
        );
      }
    } catch {
      // No auth-profiles.json is normal for agents without API access
    }
  }

  const header =
    totalIssues > 0
      ? `\u{1F511} **Auth Profile Status** \u2014 ${totalIssues} issue${totalIssues !== 1 ? "s" : ""} found\n`
      : `\u{1F511} **Auth Profile Status** \u2014 all healthy\n`;

  await auditLog(ctx.userId, "keys", ["status"], ctx.chatId, `${totalIssues}_issues`);
  return ctx.reply(header + lines.join("\n"));
}

// ---------------------------------------------------------------------------
// !backup
// ---------------------------------------------------------------------------

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

  try {
    const raw = await fsp.readFile(backupPath, "utf-8");
    JSON.parse(raw);
  } catch {
    return ctx.reply(
      `\u{274C} Backup \`${targetFile}\` contains invalid JSON. Try a different backup.`
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

    await new Promise((resolve) =>
      exec("pkill -f openclaw-gateway", () => resolve())
    );
    await new Promise((r) => setTimeout(r, 2000));

    const tmpFile = CONFIG_PATH + ".tmp";
    await fsp.copyFile(backupPath, tmpFile);
    await fsp.rename(tmpFile, CONFIG_PATH);

    setTimeout(async () => {
      try {
        const raw = await fsp.readFile(CONFIG_PATH, "utf-8");
        const config = JSON.parse(raw);
        const agentCount = Object.keys(config.agents?.agents || {}).length;
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
          `\`${p}mute [agent]\` \u2014 Require @mention for agent in this channel`,
          `\`${p}unmute [agent]\` \u2014 Let agent respond to all messages`,
        ]
      : [];

  return ctx.reply(
    [
      "**Rescue \u2014 OpenClaw Admin Bot**",
      "",
      "**Session Management**",
      `\`${p}reset\` \u2014 Reset the agent session in this channel`,
      `\`${p}reset <agent>\` \u2014 Reset agent's session in this channel`,
      `\`${p}reset <agent> all\` \u2014 Reset ALL sessions for an agent everywhere`,
      `\`${p}status\` \u2014 Show session health for this channel`,
      `\`${p}status all\` \u2014 Show all sessions across all agents`,
      `\`${p}start <message>\` \u2014 Kick off a conversation with the agent`,
      "",
      "**Model & Config**",
      `\`${p}model <alias|name>\` \u2014 Override model for this session`,
      `\`${p}model show\` \u2014 Show current model override`,
      `\`${p}model default\` \u2014 Clear model override`,
      ...discordOnly,
      "",
      "**System**",
      `\`${p}restart gateway\` \u2014 Restart the OpenClaw gateway`,
      `\`${p}keys status\` \u2014 Show auth-profile health`,
      `\`${p}backup\` \u2014 Snapshot the current gateway config`,
      `\`${p}rollback\` \u2014 Restore the last known good config`,
      `\`${p}rollback list\` \u2014 Show available config backups`,
      `\`${p}watchdog\` \u2014 Show gateway health and restart history`,
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
    }
  }
}

// ---------------------------------------------------------------------------
// Gateway watchdog
// ---------------------------------------------------------------------------

function getGatewayPid() {
  return new Promise((resolve) => {
    exec(
      `pgrep -f "${GATEWAY_PROCESS_NAME}"`,
      { timeout: 5000 },
      (err, stdout) => {
        if (err) return resolve(null);
        const pids = stdout.trim().split("\n").filter(Boolean);
        resolve(pids.length > 0 ? parseInt(pids[0], 10) : null);
      }
    );
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

    console.log(
      `[rescue] Watchdog: crash-loop detected (${recentRestarts.length} restarts in ${WATCHDOG_CRASH_WINDOW / 60000}min)`
    );
    return;
  }

  watchdogState.status = "healthy";
}

async function handleWatchdog(ctx) {
  const now = Date.now();
  const pid = watchdogState.lastPid;
  const recentRestarts = watchdogState.restarts.filter(
    (t) => now - t < WATCHDOG_CRASH_WINDOW
  );
  const allRestarts = watchdogState.restarts;

  const statusEmoji = {
    healthy: "\u{1F7E2}",
    down: "\u{1F534}",
    "crash-loop": "\u{1F6A8}",
    cooldown: "\u{1F7E1}",
    starting: "\u{2B1C}",
  };

  const lines = [
    `${statusEmoji[watchdogState.status] || "\u2753"} **Gateway Watchdog** \u2014 ${watchdogState.status}`,
    "",
    `**Gateway PID:** ${pid || "not running"}`,
    `**Restarts (last ${WATCHDOG_CRASH_WINDOW / 60000}min):** ${recentRestarts.length}`,
    `**Total restarts tracked:** ${allRestarts.length}`,
  ];

  if (watchdogState.lastAlertTime > 0) {
    const ago = Math.round((now - watchdogState.lastAlertTime) / 60000);
    lines.push(`**Last alert:** ${ago} min ago`);
    const cooldownRemaining =
      WATCHDOG_ALERT_COOLDOWN - (now - watchdogState.lastAlertTime);
    if (cooldownRemaining > 0) {
      lines.push(
        `**Cooldown:** ${Math.round(cooldownRemaining / 60000)} min remaining`
      );
    }
  }

  if (recentRestarts.length > 0) {
    lines.push("");
    lines.push("**Recent restart times:**");
    for (const t of recentRestarts.slice(-5)) {
      const secsAgo = Math.round((now - t) / 1000);
      lines.push(`  \u2022 ${secsAgo}s ago`);
    }
  }

  lines.push("");
  lines.push(
    `_Polling every ${WATCHDOG_INTERVAL / 1000}s \u2022 Alert threshold: ${WATCHDOG_CRASH_THRESHOLD} restarts in ${WATCHDOG_CRASH_WINDOW / 60000}min_`
  );

  await auditLog(ctx.userId, "watchdog", [], ctx.chatId, watchdogState.status);
  return ctx.reply(lines.join("\n"));
}

// ---------------------------------------------------------------------------
// Command router — shared by both platforms
// ---------------------------------------------------------------------------

async function routeCommand(ctx, cmd, args) {
  if (!checkCooldown(cmd)) return;

  try {
    switch (cmd) {
      case "reset":
        await handleReset(ctx, args);
        break;
      case "status":
        await handleStatus(ctx, args);
        break;
      case "restart":
        await handleRestart(ctx, args);
        break;
      case "start":
        await handleStart(ctx, args);
        break;
      case "model":
        await handleModel(ctx, args);
        break;
      case "keys":
        await handleKeys(ctx, args);
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
      case "watchdog":
        await handleWatchdog(ctx);
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

  // Stall detector
  setInterval(() => {
    checkForStalledSessions().catch((err) => {
      console.error("[rescue] Stall check error:", err.message);
    });
  }, STALL_CHECK_INTERVAL);
  console.log(
    `[rescue] Stall detector active (${STALL_CHECK_INTERVAL / 1000}s interval, ${STALL_THRESHOLD_MS / 60000}min threshold)`
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
}

// --- Discord ---

if (DISCORD_TOKEN) {
  discordClient = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent,
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
