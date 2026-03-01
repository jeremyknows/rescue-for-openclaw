#!/usr/bin/env node
/**
 * Rescue — Discord Admin Bot for OpenClaw
 *
 * Standalone bot (independent of the OpenClaw gateway) that provides
 * admin commands for managing agent sessions directly from Discord.
 * Designed as a "rescue" tool — works even when the gateway is down.
 *
 * Commands:
 *   !reset              — Reset the agent session bound to this channel
 *   !reset <agent>      — Reset agent in this channel (supports aliases)
 *   !reset <agent> all  — Reset ALL sessions for an agent everywhere
 *   !status             — Show session health for this channel's agent
 *   !status all         — Show all sessions across all agents
 *   !restart gateway    — Restart the OpenClaw gateway (clears provider cooldowns)
 *   !start <message>    — Kick off a conversation with the agent in this channel
 *   !model <alias>      — Override model for this session
 *   !model show         — Show current model override
 *   !model default      — Clear model override
 *   !mute [agent]       — Require @mention for agent in this channel
 *   !unmute [agent]     — Let agent respond to all messages in channel
 *   !keys status        — Show auth-profile health across all agents
 *   !backup             — Snapshot the current gateway config
 *   !rollback           — Restore last known good config + restart gateway
 *   !rollback list      — Show recent config backups
 *   !help               — Display command help
 *
 * Environment (required):
 *   DISCORD_BOT_TOKEN         — Bot token (also accepts DISCORD_ADMIN_BOT_TOKEN)
 *   DISCORD_ADMIN_USER_ID     — Your Discord user ID for auth
 *
 * Environment (optional):
 *   OPENCLAW_DIR               — OpenClaw base directory (default: ~/.openclaw)
 *   RESCUE_PREFIX              — Command prefix (default: !)
 *   RESCUE_AGENT_ALIASES       — JSON map of aliases, e.g. {"watson":"main"}
 *   RESCUE_OPS_CHANNEL_ID      — Discord channel ID for system alerts
 *   RESCUE_STALL_MINUTES       — Minutes before stall alert (default: 15)
 *   RESCUE_MAX_BACKUPS         — Max config backups to keep (default: 20)
 */

const { Client, GatewayIntentBits } = require("discord.js");
const fsp = require("fs/promises");
const path = require("path");
const os = require("os");
const { exec, execFile } = require("child_process");

// ---------------------------------------------------------------------------
// Config — all customizable via environment variables
// ---------------------------------------------------------------------------

const BOT_TOKEN =
  process.env.DISCORD_BOT_TOKEN || process.env.DISCORD_ADMIN_BOT_TOKEN;
const ADMIN_USER_ID = process.env.DISCORD_ADMIN_USER_ID;
const OPENCLAW_DIR =
  process.env.OPENCLAW_DIR || path.join(os.homedir(), ".openclaw");
const AGENTS_DIR = path.join(OPENCLAW_DIR, "agents");
const LOGS_DIR = path.join(OPENCLAW_DIR, "logs");
const AUDIT_LOG = path.join(LOGS_DIR, "rescue-bot-audit.jsonl");
const CONFIG_PATH = path.join(OPENCLAW_DIR, "openclaw.json");
const BACKUP_DIR = path.join(OPENCLAW_DIR, "backups");
const PREFIX = process.env.RESCUE_PREFIX || "!";
const OPS_CHANNEL_ID = process.env.RESCUE_OPS_CHANNEL_ID || null;
const MAX_BACKUPS = parseInt(process.env.RESCUE_MAX_BACKUPS) || 20;

// Agent aliases — friendly names that resolve to agent directory IDs
// Set via RESCUE_AGENT_ALIASES='{"watson":"main","barker":"herald"}'
let AGENT_ALIASES = {};
try {
  if (process.env.RESCUE_AGENT_ALIASES) {
    AGENT_ALIASES = JSON.parse(process.env.RESCUE_AGENT_ALIASES);
  }
} catch (err) {
  console.error(
    "[rescue] Failed to parse RESCUE_AGENT_ALIASES:",
    err.message
  );
}

// Stall detection — configurable thresholds
const STALL_THRESHOLD_MS =
  (parseInt(process.env.RESCUE_STALL_MINUTES) || 15) * 60 * 1000;
const STALL_ACTIVE_WINDOW = 30 * 60 * 1000;
const STALL_CHECK_INTERVAL = 60 * 1000;
const STALL_ALERT_COOLDOWN = 60 * 60 * 1000;
const staleAlertTimes = new Map();

// Gateway watchdog — crash-loop detection
const WATCHDOG_INTERVAL = 30 * 1000; // Poll every 30s
const WATCHDOG_CRASH_THRESHOLD = 3; // Restarts to trigger alert
const WATCHDOG_CRASH_WINDOW = 5 * 60 * 1000; // 5 minute window
const WATCHDOG_ALERT_COOLDOWN = 30 * 60 * 1000; // 30 min between alerts
const GATEWAY_PROCESS_NAME = process.env.RESCUE_GATEWAY_PROCESS || "openclaw-gateway";
const watchdogState = {
  lastPid: null, // Last known gateway PID
  restarts: [], // Timestamps of detected restarts (bounded to 20)
  lastAlertTime: 0, // Last alert timestamp
  lastCheckTime: 0, // Last poll timestamp
  status: "starting", // "healthy" | "down" | "crash-loop" | "cooldown" | "starting"
};

if (!BOT_TOKEN) {
  console.error(
    "DISCORD_BOT_TOKEN (or DISCORD_ADMIN_BOT_TOKEN) is required"
  );
  process.exit(1);
}
if (!ADMIN_USER_ID) {
  console.error("DISCORD_ADMIN_USER_ID is required");
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Discord message helpers (2000 char limit)
// ---------------------------------------------------------------------------

const DISCORD_MAX = 2000;

/** Reply to a message, splitting into multiple messages if needed. */
async function safeReply(message, text) {
  if (text.length <= DISCORD_MAX) {
    return message.reply(text);
  }

  // Split on newlines, respecting the character limit
  const chunks = [];
  let current = "";
  for (const line of text.split("\n")) {
    if (current.length + line.length + 1 > DISCORD_MAX) {
      if (current) chunks.push(current);
      current = line;
    } else {
      current = current ? current + "\n" + line : line;
    }
  }
  if (current) chunks.push(current);

  // First chunk as reply, rest as follow-up messages
  await message.reply(chunks[0]);
  for (let i = 1; i < chunks.length; i++) {
    await message.channel.send(chunks[i]);
  }
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
    // Lock was removed by another process
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
  return /^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$/.test(id) && id.length <= 64;
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

async function findSessionByChannel(channelId) {
  const agents = await listAgentIds();
  const pattern = `discord:channel:${channelId}`;

  for (const agentId of agents) {
    const result = await readSessionsJson(agentId);
    if (!result) continue;
    for (const [key, entry] of Object.entries(result.data)) {
      if (key.endsWith(pattern) || key.includes(pattern + ":")) {
        return { agentId, key, entry, sessionsFile: result.filePath };
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

async function handleReset(message, args) {
  const target = args[0];
  const allFlag = args[1]?.toLowerCase() === "all";

  if (target) {
    const agentId = await resolveAgentId(target);
    if (!agentId) {
      const list = await formatAgentList();
      return message.reply(`Agent \`${target}\` not found. Available: ${list}`);
    }

    if (allFlag) {
      const sessions = await findSessionsByAgent(agentId);
      if (sessions.length === 0) {
        await auditLog(message.author.id, "reset", [agentId, "all"], message.channel.id, "no_sessions");
        return message.reply(`No sessions found for **${agentDisplayName(agentId)}**.`);
      }
      for (const session of sessions) {
        await resetSession(session);
      }
      await auditLog(message.author.id, "reset", [agentId, "all"], message.channel.id, `reset_all_${sessions.length}`);
      return message.reply(
        `\u{1F504} Reset **all ${sessions.length}** sessions for **${agentDisplayName(agentId)}**. Send a message to start fresh.`
      );
    }

    const session = await findSessionByChannel(message.channel.id);
    if (!session || session.agentId !== agentId) {
      await auditLog(message.author.id, "reset", [agentId], message.channel.id, "not_in_channel");
      return message.reply(
        `**${agentDisplayName(agentId)}** doesn't have a session in this channel.\n` +
        `Use \`${PREFIX}reset ${target} all\` to reset all their sessions everywhere.`
      );
    }
    const usage = getUsageInfo(session.entry);
    await resetSession(session);
    await auditLog(message.author.id, "reset", [agentId], message.channel.id, `reset_${usage.pct}pct`);
    return message.reply(
      `\u{1F504} **${agentDisplayName(agentId)}**'s session in this channel has been reset (was at ${usage.pct}% context). Send a message to start fresh.`
    );
  }

  const session = await findSessionByChannel(message.channel.id);
  if (!session) {
    await auditLog(message.author.id, "reset", [], message.channel.id, "no_session");
    return message.reply(
      "No OpenClaw session found for this channel. Is an agent active here?"
    );
  }

  const name = agentDisplayName(session.agentId);
  const usage = getUsageInfo(session.entry);
  await resetSession(session);
  await auditLog(message.author.id, "reset", [], message.channel.id, `reset_${name}_${usage.pct}pct`);

  return message.reply(
    `\u{1F504} **${name}**'s session in this channel has been reset (was at ${usage.pct}% context). Send a message to start fresh.`
  );
}

// ---------------------------------------------------------------------------
// !status
// ---------------------------------------------------------------------------

async function handleStatus(message, args) {
  if (args[0] === "all") {
    const agents = await listAgentIds();
    if (agents.length === 0) {
      return message.reply("Could not read agents directory.");
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
        const shortKey = s.key.length > 50 ? "..." + s.key.slice(-47) : s.key;
        lines.push(`  ${s.emoji} ${s.pct}% \u2014 \`${shortKey}\``);
      }
      if (entries.length > 3) {
        lines.push(`  _...and ${entries.length - 3} more_`);
      }
    }

    lines.unshift(`\u{1F4CA} **${totalSessions} sessions across ${agents.length} agents**\n`);
    await auditLog(message.author.id, "status", ["all"], message.channel.id, `${totalSessions}_sessions`);
    return safeReply(message, lines.join("\n"));
  }

  const session = await findSessionByChannel(message.channel.id);
  if (!session) {
    return message.reply(
      `No OpenClaw session found for this channel. Use \`${PREFIX}status all\` to see everything.`
    );
  }

  const name = agentName(session.agentId);
  const usage = getUsageInfo(session.entry);
  const model = session.entry.model || "unknown";
  const modelProvider = session.entry.modelProvider || "";
  const fullModel = modelProvider ? `${modelProvider}/${model}` : model;
  const alias = await resolveModelAlias(fullModel);
  const modelDisplay = alias ? `\`${fullModel}\` (${alias})` : `\`${fullModel}\``;
  const override = session.entry.modelOverride;
  const overrideNote = override ? ` *(override active)*` : "";
  const updatedAt = session.entry.updatedAt
    ? new Date(session.entry.updatedAt).toLocaleString("en-US", {
        dateStyle: "short",
        timeStyle: "short",
      })
    : "unknown";

  await auditLog(message.author.id, "status", [], message.channel.id, `${name}_${usage.pct}pct`);
  return message.reply(
    [
      `${usage.emoji} **${name}** in this channel`,
      `Context: **${usage.pct}%** (${(usage.total / 1000).toFixed(0)}k / ${(usage.context / 1000).toFixed(0)}k tokens)`,
      `Model: ${modelDisplay}${overrideNote}`,
      `Last active: ${updatedAt}`,
      usage.pct >= 70
        ? `\n\u26A0\uFE0F Context is getting full. Use \`${PREFIX}reset\` to clear it.`
        : "",
    ]
      .filter(Boolean)
      .join("\n")
  );
}

// ---------------------------------------------------------------------------
// !start
// ---------------------------------------------------------------------------

async function handleStart(message, args) {
  if (args.length === 0) {
    return message.reply(
      `Usage: \`${PREFIX}start <message>\` \u2014 sends a prompt to the agent in this channel.`
    );
  }

  const session = await findSessionByChannel(message.channel.id);
  if (!session) {
    return message.reply(
      "No OpenClaw session found for this channel. Is an agent active here?"
    );
  }

  const prompt = args.join(" ");
  const name = agentName(session.agentId);

  await message.reply(`\u{1F4AC} Sending to **${name}**...`);
  await auditLog(message.author.id, "start", args, message.channel.id, `session:${session.key}`);

  // Try to find the ask-watson bridge script (optional — works without it)
  const bridgeScript = path.join(
    os.homedir(), ".claude", "skills", "ask-watson", "scripts", "ask-watson.js"
  );

  try {
    await fsp.access(bridgeScript);
  } catch {
    return message.channel.send(
      `\u{274C} Bridge script not found at \`${bridgeScript}\`. ` +
      `Set up the OpenClaw bridge to use \`${PREFIX}start\`.`
    );
  }

  execFile(
    "node",
    [bridgeScript, "send", session.key, prompt],
    { timeout: 120000 },
    async (err) => {
      if (err && err.killed) return; // Timeout is fine — agent is processing
      if (err) {
        console.error(`[rescue] !start send error:`, err.message);
        try {
          await message.channel.send(
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

async function handleRestart(message, args) {
  const target = args[0]?.toLowerCase();

  if (target !== "gateway") {
    return message.reply(`Usage: \`${PREFIX}restart gateway\``);
  }

  await message.reply(
    "\u{1F504} Restarting OpenClaw gateway (clears provider cooldowns)..."
  );
  await auditLog(message.author.id, "restart", ["gateway"], message.channel.id, "initiated");

  exec("pkill -f openclaw-gateway", async (err) => {
    if (err && err.code !== 1) {
      await message.channel.send(
        `\u{274C} Could not kill gateway process: ${err.message}`
      );
      await auditLog(message.author.id, "restart", ["gateway"], message.channel.id, `error: ${err.message}`);
      return;
    }

    setTimeout(async () => {
      try {
        // Quick health check — try to read config to confirm gateway dir exists
        await fsp.access(CONFIG_PATH);
        await message.channel.send(
          `\u{2705} Gateway killed — launchd should auto-restart it. Use \`${PREFIX}status all\` to verify.`
        );
        await auditLog(message.author.id, "restart", ["gateway"], message.channel.id, "success");
      } catch {
        await message.channel.send(
          "\u{26A0}\uFE0F Gateway killed but config not found. Check your OpenClaw installation."
        );
        await auditLog(message.author.id, "restart", ["gateway"], message.channel.id, "config_missing");
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

async function handleModel(message, args) {
  const { aliases, models } = await loadModelAliases();

  if (args.length === 0) {
    const aliasLines = [];
    for (const [fullName, modelConfig] of Object.entries(models)) {
      if (!modelConfig.alias) continue;
      aliasLines.push(`  \`${modelConfig.alias}\` \u2192 \`${fullName}\``);
    }
    const list = aliasLines.length > 0
      ? `\n\n**Available models:**\n${aliasLines.join("\n")}`
      : "";
    return message.reply(
      `**Usage:** \`${PREFIX}model <alias|name>\` or \`${PREFIX}model show\` or \`${PREFIX}model default\`` +
      list
    );
  }

  const input = args[0].toLowerCase();

  // !model show
  if (input === "show" || input === "current") {
    const session = await findSessionByChannel(message.channel.id);
    if (!session) {
      return message.reply("No OpenClaw session found for this channel.");
    }
    const override = session.entry.modelOverride;
    const providerOv = session.entry.providerOverride;
    if (!override) {
      return message.reply("No model override set \u2014 using agent default.");
    }
    const fullName = providerOv ? `${providerOv}/${override}` : override;
    const alias = await resolveModelAlias(fullName);
    return message.reply(
      `Current override: \`${fullName}\`${alias ? ` (${alias})` : ""}\n` +
      `Use \`${PREFIX}model default\` to clear.`
    );
  }

  // !model default
  if (input === "default" || input === "clear" || input === "reset") {
    const session = await findSessionByChannel(message.channel.id);
    if (!session) {
      return message.reply("No OpenClaw session found for this channel.");
    }
    try {
      const preCheck = await readSessionsJson(session.agentId);
      if (!preCheck || !preCheck.data[session.key]) {
        return message.reply("Session entry not found.");
      }
      const { filePath } = preCheck;
      const lock = await acquireLock(filePath);
      try {
        const freshRaw = await fsp.readFile(filePath, "utf-8");
        const data = JSON.parse(freshRaw);
        if (!data[session.key]) return message.reply("Session disappeared.");
        const old = data[session.key].modelOverride || "(none)";
        delete data[session.key].modelOverride;
        delete data[session.key].providerOverride;
        const tmpFile = filePath + ".tmp";
        await fsp.writeFile(tmpFile, JSON.stringify(data, null, 2));
        await fsp.rename(tmpFile, filePath);
        await message.reply(
          `Model override cleared (was: \`${old}\`). Using agent default on next message.`
        );
        await auditLog(message.author.id, "model", ["default"], message.channel.id, `${old} -> default`);
      } finally {
        await releaseLock(lock);
      }
    } catch (err) {
      await message.reply(`Error clearing override: ${err.message}`);
    }
    return;
  }

  // !model <name>
  const session = await findSessionByChannel(message.channel.id);
  if (!session) {
    return message.reply(
      "No OpenClaw session found for this channel. Is an agent active here?"
    );
  }

  let modelName = aliases[input] || input;

  if (!models[modelName] && !/^[a-z0-9][a-z0-9/_.-]*[a-z0-9]$/.test(modelName)) {
    const suggestions = Object.entries(aliases)
      .filter(([alias]) => alias.includes(input.split("-")[0]))
      .slice(0, 3)
      .map(([alias]) => `\`${alias}\``)
      .join(", ");
    return message.reply(
      `Unknown model: \`${input}\`${suggestions ? `\nDid you mean: ${suggestions}?` : ""}`
    );
  }

  const { provider, model } = splitModelName(modelName);

  try {
    const preCheck = await readSessionsJson(session.agentId);
    if (!preCheck) {
      return message.reply("Could not read session data. Try again in a moment.");
    }
    if (!preCheck.data[session.key]) {
      return message.reply(
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
        return message.reply("Session disappeared. Try again.");
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
    await message.reply(
      `\u{1F504} Model override set to ${displayName}\n` +
      `Previous: ${oldModel}\n` +
      `This will take effect on the next message.`
    );
    await auditLog(message.author.id, "model", [modelName], message.channel.id, `${oldModel} -> ${modelName}`);
  } catch (err) {
    console.error(`[rescue] !model error:`, err.message);
    await message.reply(`\u{274C} Error setting model: ${err.message}`);
    await auditLog(message.author.id, "model", args, message.channel.id, `error: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// !mute / !unmute
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
  // Check if this agent is the default (no explicit binding)
  const defaultAgentId = config.agents?.defaults?.agentId || "main";
  if (agentId === defaultAgentId) return "default";
  const binding = (config.bindings || []).find(
    (b) => b.agentId === agentId && b.match?.channel === "discord"
  );
  return binding?.match?.accountId || null;
}

async function handleMute(message, args, mute) {
  const channelId = message.channel.id;
  const verb = mute ? "Mute" : "Unmute";
  const pastVerb = mute ? "Muted" : "Unmuted";

  let agentId;
  if (args[0]) {
    agentId = await resolveAgentId(args[0]);
    if (!agentId) {
      const list = await formatAgentList();
      return message.reply(`Agent \`${args[0]}\` not found. Available: ${list}`);
    }
  } else {
    const session = await findSessionByChannel(channelId);
    agentId = session?.agentId || "main";
  }

  const accountId = findDiscordAccount(await readConfig(), agentId);
  if (!accountId) {
    return message.reply(
      `Could not find Discord account for **${agentDisplayName(agentId)}**.`
    );
  }

  await message.reply(
    `\u{1F504} ${verb === "Mute" ? "Muting" : "Unmuting"} **${agentDisplayName(agentId)}** in this channel...`
  );
  await auditLog(message.author.id, mute ? "mute" : "unmute", [agentId], channelId, "initiated");

  try {
    // Auto-backup config before modifying it
    await createBackup("pre-" + (mute ? "mute" : "unmute"));

    // Kill gateway first to avoid race condition on config writes
    await new Promise((resolve) =>
      exec("pkill -f openclaw-gateway", () => resolve())
    );
    await new Promise((r) => setTimeout(r, 2000));

    const config = await readConfig();
    const account = config.channels?.discord?.accounts?.[accountId];
    if (!account) {
      return message.channel.send(
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
      return message.channel.send(
        `\u{274C} Channel \`${channelId}\` not configured for **${agentDisplayName(agentId)}**'s Discord account.`
      );
    }

    await writeConfig(config);

    setTimeout(async () => {
      try {
        await message.channel.send(
          `\u{2705} **${pastVerb}** ${agentDisplayName(agentId)} in this channel. ` +
          `${mute ? "They now require an @mention to respond." : "They will respond to all messages."}`
        );
        await auditLog(message.author.id, mute ? "mute" : "unmute", [agentId], channelId, "success");
      } catch {
        /* channel unavailable */
      }
    }, 6000);
  } catch (err) {
    console.error(`[rescue] !${mute ? "mute" : "unmute"} error:`, err.message);
    await message.channel.send(`\u{274C} Error: ${err.message}`);
    await auditLog(message.author.id, mute ? "mute" : "unmute", [agentId], channelId, `error: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// !keys status
// ---------------------------------------------------------------------------

async function handleKeys(message, args) {
  const sub = args[0]?.toLowerCase();

  if (sub !== "status") {
    return message.reply(`Usage: \`${PREFIX}keys status\` \u2014 Show auth-profile health`);
  }

  const agents = await listAgentIds();
  const lines = [];
  let totalIssues = 0;

  for (const agentId of agents.sort()) {
    if (!isValidAgentId(agentId)) continue;
    const authPath = path.join(AGENTS_DIR, agentId, "agent", "auth-profiles.json");
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

  await auditLog(message.author.id, "keys", ["status"], message.channel.id, `${totalIssues}_issues`);
  return safeReply(message, header + lines.join("\n"));
}

// ---------------------------------------------------------------------------
// !backup — snapshot openclaw.json
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

  // Prune old backups beyond MAX_BACKUPS
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

async function handleBackup(message) {
  try {
    await fsp.access(CONFIG_PATH);
  } catch {
    return message.reply(
      `\u{274C} Config not found at \`${CONFIG_PATH}\`. Is OpenClaw installed?`
    );
  }

  try {
    // Validate JSON before backing up (don't back up broken configs)
    const raw = await fsp.readFile(CONFIG_PATH, "utf-8");
    JSON.parse(raw);

    const { filename, total } = await createBackup("manual");

    await message.reply(
      `\u{2705} Config backed up as \`${filename}\`\n` +
      `${total} backup${total !== 1 ? "s" : ""} stored in \`${BACKUP_DIR}/\``
    );
    await auditLog(message.author.id, "backup", [], message.channel.id, filename);
  } catch (err) {
    if (err instanceof SyntaxError) {
      return message.reply(
        `\u{26A0}\uFE0F Current config has invalid JSON \u2014 backing up anyway as a record.`
      );
    }
    await message.reply(`\u{274C} Backup failed: ${err.message}`);
    await auditLog(message.author.id, "backup", [], message.channel.id, `error: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// !rollback — restore last known good config + restart gateway
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

async function handleRollback(message, args) {
  const sub = args[0]?.toLowerCase();
  const backups = await listBackups();

  if (backups.length === 0) {
    return message.reply(
      `No backups found in \`${BACKUP_DIR}/\`.\n` +
      `Use \`${PREFIX}backup\` to create one first.`
    );
  }

  // !rollback list — show recent backups
  if (sub === "list") {
    const recent = backups.slice(0, 10);
    const lines = recent.map((f, i) => {
      const label = i === 0 ? " **(latest)**" : "";
      return `  \`${f}\`${label}`;
    });
    await auditLog(message.author.id, "rollback", ["list"], message.channel.id, `${backups.length}_backups`);
    return message.reply(
      `\u{1F4CB} **${backups.length} backups available:**\n${lines.join("\n")}\n\n` +
      `Use \`${PREFIX}rollback\` to restore the latest, or \`${PREFIX}rollback <filename>\` for a specific one.`
    );
  }

  // Determine which backup to restore
  let targetFile;
  if (!sub) {
    // Default: latest backup
    targetFile = backups[0];
  } else {
    // Specific backup by filename (or partial match)
    targetFile = backups.find(
      (f) => f === sub || f.includes(sub)
    );
    if (!targetFile) {
      return message.reply(
        `Backup \`${sub}\` not found. Use \`${PREFIX}rollback list\` to see available backups.`
      );
    }
  }

  const backupPath = path.join(BACKUP_DIR, targetFile);

  // Validate the backup is valid JSON before restoring
  try {
    const raw = await fsp.readFile(backupPath, "utf-8");
    JSON.parse(raw);
  } catch {
    return message.reply(
      `\u{274C} Backup \`${targetFile}\` contains invalid JSON. Try a different backup.`
    );
  }

  await message.reply(
    `\u{1F504} Rolling back to \`${targetFile}\`...\n` +
    `1. Backing up current config\n` +
    `2. Stopping gateway\n` +
    `3. Restoring backup\n` +
    `4. Restarting gateway`
  );

  try {
    // Step 1: Backup current config before overwriting
    await createBackup("pre-rollback");

    // Step 2: Kill gateway
    await new Promise((resolve) =>
      exec("pkill -f openclaw-gateway", () => resolve())
    );
    await new Promise((r) => setTimeout(r, 2000));

    // Step 3: Restore the backup (atomic: copy to tmp, rename)
    const tmpFile = CONFIG_PATH + ".tmp";
    await fsp.copyFile(backupPath, tmpFile);
    await fsp.rename(tmpFile, CONFIG_PATH);

    // Step 4: Gateway auto-restarts via launchd (KeepAlive=true)
    // Wait a moment and confirm
    setTimeout(async () => {
      try {
        // Verify the config is readable
        const raw = await fsp.readFile(CONFIG_PATH, "utf-8");
        const config = JSON.parse(raw);
        const agentCount = Object.keys(config.agents?.agents || {}).length;
        await message.channel.send(
          `\u{2705} **Rollback complete!** Restored \`${targetFile}\`\n` +
          `Config has ${agentCount} agent${agentCount !== 1 ? "s" : ""} configured. ` +
          `Gateway should auto-restart via launchd.`
        );
        await auditLog(message.author.id, "rollback", [targetFile], message.channel.id, "success");
      } catch (err) {
        await message.channel.send(
          `\u{26A0}\uFE0F Config restored but verification failed: ${err.message}\n` +
          `The gateway may need manual attention.`
        );
        await auditLog(message.author.id, "rollback", [targetFile], message.channel.id, `verify_error: ${err.message}`);
      }
    }, 6000);
  } catch (err) {
    console.error("[rescue] !rollback error:", err.message);
    await message.channel.send(`\u{274C} Rollback failed: ${err.message}`);
    await auditLog(message.author.id, "rollback", [targetFile], message.channel.id, `error: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// !help
// ---------------------------------------------------------------------------

async function handleHelp(message) {
  const aliasEntries = Object.entries(AGENT_ALIASES);
  const aliasLine =
    aliasEntries.length > 0
      ? `\n_Agent aliases: ${aliasEntries.map(([a, id]) => `\`${a}\`\u2192\`${id}\``).join(", ")}_`
      : "";

  return message.reply(
    [
      "**Rescue \u2014 OpenClaw Admin Bot**",
      "",
      "**Session Management**",
      `\`${PREFIX}reset\` \u2014 Reset the agent session in this channel`,
      `\`${PREFIX}reset <agent>\` \u2014 Reset agent's session in this channel`,
      `\`${PREFIX}reset <agent> all\` \u2014 Reset ALL sessions for an agent everywhere`,
      `\`${PREFIX}status\` \u2014 Show session health for this channel`,
      `\`${PREFIX}status all\` \u2014 Show all sessions across all agents`,
      `\`${PREFIX}start <message>\` \u2014 Kick off a conversation with the agent`,
      "",
      "**Model & Config**",
      `\`${PREFIX}model <alias|name>\` \u2014 Override model for this session`,
      `\`${PREFIX}model show\` \u2014 Show current model override`,
      `\`${PREFIX}model default\` \u2014 Clear model override`,
      `\`${PREFIX}mute [agent]\` \u2014 Require @mention for agent in this channel`,
      `\`${PREFIX}unmute [agent]\` \u2014 Let agent respond to all messages`,
      "",
      "**System**",
      `\`${PREFIX}restart gateway\` \u2014 Restart the OpenClaw gateway`,
      `\`${PREFIX}keys status\` \u2014 Show auth-profile health`,
      `\`${PREFIX}backup\` \u2014 Snapshot the current gateway config`,
      `\`${PREFIX}rollback\` \u2014 Restore the last known good config`,
      `\`${PREFIX}rollback list\` \u2014 Show available config backups`,
      `\`${PREFIX}watchdog\` \u2014 Show gateway health and restart history`,
      `\`${PREFIX}help\` \u2014 This message`,
      aliasLine,
    ]
      .filter(Boolean)
      .join("\n")
  );
}

// ---------------------------------------------------------------------------
// Stall detector — alerts when an agent hasn't responded in STALL_THRESHOLD_MS
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

  // Prune stale entries
  for (const [key, time] of staleAlertTimes) {
    if (now - time > STALL_ALERT_COOLDOWN * 2) staleAlertTimes.delete(key);
  }

  for (const agentId of agents) {
    const result = await readSessionsJson(agentId);
    if (!result) continue;

    for (const [key, entry] of Object.entries(result.data)) {
      if (!key.includes("discord:channel:")) continue;

      const updatedAt = entry.updatedAt || 0;
      const timeSinceUpdate = now - updatedAt;

      if (timeSinceUpdate > STALL_ACTIVE_WINDOW) continue;
      if (timeSinceUpdate < STALL_THRESHOLD_MS) continue;

      const lastAlert = staleAlertTimes.get(key) || 0;
      if (now - lastAlert < STALL_ALERT_COOLDOWN) continue;

      const lastRole = await readLastMessageRole(agentId, entry);
      if (lastRole !== "user") continue;

      const channelMatch = key.match(/discord:channel:(\d+)/);
      if (!channelMatch) continue;
      const channelId = channelMatch[1];

      try {
        const channel = await client.channels.fetch(channelId);
        if (!channel?.isTextBased()) continue;

        const name = agentName(agentId);
        const minutes = Math.round(timeSinceUpdate / 60000);
        const usage = getUsageInfo(entry);

        await channel.send(
          `\u{1F6A8} **Stall detected:** ${name} hasn't responded in ${minutes} minutes (context: ${usage.pct}%). Use \`${PREFIX}reset\` to clear the session.`
        );

        staleAlertTimes.set(key, now);
        await auditLog("system", "stall_alert", [agentId, key], channelId, `${minutes}min_${usage.pct}pct`);
      } catch (err) {
        console.error(`[rescue] Stall alert failed for ${key}:`, err.message);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Gateway watchdog — crash-loop detection and alerting
// ---------------------------------------------------------------------------

/** Get the current gateway PID, or null if not running. */
function getGatewayPid() {
  return new Promise((resolve) => {
    exec(`pgrep -f "${GATEWAY_PROCESS_NAME}"`, { timeout: 5000 }, (err, stdout) => {
      if (err) return resolve(null); // Not running or pgrep failed
      const pids = stdout.trim().split("\n").filter(Boolean);
      // Return the first PID (there should only be one gateway)
      resolve(pids.length > 0 ? parseInt(pids[0], 10) : null);
    });
  });
}

async function checkGatewayHealth() {
  const now = Date.now();
  watchdogState.lastCheckTime = now;

  const pid = await getGatewayPid();

  // Gateway is down
  if (!pid) {
    if (watchdogState.lastPid !== null) {
      // Was running, now it's not — record a restart event
      watchdogState.restarts.push(now);
      // Bound array to 20 entries
      if (watchdogState.restarts.length > 20) {
        watchdogState.restarts = watchdogState.restarts.slice(-20);
      }
    }
    watchdogState.lastPid = null;
    watchdogState.status = "down";
    return;
  }

  // Gateway is running — check if PID changed (restart detected)
  if (watchdogState.lastPid !== null && pid !== watchdogState.lastPid) {
    watchdogState.restarts.push(now);
    if (watchdogState.restarts.length > 20) {
      watchdogState.restarts = watchdogState.restarts.slice(-20);
    }
  }

  watchdogState.lastPid = pid;

  // Count recent restarts within the crash window
  const recentRestarts = watchdogState.restarts.filter(
    (t) => now - t < WATCHDOG_CRASH_WINDOW
  );

  if (recentRestarts.length >= WATCHDOG_CRASH_THRESHOLD) {
    // Crash-loop detected — check cooldown
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
      `\u2022 \`${PREFIX}rollback\` \u2014 restore last known good config\n` +
      `\u2022 \`${PREFIX}rollback list\` \u2014 see available backups\n` +
      `\u2022 Check gateway logs for the root cause`;

    // Alert in ops channel if configured
    if (OPS_CHANNEL_ID) {
      try {
        const channel = await client.channels.fetch(OPS_CHANNEL_ID);
        if (channel?.isTextBased()) {
          await channel.send(alertMessage);
        }
      } catch (err) {
        console.error("[rescue] Watchdog alert to ops channel failed:", err.message);
      }
    }

    await auditLog(
      "watchdog",
      "crash_loop_alert",
      [String(recentRestarts.length), String(pid)],
      OPS_CHANNEL_ID || "none",
      `${recentRestarts.length}_restarts_in_${WATCHDOG_CRASH_WINDOW / 60000}min`
    );

    console.log(
      `[rescue] Watchdog: crash-loop detected (${recentRestarts.length} restarts in ${WATCHDOG_CRASH_WINDOW / 60000}min)`
    );
    return;
  }

  watchdogState.status = "healthy";
}

/** Handle !watchdog command — show watchdog status. */
async function handleWatchdog(message) {
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
    const cooldownRemaining = WATCHDOG_ALERT_COOLDOWN - (now - watchdogState.lastAlertTime);
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

  await auditLog(message.author.id, "watchdog", [], message.channel.id, watchdogState.status);
  return message.reply(lines.join("\n"));
}

// ---------------------------------------------------------------------------
// Bot setup
// ---------------------------------------------------------------------------

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

client.once("clientReady", () => {
  console.log(`[rescue] Logged in as ${client.user.tag}`);
  console.log(`[rescue] Authorized user: ${ADMIN_USER_ID}`);
  console.log(`[rescue] OpenClaw dir: ${OPENCLAW_DIR}`);
  console.log(`[rescue] Config: ${CONFIG_PATH}`);
  console.log(`[rescue] Backups: ${BACKUP_DIR}`);
  console.log(`[rescue] Audit log: ${AUDIT_LOG}`);

  if (Object.keys(AGENT_ALIASES).length > 0) {
    console.log(
      `[rescue] Agent aliases: ${JSON.stringify(AGENT_ALIASES)}`
    );
  }

  // Start stall detection loop
  setInterval(() => {
    checkForStalledSessions().catch((err) => {
      console.error("[rescue] Stall check error:", err.message);
    });
  }, STALL_CHECK_INTERVAL);
  console.log(
    `[rescue] Stall detector active (${STALL_CHECK_INTERVAL / 1000}s interval, ${STALL_THRESHOLD_MS / 60000}min threshold)`
  );

  // Start gateway watchdog
  setInterval(() => {
    checkGatewayHealth().catch((err) => {
      console.error("[rescue] Watchdog check error:", err.message);
    });
  }, WATCHDOG_INTERVAL);
  // Run initial check immediately
  checkGatewayHealth().catch(() => {});
  console.log(
    `[rescue] Watchdog active (${WATCHDOG_INTERVAL / 1000}s interval, alert on ${WATCHDOG_CRASH_THRESHOLD}+ restarts in ${WATCHDOG_CRASH_WINDOW / 60000}min)`
  );
});

client.on("messageCreate", async (message) => {
  if (message.author.bot) return;
  if (!message.content.startsWith(PREFIX)) return;
  if (message.author.id !== ADMIN_USER_ID) return;

  const [command, ...args] = message.content
    .slice(PREFIX.length)
    .trim()
    .split(/\s+/);

  const cmd = command.toLowerCase();

  if (!checkCooldown(cmd)) return;

  try {
    switch (cmd) {
      case "reset":
        await handleReset(message, args);
        break;
      case "status":
        await handleStatus(message, args);
        break;
      case "restart":
        await handleRestart(message, args);
        break;
      case "start":
        await handleStart(message, args);
        break;
      case "model":
        await handleModel(message, args);
        break;
      case "keys":
        await handleKeys(message, args);
        break;
      case "mute":
        await handleMute(message, args, true);
        break;
      case "unmute":
        await handleMute(message, args, false);
        break;
      case "backup":
        await handleBackup(message);
        break;
      case "rollback":
        await handleRollback(message, args);
        break;
      case "watchdog":
        await handleWatchdog(message);
        break;
      case "help":
        await handleHelp(message);
        break;
      // Silently ignore unknown commands
    }
  } catch (err) {
    console.error(`[rescue] Error handling ${PREFIX}${cmd}:`, err);
    await auditLog(message.author.id, cmd, args, message.channel.id, `error: ${err.message}`);
    try {
      await message.reply(
        "An error occurred processing that command. Check logs for details."
      );
    } catch {
      // Can't reply
    }
  }
});

// Graceful shutdown
function shutdown(signal) {
  console.log(`[rescue] Received ${signal}, shutting down...`);
  client.destroy();
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// Start
client.login(BOT_TOKEN);
