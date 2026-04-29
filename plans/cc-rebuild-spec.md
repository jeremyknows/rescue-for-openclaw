# Rescue-Bot CC Rebuild — Spec

**Status:** Pre-PRD. Grill-me session needed before build.
**Audited:** 2026-04-22 (Terminal)
**Audit source:** `~/atlas/agents/terminal/` session

---

## Why

OpenClaw gateway entered hiatus 2026-04-18. As of 2026-04-22 it is definitively down (pid: null, port 18789 dead). ~70% of rescue-bot's commands depend on OpenClaw internals (`openclaw.json`, `sessions.json`, gateway API). Those commands silently no-op or throw.

The CC world needs a rescue/ops bot, but not the same one. Different problems, different commands.

---

## Approach

**Archive, don't delete.** Rename `index.js` → `index.openclaw-legacy.js`. Write new `index.js` from scratch — CC-only, clean. Plist points at same directory.

**Same repo** (`~/projects/rescue-bot/`). No new repo needed.

---

## What to Port (Verbatim — Already Works)

| Command | What it does | Notes |
|---------|-------------|-------|
| `!wcc` | List running watson-cc-* tmux sessions with uptime | Already CC-aware |
| `!wcc start [name]` | Start Terminal or named plain session | Already CC-aware |
| `!wcc stop [name\|all]` | Stop session(s) | Already CC-aware |
| `!wcc kill [name\|all]` | Force kill + cleanup orphans + clear MCP cache | Already CC-aware |
| `!wcc restart` | Restart Terminal with fresh context | Already CC-aware |
| `!mc [start\|stop\|status]` | Mission Control dashboard (port 3000) | Path may need update |
| `!ki [start\|stop\|status]` | Knowledge Intake launchd services | Platform-agnostic |
| `!watson-graph [start\|stop\|status]` | Watson Knowledge Graph (port 4444) | Path needs update |

---

## What to Cut (OpenClaw-Only)

- `!reset` — reads/writes `sessions.json` (OpenClaw format)
- `!handoff` — injects message via gateway HTTP API (port 18789)
- `!model` — reads model aliases from `openclaw.json`
- `!mute` / `!unmute` — modifies `openclaw.json` agent config
- `!backup` / `!rollback` — snapshots `openclaw.json`
- `!swap` — modifies `auth.order.anthropic` in `openclaw.json`
- `!restart gateway` — kills `openclaw-gateway` process
- `!gc` — prunes OpenClaw sessions/cron jobs
- Gateway stall detector — monitors OpenClaw RSS and sessions
- `checkGatewayHealth()` — entire function obsolete

---

## New Commands (CC World)

### `!service <name> [start|stop|restart|status]`

Manage launchd-backed services by friendly name.

| Name | Launchd label |
|------|---------------|
| `watson-bridge` | `com.watson.watson-bridge` |
| `builder-bridge` | `com.watson.builder-bridge` |
| `augur` | `com.watson.augur` |
| `rescue` | `com.watson.rescue-bot` |
| `discrawl` | `com.openclaw.discrawl-tail` (may rename) |

`status` shows: running/stopped, PID, exit code, last start time.
`restart` = `launchctl kickstart -k`.

### `!status`

Unified dashboard. One message showing:
- All launchd services (up/down/PID)
- Running watson-cc-* tmux sessions (name, uptime)
- Bridge session counts (if accessible)
- Gateway: explicitly "DOWN (hiatus)" to make it obvious

### `!logs <service> [N]`

Tail last N lines (default 20) from service log. Services mapped to log paths:
- `watson-bridge` → `~/atlas/logs/watson-bridge.log` (or wherever it logs)
- `builder-bridge` → `~/.openclaw/logs/builder-bridge.log` (needs path update)
- `augur` → `~/atlas/logs/augur.log`
- `rescue` → `~/atlas/logs/rescue-bot.log`

### `!backup` (reframed)

Snapshot `~/.claude/settings.json` + `~/.claude/settings.local.json` to timestamped backup.
`!rollback [filename]` restores from backup (validate JSON before writing).

---

## Infrastructure / Non-Feature Items

- **Log path:** `~/.openclaw/logs/rescue-bot-audit.jsonl` → `~/atlas/logs/rescue-bot-audit.jsonl`
- **Remove constants:** `OPENCLAW_DIR`, `AGENTS_DIR`, `CONFIG_PATH`, `CRON_JOBS_PATH`, `BACKUP_DIR`, `HANDOFF_MEMORY_DIR` — all obsolete
- **Keep:** `MessageContext` abstraction (Discord + Telegram unified), audit log pattern, ops alerts via Pulse
- **WCC script path:** Verify `~/.openclaw/scripts/start-terminal.sh` is the right entry point or update to `~/atlas/shared/scripts/...`

---

## Open Questions (Grill-Me Agenda)

1. **`!reset` replacement** — what should resetting a CC agent mean? Options: (a) `!wcc restart <name>` already covers it, (b) clear the ralph-loop state file + restart, (c) call bridge `!clear`. Or just retire the concept?

2. **`!swap` equivalent** — the CC CLI uses `~/.claude/.credentials.json` for auth. Jeremy has a `cc-account` script for swapping. Should rescue-bot expose `!swap` as a thin wrapper around `cc-account`? Or too dangerous to expose via Discord?

3. **Bridge session visibility** — Builder Bridge and Watson Bridge don't expose a health API. `!status` could show launchd status (up/down) but not "3 active CC sessions." Is launchd status enough, or should bridges get a `/health` endpoint?

4. **`!mute`/`!unmute` equivalent** — in CC world, "mute" means changing whether an agent requires @mention. This is configured in `~/.claude/channels/*/access.json`. Should rescue-bot expose this? Edge case: misconfiguration could mute an agent silently.

5. **Telegram** — rescue-bot has full Telegram support today. Keep it in the rebuild? (Jeremy uses it occasionally for ops.)

6. **Scope of `!service`** — full list of CC-era launchd services to expose. Current known list is 5. Are there others?

---

## Build Estimate

When ready:
- Cut dead code + update paths: ~2h
- Port working commands verbatim: ~1h
- `!service` launchd mgmt: ~2h
- `!status` dashboard: ~1h
- `!logs` tailing: ~1h
- `!backup`/`!rollback` for CC configs: ~1h

**~8h total.** Reasonable single Builder session after PRD is locked.
