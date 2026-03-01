# Rescue

**Discord admin bot for [OpenClaw](https://openclaw.ai) — session management, config rollback, and crash-loop detection.**

Rescue runs independently of the OpenClaw gateway. When your gateway is down, your agents are stuck, and your config is broken — Rescue still works. That's the point.

![Node.js](https://img.shields.io/badge/node-%3E%3D18-brightgreen) ![Discord.js](https://img.shields.io/badge/discord.js-v14-blue) ![License](https://img.shields.io/badge/license-MIT-green)

## Why Rescue Exists

OpenClaw has no built-in admin interface. When something goes wrong — an agent runs out of context, the gateway crashes from a bad config, API keys expire — you're stuck SSHing into a terminal. Rescue gives you a one-word fix from Discord:

- **`!reset`** clears a stuck agent session
- **`!rollback`** restores your last working config
- **`!watchdog`** tells you when the gateway is crash-looping before you even notice

Single file. Single dependency. Works when everything else is broken.

## Quick Start

```bash
git clone https://github.com/jeremyknows/rescue-for-openclaw.git
cd rescue-for-openclaw
npm install
```

Set two environment variables:

```bash
export DISCORD_BOT_TOKEN="your-bot-token"
export DISCORD_ADMIN_USER_ID="your-discord-user-id"
```

Run it:

```bash
node index.js
```

That's it. Rescue will connect to Discord and start monitoring your gateway.

> **Need a Discord bot token?** Create one at the [Discord Developer Portal](https://discord.com/developers/applications). Your bot needs `MESSAGE_CONTENT`, `GUILDS`, and `GUILD_MESSAGES` intents.

## Commands

### Session Management

| Command | What it does |
|---------|-------------|
| `!reset` | Reset the agent session in this channel |
| `!reset <agent>` | Reset a specific agent's session (supports aliases) |
| `!reset <agent> all` | Reset ALL sessions for an agent everywhere |
| `!status` | Show session health — context %, model, last active |
| `!status all` | Show all sessions across all agents |
| `!start <message>` | Send a prompt to the agent in this channel |

### Model Control

| Command | What it does |
|---------|-------------|
| `!model` | List available model aliases |
| `!model <alias>` | Override the model for this session |
| `!model show` | Show current model override |
| `!model default` | Clear override, return to agent default |

### System

| Command | What it does |
|---------|-------------|
| `!restart gateway` | Kill the gateway (launchd auto-restarts it) |
| `!keys status` | Show auth-profile health across all agents |
| `!backup` | Snapshot the current gateway config |
| `!rollback` | Restore the most recent backup + restart gateway |
| `!rollback list` | Show available config backups |
| `!rollback <name>` | Restore a specific backup |
| `!watchdog` | Show gateway health and restart history |
| `!mute [agent]` | Require @mention for agent in this channel |
| `!unmute [agent]` | Let agent respond to all messages |

## Background Monitors

Rescue runs two background monitors automatically:

**Stall Detector** — Checks every 60 seconds for agents that received a message but haven't responded in 15+ minutes. Posts an alert in the channel where the agent is stuck.

**Gateway Watchdog** — Polls the gateway process every 30 seconds. If it detects 3+ restarts within 5 minutes (crash-loop), it alerts your ops channel with recommended actions.

## Configuration

All configuration is via environment variables. Only the first two are required.

| Variable | Default | Description |
|----------|---------|-------------|
| `DISCORD_BOT_TOKEN` | *required* | Discord bot token (also accepts `DISCORD_ADMIN_BOT_TOKEN`) |
| `DISCORD_ADMIN_USER_ID` | *required* | Your Discord user ID — only you can use commands |
| `OPENCLAW_DIR` | `~/.openclaw` | Path to your OpenClaw installation |
| `RESCUE_PREFIX` | `!` | Command prefix |
| `RESCUE_AGENT_ALIASES` | `{}` | JSON map of friendly names, e.g. `{"watson":"main"}` |
| `RESCUE_OPS_CHANNEL_ID` | — | Channel ID for system alerts (watchdog, stalls) |
| `RESCUE_STALL_MINUTES` | `15` | Minutes before stall alert fires |
| `RESCUE_MAX_BACKUPS` | `20` | Config backups to keep before pruning |
| `RESCUE_GATEWAY_PROCESS` | `openclaw-gateway` | Process name for watchdog monitoring |

## Running as a Service

### macOS (launchd)

Create `~/Library/LaunchAgents/com.rescue-bot.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.rescue-bot</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/node</string>
    <string>/path/to/rescue-for-openclaw/index.js</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>DISCORD_BOT_TOKEN</key>
    <string>your-bot-token</string>
    <key>DISCORD_ADMIN_USER_ID</key>
    <string>your-discord-user-id</string>
  </dict>
  <key>KeepAlive</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
  <key>StandardOutPath</key>
  <string>/tmp/rescue-bot.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/rescue-bot.err.log</string>
</dict>
</plist>
```

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.rescue-bot.plist
```

### Linux (systemd)

```ini
[Unit]
Description=Rescue Bot for OpenClaw
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/node /path/to/rescue-for-openclaw/index.js
Environment=DISCORD_BOT_TOKEN=your-bot-token
Environment=DISCORD_ADMIN_USER_ID=your-discord-user-id
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## How It Works

Rescue reads and writes files in your OpenClaw directory (`~/.openclaw/` by default):

- **Sessions**: `agents/{id}/sessions/sessions.json` — reset sessions, set model overrides
- **Config**: `openclaw.json` — read model aliases, modify mute settings, backup/restore
- **Auth profiles**: `agents/{id}/agent/auth-profiles.json` — check API key health
- **Backups**: `backups/openclaw.json.*` — timestamped config snapshots

It communicates with the gateway only by killing the process (`pkill`) and relying on your process manager (launchd/systemd) to restart it. This means Rescue works even when the gateway is completely broken.

## Security

- **Single admin** — commands are silently ignored for all users except `DISCORD_ADMIN_USER_ID`
- **No secrets in code** — all credentials come from environment variables
- **Path traversal protection** — agent IDs and session IDs are regex-validated before filesystem access
- **Atomic file writes** — all config/session writes use tmp-file-then-rename to prevent corruption
- **File locking** — mkdir-based locks prevent race conditions with the gateway
- **JSON validation** — rollback validates backup JSON before restoring; won't restore corrupt files
- **Rate limiting** — all commands have cooldowns to prevent accidental spam

## Extending Rescue

### Adding a command

1. Write a `handleYourCommand(message, args)` function
2. Add a cooldown to the `COOLDOWNS` map
3. Add a `case` to the `switch` in the message handler
4. Add a line to `handleHelp()`

### Adding agent aliases

No code change needed:

```bash
export RESCUE_AGENT_ALIASES='{"watson":"main","barker":"herald"}'
```

## Roadmap

- [ ] **Telegram support** — same commands, works in Telegram groups alongside Discord
- [ ] **Auto-rollback** (opt-in) — watchdog automatically restores last known good config on crash-loop
- [ ] **Config diff** — `!diff` to show what changed between current config and last backup
- [ ] **Health dashboard** — `!health` combining gateway status, session health, and API key state in one view

## License

MIT — see [LICENSE](LICENSE).

## Credits

Built by [@jeremyknowsVF](https://twitter.com/jeremyknowsVF) for the [OpenClaw](https://openclaw.ai) community.
