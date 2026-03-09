# !handoff Command — Non-Destructive Session Resets

**Status:** Planned
**Priority:** High — solves real pain point (context loss on reset)

## Problem

When an agent session hits context limits (80-100K+ tokens), the only option is `!reset`, which destroys all context. The operator must:
1. Manually ask the agent to write a handoff
2. Wait for completion
3. Run `!reset`
4. Hope the agent reads the handoff on restart

This is error-prone and often skipped under time pressure, leading to lost institutional memory.

## Solution

`!handoff [agent]` — Automated context-preserving reset:

1. **Inject handoff prompt** into agent's session:
   ```
   URGENT: Session approaching context limit. Write a context handoff NOW.
   Save to memory/YYYY-MM-DD-HHMM-context-handoff.md with:
   - Current objective and status
   - Key decisions made this session
   - Pending work and blockers
   - Resume command for next session
   ```

2. **Wait for completion** (poll session for new message, timeout 120s)

3. **Verify handoff written** (check memory/ for new file)

4. **Reset session** (same as `!reset`)

5. **Inject breadcrumb** into fresh session:
   ```
   [SYSTEM] Previous session handed off. Read: memory/YYYY-MM-DD-HHMM-context-handoff.md
   ```

## Command Variants

```
!handoff           — Handoff agent bound to this channel
!handoff watson    — Handoff specific agent (uses aliases)
!handoff --force   — Skip the "are you sure" confirmation
!handoff --dry-run — Show what would happen without executing
```

## Implementation Notes

### Message Injection
Use OpenClaw's session injection (if available) or:
- Write to a "pending injection" file the gateway reads
- Or use the bridge protocol for main agent

### Polling for Completion
```javascript
async function waitForHandoff(agentId, timeout = 120000) {
  const start = Date.now();
  const memoryDir = path.join(AGENTS_DIR, agentId, 'workspace', 'memory');
  const beforeFiles = await fsp.readdir(memoryDir).catch(() => []);

  while (Date.now() - start < timeout) {
    await sleep(5000);
    const afterFiles = await fsp.readdir(memoryDir).catch(() => []);
    const newFiles = afterFiles.filter(f =>
      !beforeFiles.includes(f) && f.includes('handoff')
    );
    if (newFiles.length > 0) return newFiles[0];
  }
  return null; // timeout
}
```

### Edge Cases
- Agent doesn't respond (timeout → offer regular reset)
- Agent writes handoff but reset fails (keep handoff, report error)
- Multiple handoffs in quick succession (use timestamps)
- Agent is in isolated/cron session (may not have memory/ access)

## Success Criteria

1. Single command replaces 4-step manual process
2. Handoff file reliably created before reset
3. New session has breadcrumb pointing to handoff
4. Works for all agents, not just main

## Dependencies

- Ability to inject messages into agent sessions (investigate gateway API)
- File system access to agent workspace/memory/
- Session reset capability (already exists)

## Open Questions

1. Should handoff be opt-out (always runs) or opt-in (explicit `!handoff`)?
2. Should we auto-trigger at 90% context? (risky — might interrupt important work)
3. How to inject the breadcrumb into the new session? Gateway hook? First-message injection?

---

*Created: 2026-03-09*
*Context: Discussion with Jeremy about Rescue improvements*
