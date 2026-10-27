---
name: omp-channel-octo-inbound-delivery-triage
description: "Diagnose an omp-channel-octo Octo bot that seems deaf or that \"ignores\" a user reply: prove whether the message ever reached the channel (SQLite inbound/tasks + super logs + Octo message-id timestamps) before changing routing or answer logic, and separate the silent permission-gate drop classes (missing @mention in a mention-required group, unpermitted chat, control commands) from real delivery failures; instrument the WebSocket RECV path only as a bounded temporary trace."
---

# omp-channel-octo inbound delivery triage

Use when the Octo bot appears to ignore a message ("我说了没反应", reply-as-answer doesn't work, `/answer` does nothing). Order matters: prove delivery first, touch code last.

## 1. Did the message reach the service?

```sh
D=~/.local/state/omp-channel-octo/channel.sqlite
sqlite3 -header -column $D "select datetime(accepted_at/1000,'unixepoch','localtime') t, kind, sender_id, substr(text,1,30) text from inbound order by accepted_at desc limit 6;"
sqlite3 -header -column $D "select datetime(accepted_at/1000,'unixepoch','localtime') t, state, substr(text,1,30) text from tasks order by accepted_at desc limit 6;"
sqlite3 -header -column $D "select state, count(*) from questions group by state;"
```

- A missing `inbound` row does NOT prove "never received": the message can be dropped by the silent permission gate **before** any persistence. Run §1b before concluding non-delivery.
- `questions` shows the real deadline: a question is asked at `created_at` and expires 10 minutes later (`answered_at` of an `expired` row = expiry). A reply after that can never be an answer; a service restart also invalidates waits.
- `outbox` shows what the channel actually sent (and when), which separates "model is silent" from "delivery is broken".

## 1b. Silent drop classes (no inbound row, no log, no reply)

A message can be rejected inside `CoreService.isAllowed` / `handleUnlistedOwnerContact` with zero trace. The only exception is the owner's @Bot mention in an UNLISTED chat (one throttled notice). Check these before instrumenting (host injects the bot identity; `octo-cli auth status` first; all reads):

1. **Chat permitted?** `allowed_chats` rows ∪ config `allowedGroupIds`; a thread is permitted by its own id or the parent group's.
2. **Mention required?** `octo-cli api GET /v1/bot/groups/<group_no>/mention_pref` (threads use the parent group). `effective=false` + a message with no `@Bot` mention (`payload.mention` null) = dropped by design — spec acceptance 24: 不启动任务、不回复也不落任何入站副作用. Groups differ; 2026-09-17 实测: 某个 issue 群（`<group_no>`）= false，另几个群 = true. DMs never need a mention; non-owner and bot echoes are always silent.
3. **Control commands take the same gate**: an unmentioned `/stop`, `/cancel` or `/status` in a mention-required chat is dropped, so "当前没有可取消的任务。" never appears. Silence is not "no task to cancel".
4. **Did the server even get it?** `octo-cli api POST /v1/bot/messages/sync --data '{"channel_id":"<id>","channel_type":<2|5>,"start_message_seq":0,"end_message_seq":0,"limit":200,"pull_mode":1}'` — the base64 `payload` decodes to `content` + `mention` (null when absent) plus `message_seq`. This is the server-side record and separates "server never had it" from "channel ignored it".

## 2. Read the live logs

```sh
super info omp-channel-octo | grep -E "State|PID"   # program id 由 super list 给出
tail -30 ~/.super/logs/<program-id>.err
tail -5  ~/.super/logs/<program-id>.out
```

- `.out` prints one line per process start -> use it to timestamp restarts.
- `.err` only carries errors and *dropped* inbound frames (`Octo message channel type N is unsupported`, `Octo group message has no channel id`, and since 2026-09-15 the same lines carry `channelType=/sender=/channel=`). Gate drops (§1b) print nothing, so a silent `.err` with an authenticated socket is consistent with both "no user message arrived" and "a message was rejected by design"; the SQLite rows, `mention_pref`, and `messages/sync` decide which.
- Service-owned logs live in `~/.oco/logs/channel-YYYY-MM-DD.log` (group-history injections, progress-card failures, restarts); `[group-history]` lines mark turns, never gate decisions.
- `Octo WebSocket session authenticated` / `disconnected with reason N` lines only exist in debug builds; a kick (`reason != 0`) currently makes the transport permanently stop (`failPermanently`), so the process stays Healthy while deaf. Restarting is then the only recovery.

## 3. Octo message ids are a clock

Ids are snowflakes: `id >> 22` is the server's millisecond counter, so the timestamp ordering is monotonic and comparable with `accepted_at`. Calibrate against one message whose send time you know; 2026-09-17 observed `wall_ms ≈ (id>>22) + 1288834974121` (±1s).

```sh
sqlite3 $D "select message_id from inbound order by accepted_at desc limit 6;" | while read id; do
  python3 -c "import datetime;print($id, ($id>>22))"; done
```

- Δ(id>>22) between two rows equals Δ wall-clock milliseconds. A batch of user messages sharing one arrival second, while the UI timestamps are much older, means the client/server held or re-sent them; the channel received them late but intact.
- Client UI timestamps are optimistic: a message can be displayed as sent while the server only allocates its id at a later flush. Do not treat UI times as delivery proof — verify with `messages/sync` (§1b.4).

## 4. Only then instrument

If the DB shows nothing, the gate classes are ruled out, and frames are known to arrive, add a temporary trace in `handleRecv` (`src/octo/index.ts`), right after `const channelType = reader.byte();`:

```ts
this.logger(`Octo frame trace: type=${channelType} sender=${senderId} channel=${rawChannelId} setting=${setting} id=${messageId} payload=${JSON.stringify(payload).slice(0, 160)}`);
```

- Restart (`super -y restart omp-channel-octo`), have the user send exactly one message, read `.err`, then remove the trace.
- Bounded alternative that can stay in the tree: the header string appended to the normalization-failure log.
- Never leave the full per-frame trace deployed: Octo broadcasts `{"cmd":"syncMessageExtra","type":99}` frames several times per second while the peer is active, which floods `.err`.

## 5. Deployment etiquette

- `super -y restart omp-channel-octo` interrupts running/queued tasks (they end `cancelled`/`interrupted` and the chat is told). Check first: `sqlite3 $D "select state,count(*) from tasks group by state;"` — restart only when nothing is `queued`/`started`.
- Root cause of a silent period is often outside the channel. Say so with evidence instead of "fixing" the routing layer; a restart only swaps the process.
