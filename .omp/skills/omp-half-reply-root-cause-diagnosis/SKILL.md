---
name: omp-half-reply-root-cause-diagnosis
description: Diagnose apparent half-replies in omp-channel-octo by separating upstream generation stops from Octo stream delivery failures
---

# OMP half-reply root-cause diagnosis

Use this procedure when an Octo message appears to stop mid-sentence.

## 1. Correlate one task end to end

Use the same task/message/session identity and inspect:

- `tasks.result_text` and terminal task state;
- the OMP session's persisted assistant message and terminal metadata;
- stream outbox rows: `remote_message_id`, `desiredVersion`, `ackedVersion`, retry/error state.

Do not infer from a screenshot alone.

## 2. Classify the boundary

- **Upstream generation:** task result and persisted OMP assistant text are the same incomplete body, OMP reports a normal `agent_end`/`stop`, and the stream row has a remote ID with the latest version acknowledged and no retry/error.
- **Delivery/edit path:** the task/session has a longer authoritative body than the client, or a stream row remains pending/unknown, has an unacknowledged desired version, or records a transport/edit error.
- **Lifecycle/event path:** prompt does not reach terminal `agent_end`, task remains running/failed unexpectedly, or a `message_end` final snapshot is absent while upstream supplied a distinct authoritative body.

## 3. Interpret host tools correctly

`ask`, `channel_control`, and `octo_send_file` are optional host tools exposed to the model. A missing `host_tool_call` is not evidence of a channel bug; `ask` is only called when the model chooses to ask the owner. Also distinguish prompt acknowledgement from completion: the worker waits for terminal lifecycle events.

## 4. Interpret worker shutdown

A `SIGTERM` shortly after a completed prompt is normally the service's `finally` cleanup calling `worker.stop()`. It is causal only when it precedes the missing output or the prompt is still active.

## 5. Choose safe follow-up

Do not auto-rerun a normally stopped task because tool calls may have caused side effects. If upstream generation is implicated, capture only provider metadata—`finish_reason`, stop sequence, completion-token count, model/route, and effective output limits—without prompts, tool arguments/results, credentials, or raw sensitive payloads. Reproduce with the same route/model before changing stream code.
