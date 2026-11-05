export type ReferenceKind = "mr" | "pr";

export interface ThreadReference {
  kind: ReferenceKind;
  url: string;
}

export interface ThreadReferences {
  mr: string[];
  pr: string[];
}

export interface ReferenceCandidate extends ThreadReference {
  id: string;
  excerpt: string;
}

export interface MatchedReferenceHistory {
  candidateId: string;
  url: string;
  taskRequirement: string;
  reportExcerpt: string;
}

export interface ReferenceEvidence {
  thread: string;
  task: string;
  recent: readonly { role: "user" | "assistant"; text: string }[];
  matchedHistory?: readonly MatchedReferenceHistory[];
  candidates: readonly ReferenceCandidate[];
}

export interface ReferenceJudgment {
  id: string;
  choice: "work_object" | "reference_or_unrelated" | "insufficient_evidence";
  probability: number;
}

export type ReferenceJudge = (
  evidence: ReferenceEvidence,
  signal: AbortSignal,
) => Promise<readonly ReferenceJudgment[]>;

export type DecisionFetch = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

export interface EnabledDecisionsConfig {
  enabled: true;
  protocol: "typesafe-systemone" | "openai-decisions";
  baseUrl: string;
  model: string;
  apiKeyEnv: string;
  apiKey: string;
}

export type DecisionsConfig = { enabled: false } | EnabledDecisionsConfig;
