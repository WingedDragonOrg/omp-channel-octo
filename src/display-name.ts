/**
 * uid → 显示名解析：把回答署名里的 uid 渲染成用户看得懂的名字。名字只是美化——它绝不参与
 * 身份判断，也绝不能挡住作答与卡片投递：查不到、超时或失败都让该 uid 回退成它自己。
 *
 * 缓存有界（TTL + 条目上限）并合并同一 uid 的在途查询：同一张卡片反复改写、同一个群反复
 * 作答都只花一次网络调用；失败不缓存，下一次渲染仍会重试，且每次查询都有预算上限——即使查询
 * 实现无视取消信号，解析本身也按时收尾，不会把调用方拖住。
 */
export interface DisplayNameLookup {
  /** 查询一个 uid 的显示名；查不到或失败返回空串。 */
  (uid: string, signal?: AbortSignal): Promise<string>;
}

export interface DisplayNameCacheOptions {
  /** 命中后的有效期，默认 30 分钟。 */
  ttlMs?: number;
  /** 条目上限，超出按插入顺序淘汰最旧的，默认 200。 */
  maxEntries?: number;
  /** 单次查询的预算，默认 3 秒。 */
  lookupTimeoutMs?: number;
}

export interface DisplayNameCache {
  /**
   * 解析一批 uid 的显示名。只返回解析成功且非空的条目；失败、超时与空名被吞掉，回退到 uid
   * 由渲染方决定——因此本方法永不抛错。
   */
  names(uids: readonly string[], signal?: AbortSignal): Promise<ReadonlyMap<string, string>>;
}

export const DISPLAY_NAME_TTL_MS = 30 * 60 * 1_000;
export const DISPLAY_NAME_MAX_ENTRIES = 200;
export const DISPLAY_NAME_LOOKUP_TIMEOUT_MS = 3_000;

const NO_NAMES: ReadonlyMap<string, string> = new Map();

/** 按预算收尾：查询实现不理会取消信号时，这里也把结果按时交还给调用方。 */
async function withinBudget<T>(work: Promise<T>, budgetMs: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("display name lookup timed out")), budgetMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export function createDisplayNameCache(
  lookup: DisplayNameLookup,
  options: DisplayNameCacheOptions = {},
): DisplayNameCache {
  const ttlMs = options.ttlMs ?? DISPLAY_NAME_TTL_MS;
  const maxEntries = options.maxEntries ?? DISPLAY_NAME_MAX_ENTRIES;
  const lookupTimeoutMs = options.lookupTimeoutMs ?? DISPLAY_NAME_LOOKUP_TIMEOUT_MS;
  const cached = new Map<string, { name: string; expiresAt: number }>();
  const inFlight = new Map<string, Promise<string>>();

  function remember(uid: string, name: string): void {
    // 重新插入让 Map 的插入顺序保持 LRU：最新解析的名字排在最前。
    cached.delete(uid);
    cached.set(uid, { name, expiresAt: Date.now() + ttlMs });
    for (const oldest of cached.keys()) {
      if (cached.size <= maxEntries) break;
      cached.delete(oldest);
    }
  }

  function resolve(uid: string, signal?: AbortSignal): Promise<string> {
    const hit = cached.get(uid);
    if (hit !== undefined && hit.expiresAt > Date.now()) return Promise.resolve(hit.name);
    const pending = inFlight.get(uid);
    if (pending !== undefined) return pending;
    const request = (async () => {
      try {
        const name = (await withinBudget(lookup(uid, signal), lookupTimeoutMs)).trim();
        if (name.length > 0) remember(uid, name);
        return name;
      } catch {
        return "";
      } finally {
        inFlight.delete(uid);
      }
    })();
    inFlight.set(uid, request);
    return request;
  }

  return {
    async names(uids: readonly string[], signal?: AbortSignal): Promise<ReadonlyMap<string, string>> {
      const wanted: string[] = [];
      for (const raw of uids) {
        const uid = raw.trim();
        if (uid.length === 0 || wanted.includes(uid)) continue;
        wanted.push(uid);
      }
      if (wanted.length === 0) return NO_NAMES;
      const budget = AbortSignal.timeout(lookupTimeoutMs);
      const deadline = signal === undefined ? budget : AbortSignal.any([signal, budget]);
      const resolved = await Promise.all(
        wanted.map(async (uid) => [uid, await resolve(uid, deadline)] as const),
      );
      const names = new Map<string, string>();
      for (const [uid, name] of resolved) {
        if (name.length > 0) names.set(uid, name);
      }
      return names.size === 0 ? NO_NAMES : names;
    },
  };
}
