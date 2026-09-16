/**
 * Octo 传输层的错误类型：`status` 带上 HTTP 状态，`permanent` 标记不可重试，`code` 携带
 * 服务端返回的错误码（例如 `err.server.bot_api.store_failed`）。宿主按这些字段决定重试与文案。
 */
export class OctoError extends Error {
  readonly status: number | undefined;
  readonly permanent: boolean;
  /** Server error identifier from the response body (`error.code`), when the body carries one. */
  readonly code: string | undefined;

  constructor(
    message: string,
    options: { status?: number; permanent?: boolean; code?: string } = {},
  ) {
    super(message);
    this.name = "OctoError";
    this.status = options.status;
    this.permanent = options.permanent ?? false;
    this.code = options.code;
  }
}
