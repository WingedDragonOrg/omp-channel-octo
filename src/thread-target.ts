/**
 * Single source for Octo thread target parsing (`<groupId>____<threadId>`): both the
 * transport (context path) and the service (work directory inheritance) resolve the
 * parent group id through this helper.
 */
export function threadParentGroupId(channelId: string): string | null {
  const separator = channelId.indexOf("____");
  if (separator <= 0 || separator + 4 >= channelId.length) return null;
  return channelId.slice(0, separator);
}

/** The inverse of `threadParentGroupId`: the channel id a thread binding is keyed by. */
export function threadChannelId(groupId: string, shortId: string): string {
  return `${groupId}____${shortId}`;
}
