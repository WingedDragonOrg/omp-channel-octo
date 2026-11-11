import { writeFile } from "node:fs/promises";
import { expect, test } from "bun:test";
import { withZedChromiumPage, type ZedChromiumFixture } from "./fixtures/zed-chromium.ts";

const zedAction = (fixture: ZedChromiumFixture) => fixture.page.getByRole("button", { name: /zed/i });

type ElementBox = { x: number; y: number; width: number; height: number };
type BoundedElement = { boundingBox(): Promise<ElementBox | null> };

async function gapBetween(first: BoundedElement, second: BoundedElement): Promise<number> {
  const [a, b] = await Promise.all([first.boundingBox(), second.boundingBox()]);
  if (!a || !b) return Number.POSITIVE_INFINITY;
  const horizontalGap = Math.max(0, a.x - (b.x + b.width), b.x - (a.x + a.width));
  const verticalGap = Math.max(0, a.y - (b.y + b.height), b.y - (a.y + a.height));
  return Math.hypot(horizontalGap, verticalGap);
}

async function expectNear(first: BoundedElement, second: BoundedElement): Promise<void> {
  expect(await gapBetween(first, second)).toBeLessThan(140);
}

async function expectNoActionNear(fixture: ZedChromiumFixture, link: BoundedElement): Promise<void> {
  const actions = zedAction(fixture);
  for (let index = 0; index < await actions.count(); index += 1) {
    const action = actions.nth(index);
    if (await action.isVisible()) expect(await gapBetween(action, link)).toBeGreaterThanOrEqual(140);
  }
}

test("clicking a Zed SSH link opens its exact URL only for a trusted click", async () => {
  const url = "zed://ssh/my-mini/Users/dong/workspace/project/docs/design.md";
  await withZedChromiumPage({
    target: "communication",
    links: [{ kind: "relative", label: "Remote file" }],
  }, async (fixture) => {
    const link = fixture.link(0);
    await link.evaluate((element, href) => element.setAttribute("href", href), url);
    await link.evaluate((element: Element) => (element as HTMLAnchorElement).click());
    expect(await fixture.bridgeCalls()).toEqual([]);
    await link.click();
    expect(await fixture.bridgeCalls()).toEqual([{ path: url }]);
  });
}, 15_000);

test("a file URL with spaces and #L line opens through a trusted Zed action", async () => {
  await withZedChromiumPage({
    target: "communication",
    links: [{ kind: "file-url", label: "Open Widget source", line: 42 }],
  }, async (fixture) => {
    const link = fixture.link(0);
    await link.hover();
    const action = zedAction(fixture);
    await action.waitFor({ state: "visible" });
    await expectNear(action, link);

    expect(await fixture.page.evaluate(() => Reflect.get(window, "octoEnhancer"))).toBeUndefined();
    await action.click();
    expect(await fixture.bridgeCalls()).toEqual([{ path: fixture.filePath, line: 42 }]);
  });
}, 15_000);

test("an absolute path link with spaces and :line opens from the main renderer", async () => {
  await withZedChromiumPage({
    target: "renderer",
    links: [{ kind: "absolute-path", label: "Open Widget source", line: 27 }],
  }, async (fixture) => {
    await fixture.link(0).hover();
    const action = zedAction(fixture);
    await action.waitFor({ state: "visible" });
    await action.click();
    expect(await fixture.bridgeCalls()).toEqual([{ path: fixture.filePath, line: 27 }]);
  });
}, 15_000);

test("a literal %20 in an absolute file path is not decoded into a space", async () => {
  await withZedChromiumPage({
    target: "renderer",
    links: [{ kind: "absolute-path", label: "Open percent-named file", line: 18 }],
  }, async (fixture) => {
    const literalPercentPath = fixture.filePath.replace("Widget with spaces.ts", "Widget%20with%20spaces.ts");
    await writeFile(literalPercentPath, "export const percentName = true;\n", "utf8");
    const link = fixture.link(0);
    await link.evaluate((anchor: Element, href: string) => anchor.setAttribute("href", href), `${literalPercentPath}:18`);
    await link.hover();
    const action = zedAction(fixture);
    await action.waitFor({ state: "visible" });
    await action.click();
    expect(await fixture.bridgeCalls()).toEqual([{ path: literalPercentPath, line: 18 }]);
  });
}, 15_000);

test("only the local file gets a Zed action; HTTP navigation stays ordinary and relative paths are ignored", async () => {
  await withZedChromiumPage({
    target: "communication",
    links: [
      { kind: "file-url", label: "Local Widget", line: 9 },
      { kind: "http", label: "Remote Widget", line: 1 },
      { kind: "relative", label: "Relative Widget", line: 9 },
    ],
  }, async (fixture) => {
    const action = zedAction(fixture);
    const localLink = fixture.link(0);
    await localLink.hover();
    await action.waitFor({ state: "visible" });
    await expectNear(action, localLink);

    const remoteLink = fixture.link(1);
    await remoteLink.hover();
    await expectNoActionNear(fixture, remoteLink);
    const remotePrefix = "https://example.invalid/source/Widget.ts";
    const remoteRequest = fixture.page.context().waitForEvent("request", (request: { url(): string }) => request.url().startsWith(remotePrefix));
    const blockedRequest = fixture.waitForBlockedRequest(remotePrefix);
    await Promise.all([remoteRequest, blockedRequest, remoteLink.click({ noWaitAfter: true })]);
    expect(fixture.blockedRequests().some((url) => url.startsWith(remotePrefix))).toBe(true);
    expect(await fixture.bridgeCalls()).toEqual([]);
    await expectNoActionNear(fixture, remoteLink);

    const relativeLink = fixture.link(2);
    await relativeLink.hover();
    await expectNoActionNear(fixture, relativeLink);
  });
}, 15_000);

test("a synthetic DOM click cannot invoke the native Zed capability", async () => {
  await withZedChromiumPage({
    target: "renderer",
    links: [{ kind: "file-url", label: "Open Widget source", line: 16 }],
  }, async (fixture) => {
    await fixture.link(0).hover();
    const action = zedAction(fixture);
    await action.waitFor({ state: "visible" });

    await action.evaluate((element: Element) => (element as HTMLButtonElement).click());
    expect(await fixture.bridgeCalls()).toEqual([]);
  });
}, 15_000);

test("disposal removes the Zed action and leaves no handler that calls the native capability", async () => {
  await withZedChromiumPage({
    target: "communication",
    links: [{ kind: "file-url", label: "Open Widget source", line: 31 }],
  }, async (fixture) => {
    const link = fixture.link(0);
    await link.hover();
    const action = zedAction(fixture);
    await action.waitFor({ state: "visible" });
    expect(await fixture.disposalRegistrations()).toBeGreaterThan(0);

    expect(await fixture.dispose()).toBeGreaterThan(0);
    await action.waitFor({ state: "detached" });
    await link.click();
    expect(await fixture.bridgeCalls()).toEqual([]);
  });
}, 15_000);
