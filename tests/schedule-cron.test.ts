import { expect, test } from "bun:test";
import { matchesCron, parseCron } from "../src/scheduler/cron.ts";

test("matches five cron fields with lists, ranges, and steps", () => {
  const parsed = parseCron("5,20-30/5 8-10/2 1,15 6-8/2 *", "UTC");

  expect(matchesCron(parsed, Date.parse("2026-06-01T08:25:00Z"))).toBe(true);
  expect(matchesCron(parsed, Date.parse("2026-06-01T08:05:00Z"))).toBe(true);
  expect(matchesCron(parsed, Date.parse("2026-06-01T08:26:00Z"))).toBe(false);
  expect(matchesCron(parsed, Date.parse("2026-06-01T09:25:00Z"))).toBe(false);
  expect(matchesCron(parsed, Date.parse("2026-07-01T08:25:00Z"))).toBe(false);
});

test("uses OR when both day-of-month and day-of-week are restricted", () => {
  const parsed = parseCron("0 9 13 * 1", "UTC");

  expect(matchesCron(parsed, Date.parse("2026-04-06T09:00:00Z"))).toBe(true);
  expect(matchesCron(parsed, Date.parse("2026-05-13T09:00:00Z"))).toBe(true);
  expect(matchesCron(parsed, Date.parse("2026-05-14T09:00:00Z"))).toBe(false);
});

test("skips a scheduled local time that does not exist at the DST spring transition", () => {
  const parsed = parseCron("30 2 * * *", "America/New_York");

  expect(matchesCron(parsed, Date.parse("2026-03-08T06:30:00Z"))).toBe(false);
  expect(matchesCron(parsed, Date.parse("2026-03-08T07:30:00Z"))).toBe(false);
  expect(matchesCron(parsed, Date.parse("2026-03-09T06:30:00Z"))).toBe(true);
});

test("matches both UTC instants for a repeated local time at the DST fall transition", () => {
  const parsed = parseCron("30 1 * * *", "America/New_York");

  expect(matchesCron(parsed, Date.parse("2026-11-01T05:30:00Z"))).toBe(true);
  expect(matchesCron(parsed, Date.parse("2026-11-01T06:30:00Z"))).toBe(true);
  expect(matchesCron(parsed, Date.parse("2026-11-01T06:31:00Z"))).toBe(false);
});

test("accepts Sunday as either 0 or 7", () => {
  const sundayAsZero = parseCron("0 9 * * 0", "UTC");
  const sundayAsSeven = parseCron("0 9 * * 7", "UTC");
  const sunday = Date.parse("2026-04-05T09:00:00Z");
  const monday = Date.parse("2026-04-06T09:00:00Z");

  expect(matchesCron(sundayAsZero, sunday)).toBe(true);
  expect(matchesCron(sundayAsSeven, sunday)).toBe(true);
  expect(matchesCron(sundayAsZero, monday)).toBe(false);
  expect(matchesCron(sundayAsSeven, monday)).toBe(false);
});

test("rejects a six-field cron expression with seconds", () => {
  expect(() => parseCron("*/15 * * * * *", "UTC")).toThrow();
});

test("rejects an invalid IANA timezone", () => {
  expect(() => parseCron("* * * * *", "Etc/Not_A_Zone")).toThrow();
});
