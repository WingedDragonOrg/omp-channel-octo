export interface ParsedCron {
  readonly expression: string;
  readonly timezone: string;
}

interface FieldSpec {
  readonly name: string;
  readonly min: number;
  readonly max: number;
  readonly inputMax?: number;
  readonly normalize?: (value: number) => number;
}

interface CronField {
  readonly values: Uint8Array;
  readonly unrestricted: boolean;
}

interface CompiledCron {
  readonly minute: CronField;
  readonly hour: CronField;
  readonly dayOfMonth: CronField;
  readonly month: CronField;
  readonly dayOfWeek: CronField;
  readonly formatter: Intl.DateTimeFormat;
}

const FIELD_SPECS: readonly FieldSpec[] = [
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "day of month", min: 1, max: 31 },
  { name: "month", min: 1, max: 12 },
  { name: "day of week", min: 0, max: 6, inputMax: 7, normalize: (value) => value === 7 ? 0 : value },
];

const WEEKDAY_INDEX: Readonly<Record<string, number>> = {
  Sun: 0,
  Mon: 1,
  Tue: 2,
  Wed: 3,
  Thu: 4,
  Fri: 5,
  Sat: 6,
};

const compiledCrons = new WeakMap<ParsedCron, CompiledCron>();

function invalidField(name: string): never {
  throw new RangeError(`Invalid cron ${name} field.`);
}

function parseInteger(text: string, min: number, max: number, fieldName: string): number {
  if (!/^\d+$/.test(text)) invalidField(fieldName);
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value < min || value > max) invalidField(fieldName);
  return value;
}

function parseField(expression: string, spec: FieldSpec): CronField {
  const inputMax = spec.inputMax ?? spec.max;
  const values = new Uint8Array(spec.max + 1);

  for (const component of expression.split(",")) {
    if (component.length === 0) invalidField(spec.name);
    const slash = component.indexOf("/");
    if (slash !== component.lastIndexOf("/")) invalidField(spec.name);

    const base = slash < 0 ? component : component.slice(0, slash);
    const hasStep = slash >= 0;
    const step = hasStep
      ? parseInteger(component.slice(slash + 1), 1, Number.MAX_SAFE_INTEGER, spec.name)
      : 1;

    let start: number;
    let end: number;
    if (base === "*") {
      start = spec.min;
      end = inputMax;
    } else {
      const hyphen = base.indexOf("-");
      if (hyphen >= 0) {
        if (hyphen !== base.lastIndexOf("-")) invalidField(spec.name);
        start = parseInteger(base.slice(0, hyphen), spec.min, inputMax, spec.name);
        end = parseInteger(base.slice(hyphen + 1), spec.min, inputMax, spec.name);
        if (end < start) invalidField(spec.name);
      } else {
        start = parseInteger(base, spec.min, inputMax, spec.name);
        end = hasStep ? inputMax : start;
      }
    }

    for (let value = start; value <= end; value += step) {
      const normalized = spec.normalize ? spec.normalize(value) : value;
      values[normalized] = 1;
    }
  }

  let unrestricted = true;
  for (let value = spec.min; value <= spec.max; value += 1) {
    if (values[value] === 0) {
      unrestricted = false;
      break;
    }
  }
  return { values, unrestricted };
}

function createFormatter(timezone: string): Intl.DateTimeFormat {
  if (
    typeof timezone !== "string" ||
    timezone.length === 0 ||
    timezone !== timezone.trim() ||
    /^(?:[+-]\d{2}(?::?\d{2})?|(?:GMT|UTC)[+-]\d{1,2}:\d{2})$/i.test(timezone)
  ) {
    throw new RangeError("Invalid IANA timezone.");
  }

  try {
    return new Intl.DateTimeFormat("en-US-u-ca-gregory-nu-latn", {
      timeZone: timezone,
      calendar: "gregory",
      numberingSystem: "latn",
      hourCycle: "h23",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
      weekday: "short",
    });
  } catch {
    throw new RangeError("Invalid IANA timezone.");
  }
}

export function parseCron(expression: string, timezone: string): ParsedCron {
  if (typeof expression !== "string") throw new TypeError("Cron expression must be a string.");
  const fields = expression.trim().split(/\s+/);
  if (fields.length !== FIELD_SPECS.length) {
    throw new RangeError("Cron expression must contain exactly five fields.");
  }

  const formatter = createFormatter(timezone);
  const parsedFields = FIELD_SPECS.map((spec, index) => parseField(fields[index]!, spec));
  const parsed = Object.freeze({ expression, timezone });
  compiledCrons.set(parsed, {
    minute: parsedFields[0]!,
    hour: parsedFields[1]!,
    dayOfMonth: parsedFields[2]!,
    month: parsedFields[3]!,
    dayOfWeek: parsedFields[4]!,
    formatter,
  });
  return parsed;
}

export function matchesCron(parsed: ParsedCron, utcMs: number): boolean {
  const cron = compiledCrons.get(parsed);
  if (!cron) throw new TypeError("Cron must be parsed by parseCron().");
  if (!Number.isSafeInteger(utcMs) || utcMs % 60_000 !== 0) return false;

  const instant = new Date(utcMs);
  if (Number.isNaN(instant.getTime())) return false;

  let minute = -1;
  let hour = -1;
  let dayOfMonth = -1;
  let month = -1;
  let dayOfWeek = -1;
  for (const part of cron.formatter.formatToParts(instant)) {
    switch (part.type) {
      case "minute":
        minute = Number(part.value);
        break;
      case "hour":
        hour = Number(part.value);
        break;
      case "day":
        dayOfMonth = Number(part.value);
        break;
      case "month":
        month = Number(part.value);
        break;
      case "weekday":
        dayOfWeek = WEEKDAY_INDEX[part.value] ?? -1;
        break;
    }
  }

  if (minute < 0 || hour < 0 || dayOfMonth < 0 || month < 0 || dayOfWeek < 0) return false;

  const dayOfMonthMatches = cron.dayOfMonth.values[dayOfMonth] === 1;
  const dayOfWeekMatches = cron.dayOfWeek.values[dayOfWeek] === 1;
  const dayMatches = cron.dayOfMonth.unrestricted
    ? dayOfWeekMatches
    : cron.dayOfWeek.unrestricted
      ? dayOfMonthMatches
      : dayOfMonthMatches || dayOfWeekMatches;

  return (
    cron.minute.values[minute] === 1 &&
    cron.hour.values[hour] === 1 &&
    cron.month.values[month] === 1 &&
    dayMatches
  );
}
