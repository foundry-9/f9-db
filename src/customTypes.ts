import type {
  ComparableValue,
  CustomTypeDefinition,
  CustomTypeRegistry
} from './types.js';

function assertStringInput(value: unknown, name: string): string {
  if (typeof value === 'string') {
    return value;
  }
  throw new Error(`Custom type '${name}' requires a string input`);
}

function normalizeInteger(value: unknown): number {
  let parsed: number;
  if (typeof value === 'string') {
    parsed = Number.parseInt(value, 10);
  } else if (typeof value === 'number') {
    parsed = value;
  } else {
    parsed = Number.NaN;
  }
  if (!Number.isSafeInteger(parsed)) {
    throw new TypeError(`Value '${String(value)}' is not a safe integer`);
  }
  return parsed;
}

function normalizeFloat(value: unknown): number {
  let parsed: number;
  if (typeof value === 'string') {
    parsed = Number.parseFloat(value);
  } else if (typeof value === 'number') {
    parsed = value;
  } else {
    parsed = Number.NaN;
  }
  if (!Number.isFinite(parsed)) {
    throw new TypeError(`Value '${String(value)}' is not a finite float`);
  }
  return parsed;
}

const DECIMAL_REGEX = /^[+-]?\d+(?:\.\d+)?$/;

/**
 * Normalize raw numbers/strings into canonical decimal strings without scientific notation.
 * Rejects non-decimal inputs so the stored value can be compared lexically.
 */
function normalizeDecimalString(value: unknown): string {
  if (typeof value !== 'string' && typeof value !== 'number') {
    throw new TypeError('Decimal inputs must be a string or number');
  }

  let raw: string;
  if (typeof value === 'number') {
    raw = Number.isFinite(value) ? value.toString() : Number.NaN.toString();
  } else {
    raw = value.trim();
  }

  if (!DECIMAL_REGEX.test(raw)) {
    throw new Error(
      `Invalid decimal '${String(value)}'; use plain digits with optional leading sign and decimal point`
    );
  }

  const [intPartRaw, fracPartRaw = ''] = raw.split('.');
  const sign = intPartRaw.startsWith('-') ? '-' : '';
  const cleanedInt = intPartRaw.replace(/^[-+]?0+(?=\d)/, '').replace(/^[-+]?$/, '0');
  const unsignedInt = cleanedInt.replace(/^[-+]?/, '');
  const fracPart = fracPartRaw.replace(/0+$/, '');
  const isZero = (unsignedInt === '' || unsignedInt === '0') && fracPart === '';
  const normalizedInt = isZero ? '0' : unsignedInt;
  const signPrefix = sign === '-' && !isZero ? '-' : '';

  return fracPart.length > 0
    ? `${signPrefix}${normalizedInt}.${fracPart}`
    : `${signPrefix}${normalizedInt}`;
}

function applySign(comparison: number, sign: number): number {
  return sign === 1 ? comparison : -comparison;
}

function compareFractionalParts(leftFrac: string, rightFrac: string, sign: number): number {
  const maxFracLength = Math.max(leftFrac.length, rightFrac.length);
  const leftFracPadded = leftFrac.padEnd(maxFracLength, '0');
  const rightFracPadded = rightFrac.padEnd(maxFracLength, '0');
  const fracComparison = leftFracPadded.localeCompare(rightFracPadded);
  
  if (fracComparison === 0) {
    return 0;
  }
  return applySign(fracComparison < 0 ? -1 : 1, sign);
}

/**
 * Decimal-safe comparator that orders canonical decimal strings without losing precision.
 * Falls back to `null` when either side is not a decimal string.
 */
function compareDecimalStrings(left: ComparableValue, right: ComparableValue): number | null {
  if (typeof left !== 'string' || typeof right !== 'string') {
    return null;
  }

  const [leftIntRaw, leftFrac = ''] = left.split('.');
  const [rightIntRaw, rightFrac = ''] = right.split('.');
  const leftSign = leftIntRaw.startsWith('-') ? -1 : 1;
  const rightSign = rightIntRaw.startsWith('-') ? -1 : 1;
  const leftInt = leftIntRaw.replace(/^[-+]/, '');
  const rightInt = rightIntRaw.replace(/^[-+]/, '');

  if (leftSign !== rightSign) {
    return leftSign < rightSign ? -1 : 1;
  }

  if (leftInt.length !== rightInt.length) {
    const lengthComparison = leftInt.length < rightInt.length ? -1 : 1;
    return applySign(lengthComparison, leftSign);
  }

  const intComparison = leftInt.localeCompare(rightInt);
  if (intComparison !== 0) {
    return applySign(intComparison, leftSign);
  }

  return compareFractionalParts(leftFrac, rightFrac, leftSign);
}

const intType: CustomTypeDefinition<number, number> = {
  name: 'int',
  description: 'Coerces to a safe integer, rejecting fractional inputs.',
  baseType: 'number',
  accepts: ['number', 'string integer'],
  fromInput: normalizeInteger,
  toComparable: (value) => value,
  project: (value) => value
};

const floatType: CustomTypeDefinition<number, number> = {
  name: 'float',
  description: 'Coerces to a finite floating point number.',
  baseType: 'number',
  accepts: ['number', 'string float'],
  fromInput: normalizeFloat,
  toComparable: (value) => value,
  project: (value) => value
};

const decimalType: CustomTypeDefinition<string, number> = {
  name: 'decimal',
  description: 'Canonical decimal string preserved in storage; projected as a number.',
  baseType: 'string',
  accepts: ['string decimal', 'number (avoid exponents for precision)'],
  fromInput: normalizeDecimalString,
  toComparable: (value) => value,
  compare: compareDecimalStrings,
  project: (value) => Number.parseFloat(value)
};

const varcharType: CustomTypeDefinition<string, string> = {
  name: 'varchar',
  description: 'Fixed-length string that enforces a maximum length.',
  baseType: 'string',
  accepts: ['string'],
  fromInput: (value, options) => {
    const stringValue = assertStringInput(value, 'varchar');
    const maxLength = Number.isFinite(options?.maxLength)
      ? Number(options?.maxLength)
      : undefined;

    if (maxLength !== undefined && stringValue.length > maxLength) {
      throw new Error(
        `Varchar value exceeds maxLength=${maxLength}; received length=${stringValue.length}`
      );
    }

    return stringValue;
  },
  toComparable: (value) => value,
  project: (value) => value
};

/**
 * Build the default custom type registry, optionally overriding any built-in definitions.
 *
 * @param overrides Custom type overrides or additions keyed by type name.
 * @returns Combined registry that includes built-in and user-provided types.
 */
export function createDefaultCustomTypes(
  overrides: CustomTypeRegistry = {}
): CustomTypeRegistry {
  return {
    int: intType as CustomTypeDefinition<unknown, unknown>,
    float: floatType as CustomTypeDefinition<unknown, unknown>,
    decimal: decimalType as CustomTypeDefinition<unknown, unknown>,
    varchar: varcharType as CustomTypeDefinition<unknown, unknown>,
    ...overrides
  };
}

/**
 * Export decimal normalization/comparison helpers so consumers can reuse them in custom types.
 */
export { normalizeDecimalString, compareDecimalStrings };
