const test = require("node:test");
const assert = require("node:assert/strict");
const {
  walkShareRows,
  findNegativeHoldings,
  STATUS,
} = require("../functions/NegativeHoldingsApi/negativeHoldings.js");

const row = (TYPE, QUANTITY, HOLDING, extra = {}) => ({
  TYPE,
  QUANTITY,
  HOLDING,
  ISIN: "INE000000001",
  TRANSACTION_DATE: "2026-01-10",
  SETTLEMENT_DATE: "2026-01-11",
  ...extra,
});

test("hold 100, sell 150, buy 80: short by 50, then covered with a real holding of 30", () => {
  const { rows, summary } = walkShareRows([
    row("BY-", 100, 100),
    row("SL+", 150, 0, { TRANSACTION_DATE: "2026-02-01" }),
    row("BY-", 80, 80),
  ]);

  assert.deepEqual(rows.map((r) => r.shortBy), [0, 50, 0]);
  assert.deepEqual(rows.map((r) => r.realHolding), [100, -50, 30]);
  assert.equal(summary.totalShort, 50);
  assert.equal(summary.firstShortDate, "2026-02-01");
  assert.equal(summary.lowestRealHolding, -50);
  assert.equal(summary.realHolding, 30);
  assert.equal(summary.storedHolding, 80);
  assert.equal(summary.status, STATUS.COVERED_LATER);
});

test("still short at the end is Still negative, with the exact minus value", () => {
  const { summary } = walkShareRows([row("BY-", 100, 100), row("SL+", 150, 0)]);
  assert.equal(summary.status, STATUS.STILL_NEGATIVE);
  assert.equal(summary.realHolding, -50);
  assert.equal(summary.storedHolding, 0);
  assert.equal(summary.holdingBeforeCorporateAction, null);
});

test("fractional units keep their exact value", () => {
  const { summary } = walkShareRows([
    row("BY-", 10.5, 10.5),
    row("SL+", 12.123456, 0),
  ]);
  assert.equal(summary.realHolding, -1.623456);
});

test("the exact value matches buys minus sells when there are no corporate actions", () => {
  const { summary } = walkShareRows([
    row("BY-", 100, 100),
    row("SL+", 150, 0),
    row("SL+", 10, 0),
    row("BY-", 25, 25),
  ]);
  assert.equal(summary.realHolding, 100 - 150 - 10 + 25);
});

test("selling exactly what is held is not flagged", () => {
  const { summary } = walkShareRows([row("BY-", 100, 100), row("SL+", 100, 0)]);
  assert.equal(summary, null);
});

test("a sell with no earlier holding is short by its full quantity", () => {
  const { summary } = walkShareRows([row("SL+", 40, 0)]);
  assert.equal(summary.totalShort, 40);
  assert.equal(summary.status, STATUS.STILL_NEGATIVE);
});

test("buying the shortfall back exactly is Covered later", () => {
  const { summary } = walkShareRows([row("SL+", 40, 0), row("BY-", 40, 40)]);
  assert.equal(summary.realHolding, 0);
  assert.equal(summary.status, STATUS.COVERED_LATER);
});

test("other sell types count too", () => {
  for (const type of ["SQS", "OPO", "NF-"]) {
    const { summary } = walkShareRows([row("BY-", 10, 10), row(type, 15, 0)]);
    assert.equal(summary.totalShort, 5, type);
  }
});

test("a sell after a split uses the new quantity, so there is no false alarm", () => {
  const { summary } = walkShareRows([
    row("BY-", 100, 100),
    row("SPLIT", 500, 500),
    row("SL+", 500, 0),
  ]);
  assert.equal(summary, null);
});

test("a sell after a bonus uses the higher holding", () => {
  const { summary } = walkShareRows([
    row("BY-", 100, 100),
    row("BONUS", 100, 200),
    row("SL+", 200, 0),
  ]);
  assert.equal(summary, null);
});

test("merger rows are not sells: receiving and closing rows are not flagged", () => {
  assert.equal(
    walkShareRows([row("MERGER", 60, 60), row("SL+", 60, 0)]).summary,
    null,
    "surviving share",
  );
  assert.equal(
    walkShareRows([row("BY-", 100, 100), row("MERGER", 100, 0)]).summary,
    null,
    "merged-away share",
  );
});

test("a demerger with no oversell is not flagged", () => {
  const { summary } = walkShareRows([
    row("BY-", 100, 100),
    row("DEMERGER", 100, 100),
    row("SL+", 100, 0),
  ]);
  assert.equal(summary, null);
});

test("a shortfall followed by a split needs checking by hand", () => {
  const { rows, summary } = walkShareRows([
    row("BY-", 100, 100),
    row("SL+", 150, 0),
    row("SPLIT", 500, 500),
  ]);
  assert.equal(summary.status, STATUS.CHECK_BY_HAND);
  assert.equal(summary.realHolding, null);
  assert.equal(summary.holdingBeforeCorporateAction, -50);
  assert.equal(rows[1].realHolding, -50);
  assert.equal(rows[2].realHolding, null);
});

test("a split before the shortfall is fine: the status still comes from the numbers", () => {
  const { summary } = walkShareRows([
    row("BY-", 100, 100),
    row("SPLIT", 500, 500),
    row("SL+", 600, 0),
  ]);
  assert.equal(summary.totalShort, 100);
  assert.equal(summary.status, STATUS.STILL_NEGATIVE);
});

test("dividend rows don't reset the holding used for the next sell", () => {
  const { summary } = walkShareRows([
    row("BY-", 100, 100),
    row("DIV+", 0, 0),
    row("SL+", 100, 0),
  ]);
  assert.equal(summary, null);
});

test("several shortfalls add up", () => {
  const { summary } = walkShareRows([
    row("SL+", 10, 0),
    row("BY-", 5, 5),
    row("SL+", 8, 0),
  ]);
  assert.equal(summary.totalShort, 13);
  assert.equal(summary.realHolding, -13);
  assert.equal(summary.status, STATUS.STILL_NEGATIVE);
});

test("fractional units don't flag rounding noise", () => {
  const { summary } = walkShareRows([
    row("BY-", 10.123456, 10.123456),
    row("SL+", 10.1234561, 0),
  ]);
  assert.equal(summary, null);
});

test("two shares in one account are worked out separately", () => {
  const shares = findNegativeHoldings([
    row("BY-", 100, 100, { ISIN: "INE000000001" }),
    row("SL+", 150, 0, { ISIN: "INE000000002" }),
    row("SL+", 100, 0, { ISIN: "INE000000001" }),
  ]);
  assert.equal(shares.length, 1);
  assert.equal(shares[0].isin, "INE000000002");
  assert.equal(shares[0].totalShort, 150);
});

test("no rows means nothing to report", () => {
  assert.deepEqual(findNegativeHoldings([]), []);
});
