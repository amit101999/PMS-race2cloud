const catalyst = require("zcatalyst-sdk-node");
const { runQuantityBackfill } = require("./runQuantityBackfill");

module.exports = async (jobRequest, context) => {
  try {
    const catalystApp = catalyst.initialize(context);
    const zcql = catalystApp.zcql();

    await runQuantityBackfill(zcql);

    console.log("[CalculateAllTransactionHoldingOnce] Completed successfully");
    context.closeWithSuccess();
  } catch (error) {
    console.error("[CalculateAllTransactionHoldingOnce] Fatal error:", error);
    context.closeWithFailure();
  }
};
