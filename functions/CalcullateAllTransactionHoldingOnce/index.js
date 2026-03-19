const catalyst = require("zcatalyst-sdk-node");
const { runQuantityBackfill } = require("./runQuantityBackfill");

module.exports = async (jobRequest, context) => {
  try {
    const catalystApp = catalyst.initialize(context);

    await runQuantityBackfill(catalystApp);

    console.log("[CalculateAllTransactionHoldingOnce] Completed successfully");
    context.closeWithSuccess();
  } catch (error) {
    console.error("[CalculateAllTransactionHoldingOnce] Fatal error:", error);
    context.closeWithFailure();
  }
};
