/**
 *
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 *
 */

const catalyst = require("zcatalyst-sdk-node");
const { runQuantityBackfill } = require("./runQuantityBackfill");

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  await runQuantityBackfill(zcql);

  context.closeWithSuccess();
};
