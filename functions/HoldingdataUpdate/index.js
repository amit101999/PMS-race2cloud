const catalyst = require("zcatalyst-sdk-node");

module.exports = async (event, context) => {
  const DATA = event.data; //event data
  const TIME = event.time; //event occured time

  const RAW_DATA = event.getRawData();
  console.log("RAW DATA ", JSON.stringify(RAW_DATA));

  const SOURCE_ACTION = event.getAction(); //(insert | fetch | invoke ...)
  const SOURCE_TYPE = event.getSource(); //(datastore | cache | queue ...)
  const SOURCE_ENTITY_ID = event.getSourceEntityId(); //if type is datastore then entity id is tableid
  const SOURCE_BUS_DETAILS = event.getEventBusDetails(); //event bus details

  const PROJECT_DETAILS = event.getProjectDetails(); //event project details

  //  get catalyst app
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  const tableName = "Holdings";

  await zcql.executeZCQLQuery(`insert into ${tableName} (WS_Account_code , ISIN , QTY , Holding_Date) values ('1000AC' ,
    '10NSUS' , 100 , '2022-01-01')`);

  /*
        CONTEXT FUNCTIONALITIES
    */
  context.closeWithSuccess(); //end of application with success
  // context.closeWithFailure(); //end of application with failure
};
