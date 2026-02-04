const fs = require("fs");
const { PassThrough } = require("stream");
const readline = require("readline");
const catalyst = require("zcatalyst-sdk-node");

/**
 * 
 * @param {import("./types/job").JobRequest} jobRequest 
 * @param {import("./types/job").Context} context 
 */

module.exports = async (jobRequest, context) => {
	const catalystApp = catalyst.initialize(context);
	const zcql = catalystApp.zcql();

	const fileName = jobRequest.getJobParam("fileName");
	const jobName = jobRequest.getJobParam("jobName");
	const stratus = catalystApp.stratus();
	const bucket = stratus.bucket("upload-data-bucket");

	try {
		// -------------------------
		// Job start
		// -------------------------
		await zcql.executeZCQLQuery(`
		INSERT INTO Jobs (jobName, status)
		VALUES ('${jobName}', 'PENDING')
	  `);


		const objectStream = await bucket.getObject(fileName);

		// 2️⃣ Create PassThrough
		const passThrough = new PassThrough();

		// 3️⃣ Pipe Stratus stream → PassThrough
		objectStream.pipe(passThrough);

		// 4️⃣ Consume with readline
		const rl = readline.createInterface({
			input: passThrough,
			crlfDelay: Infinity
		});

		let headers = [];
		let isHeader = true;

		for await (const line of rl) {
			const cols = line.split(",");

			if (isHeader) {
				headers = cols.map(h =>
				  h
					.replace(/\uFEFF/g, "") // remove BOM
					.replace(/\r/g, "")     // remove CR
					.trim()                 // remove spaces
				);
				isHeader = false;
				continue;
			  }

			  console.log("headers are " , headers);

			// Map columns to object
			const row = {};
			headers.forEach((h, i) => {
				row[h] = cols[i]?.trim();
			});

			const {
				Security_code,
				Security_Name,
				ISIN,
				WS_client_id,
				WS_Account_code
			} = row;


			   /* ---------------- INSERT SECURITY ---------------- */
			   if (Security_code && ISIN) {
				await zcql.executeZCQLQuery(`
				  INSERT INTO Security_List (Security_Code, Security_Name, ISIN)
				  VALUES ('${Security_code}', '${Security_Name}', '${ISIN}')
				`);
			  }
		
			  /* ---------------- INSERT CLIENT ---------------- */
			  if (WS_client_id && WS_Account_code) {
				await zcql.executeZCQLQuery(`
				  INSERT INTO clientIds (WS_client_id, WS_Account_code)
				  VALUES ('${WS_client_id}', '${WS_Account_code}')
				`);
			  }
			}

		await zcql.executeZCQLQuery(`
			UPDATE Jobs SET status = 'COMPLETED'
			WHERE jobName = '${jobName}'
		  `);

		context.closeWithSuccess();
	} catch (error) {
		console.error(error);

		await zcql.executeZCQLQuery(`
			UPDATE Jobs SET status = 'FAILED'
			WHERE jobName = '${jobName}'
		  `);

		context.closeWithFailure();
	}
};