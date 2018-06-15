var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});

const sqs = new AWS.SQS();
const sns = new AWS.SNS();

const sqs_url = process.env.SQS_URL;
const sns_arn = process.env.SNS_ARN;

const sql = require('mssql');

const master_db_config = {
	user: process.env.MASTER_DB_USER,
	password: process.env.MASTER_DB_PASSWORD,
	server: process.env.MASTER_DB_ADDRESS,
	database: process.env.MASTER_DB_NAME,
	port: process.env.MASTER_DB_PORT
};

const query_string =
`SELECT
	a.ModelCode AS StockItemNumber,
	a.ModelName AS ScientificName,
	a.UDTextModel1 AS Plant,
	a.UDTextModel6 AS Variety,
	b.Quantity AS QuantityOnHand,
	ordered_line_items.Units AS QuantityOnHold,
	c.LocationName AS SeedBank

FROM Models a
LEFT JOIN Items b ON a.ModelID = b.ModelID
INNER JOIN Locations c ON b.LocationIDCurrent = c.LocationID
LEFT JOIN (
	SELECT x.Units, z.ModelCode 
	FROM IssueOrderLine x
	INNER JOIN IssueOrders y
	ON x.IssueOrderID=y.IssueOrderID
	INNER JOIN Models z
	ON x.ModelID = z.ModelID
	WHERE y.SOClosed = 0 AND y.Deleted = 0
) ordered_line_items ON  ordered_line_items.ModelCode = a.ModelCode

WHERE b.ItemID in (
	SELECT ItemID
	FROM Items
	JOIN (
		SELECT ItemCode, MAX(Quantity) as Quantity
		FROM Items
		GROUP BY ItemCode
	) max_quantity_table
	ON Items.ItemCode = max_quantity_table.ItemCode AND Items.Quantity = max_quantity_table.Quantity
)

AND a.ModelsUDValueCode2 = 'NETWORK'
AND a.Deleted = 0
ORDER BY StockItemNumber`;


// Intended for Lambda, this is the function called by the service.
// Event is a json object with the payload delivered to the Lambda function.
// Context is the way to interact with the service.
exports.handler = function (event, context) {
	sql.connect(master_db_config).then(pool => {
		// Query
		console.log("Connecting To Passport Database.");
		return pool.request().query(query_string);
	}).then(result => {
		sql.close();
		console.log("Successfully Queried Database.");
		processRecordset(result.recordset, sendSqsMessage, context);
	}).catch(err => {
		sql.close();
		console.error(err);
		context.fail(err);
	});

	sql.on('error', err => {
		sql.close();
		console.error(err);
		context.fail(err);
	});
};

function processRecordset(recordset, operation, context){
	var doneCount = 0;

	function report(err, data) {
		doneCount ++;
		
		if (err) {
			console.error("Error", err);
		} else {
			console.log("Sent Message "+ doneCount +" of "+ recordset.length + ".");
		}

		if(doneCount === recordset.length) {
			message = "Sent "+ recordset.length +" Messages To Queue";
			sendSnsMessage(message, complete);
		}
	}

	function complete(err, data) {
		if (err) {
			console.error("Error", err);
			context.fail(err);
		} else {
			console.log("Sent SNS Message.");
			context.succeed(data);
		}
	}

	for(var i=0 ; i < recordset.length; i++) {
		operation(recordset[i], report);
	}
}


function sendSqsMessage(message_body, callback) {
	params = {
		DelaySeconds: 0,
		MessageBody: JSON.stringify(message_body),
		QueueUrl: sqs_url
	};
	sqs.sendMessage(params, callback);
}

function sendSnsMessage(message_body, callback) {
	params = {
		TopicArn: sns_arn,
		Message: message_body +": "+ new Date().toLocaleString()
	};
	sns.publish(params, callback);
}