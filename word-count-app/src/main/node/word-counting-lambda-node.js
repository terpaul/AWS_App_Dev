'use strict';

console.log('Loading function');

const DYNAMO_ENDPOINT = "";
const DYNAMO_TABLE = "word_count";
const DYNAMO_REGION = "us-west-2";

const AWS = require("aws-sdk");
AWS.config.update({
  region: DYNAMO_REGION
});

const docClient = new AWS.DynamoDB.DocumentClient();

function getFromDynamo(url, callback) {
    console.log("Searching DynamoDB for URL " + url + " in table " + DYNAMO_TABLE);

    var params = {
        TableName : DYNAMO_TABLE,
        KeyConditionExpression: "#url = :url",
        ExpressionAttributeNames:{
            "#url": "url"
        },
        ExpressionAttributeValues: {
            ":url":url
        }
    };

    docClient.query(params, function(err, data) {
        if (err) {
            console.error("Error searching DB for URL ", url, ":", JSON.stringify(err, null, 2));
        } else {
            console.log("Received '" + JSON.stringify(data, null, 2) + "' from DB for URL " + data.url);
            if (data.Count > 0) {
            data.Items.forEach(function(item) {
                //console.log("Fetched '", item.snippet, "' from DynamoDB for URL " + url);
                callback(item.snippet);
            });
            } else {
                console.log("Zero items in DB for URL " + url + ", returning null");
                callback(null);
            }
        }
    });
}

exports.handler = (data, context, callback) => {
    console.log('Received event:', JSON.stringify(data, null, 2));
    if (data.url.includes("google")) {
        console.log("[UNEXPECTED_INPUT] cannot process google");
        callback(null, "ERROR: Cannot process google");
        return;
    }
    var snippet = getFromDynamo(data.url, function(snippet) {
    console.log("Received '" + snippet + "' from DB for URL " + data.url);
    if (snippet !== null && typeof snippet != 'undefined') {
        console.log("Fetched '" + snippet + "' from DB for URL " + data.url);
        callback(null, snippet);
        return;
    } else {
        console.log("No data in DB for URL " + data.url);
        // Send SQS message
        console.log("Sending SQS message...");
        var sqs = new AWS.SQS();

        var params = {
         DelaySeconds: 10,
         MessageAttributes: {
          "message_type": {
            DataType: "String",
            StringValue: "word_count"
           }
         },
         MessageBody: data.url,
         QueueUrl: "https://sqs.us-west-2.amazonaws.com/714389097446/WordCountJobs"
        };

        sqs.sendMessage(params, function(err, data) {
          if (err) {
            console.log("Error sending SQS message ", err);
          } else {
            console.log("Successfully sent message for word count job", data.MessageId);
          }
        });

        callback(null, "Calculating words on URL " + data.url + ".  Please check back later.");
    }
  });
};
