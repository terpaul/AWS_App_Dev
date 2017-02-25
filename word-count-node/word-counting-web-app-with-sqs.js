
/* --- application logic --- */

// constants specific to the application
const TOP_LIMIT = 10;
const DYNAMO_ENDPOINT = "";
const DYNAMO_TABLE = "word_count";
const DYNAMO_REGION = "us-west-2";

function findTopWords(content) {
  // split the text by sequence of non-alphanumeric characters
  const wordSequence = content.split(/[^\w]+/);

  // count each word
  const wordCounts = {}
  for (var i=0; i<wordSequence.length; i++) {
    var word = wordSequence[i];
    if (! Number.isInteger(wordCounts[word])) {
      wordCounts[word] = 0;
    }
    wordCounts[word] += 1;
  }

  // sort words by their counts in descending order
  var words = Object.keys(wordCounts);
  var wordsSortedByCount = words.sort(function(a, b) {
    return wordCounts[b] - wordCounts[a];
  });

  // determine the most frequent words
  var topWords = [];
  for (var i=0; i<TOP_LIMIT && i<wordsSortedByCount.length; i++) {
    var word = wordsSortedByCount[i];
    topWords.push({ word: word, count: wordCounts[word] });
  }

  return topWords;
}

function generateTopWordsHTMLSnippet(topWords) {
  return `
    <table border="1">
      <tr><th>word</th><th>count</th></tr>
      ${topWords.map(function(wordCount){
          return `<tr><td>${wordCount.word}</td><td>${wordCount.count}</td></tr>`;
        }).join('')}
    </table>
  `
}

function getFromDynamo(url, callback) {
    console.log("Searching DynamoDB for URL " + url + " in table " + DYNAMO_TABLE);
    var AWS = require("aws-sdk");

    AWS.config.update({
      region: DYNAMO_REGION
    });

    var docClient = new AWS.DynamoDB.DocumentClient();
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

function saveInDynamo(url, snippet) {
    var AWS = require("aws-sdk");

    AWS.config.update({
      region: DYNAMO_REGION
    });

    var docClient = new AWS.DynamoDB.DocumentClient();
    var params = {
      TableName:DYNAMO_TABLE,
      Item:{
          "url": url,
          "snippet": snippet
      }
    };

    console.log("Saving snippet '" + snippet + "' in DB for URL " + url);
    docClient.put(params, function(err, data) {
      if (err) {
          console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
      } else {
          //console.log("Added item:", JSON.stringify(data, null, 2));
      }
    });
}

function handleRequest(data, callback) {
  if (data.url.length == 0) {
    callback(generateTopWordsHTMLSnippet(findTopWords(data.content)));
  } else {
    var snippet = getFromDynamo(data.url, function(snippet) {
    console.log("Received '" + snippet + "' from DB for URL " + data.url);
    if (snippet !== null && typeof snippet != 'undefined') {
        console.log("Fetched '" + snippet + "' from DB for URL " + data.url);
        callback(snippet);
        //console.log("After the callback.");
        return;
    } else {
        console.log("No data in DB for URL " + data.url);
        // Send SQS message
        console.log("Sending SQS message...");
        var AWS = require("aws-sdk");
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

        callback("Calculating words on URL " + data.url + ".  Please check back later.");
    }
  });
}
}

// modules/dependencies we need to run a web server
const express = require('express')  
const bodyParser = require("body-parser");
const app = express()  
const PORT = 3000;

app.use(bodyParser.urlencoded({extended : true}));

app.listen(PORT, function(err) {  
  if (err) {
    return console.log('Failed to start', err)
  }

  console.log(`server is listening on ${PORT}`)
})

app.post('/findTopWords', function(request, response) {  
  handleRequest(request.body, function (output) {
    // add a CORS header, so that the request can be done from any domain
    response.header('Access-Control-Allow-Origin', '*')
    response.send(output)
  });
})

