'use strict';

console.log("Instantiating lambda...");

const AWS = require('aws-sdk');

const Lambda = new AWS.Lambda();

const functionName = 'WordCountMessageConsumer';

function invokePoller() {

    console.log(`invokePoller: ${new Date()}`);

    const payload = {};

    const params = {

        FunctionName: functionName,

        InvocationType: 'Event',

        Payload: new Buffer(JSON.stringify(payload)),

    };

    console.log("Promising lambda " + functionName);
    return new Promise((resolve, reject) => {

        console.log("Invoking lambda " + functionName);
        Lambda.invoke(params, (err) => (err ? reject(err) : resolve()));
        console.log("Lambda " + functionName + " invoked.");
    });

}

 

exports.handler = (event, context, callback) => {
    console.log("Handler starting...");
    var promises = [];

    var times = function (n, sleep) {
        console.log("function 'times' starting with n = " + n);
        if (n <= 0) {

            console.log(`we reached the end n = ${n}`);

            Promise.all(promises).then(function(){

               callback(null, 'success');

            });

            return;

        }

        console.log("Calling timeout function with n=" + n + " and sleep " + sleep);
        setTimeout(function(){

           console.log(`progress: n = ${n}`);

           promises.push(invokePoller());

           times(n-1, sleep);

        }, sleep);

    }
    console.log(`Starting timer...`);
    times(60, 1000);

};