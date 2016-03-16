var express = require('express');
var router = express.Router();
var utils = require('./utils.js');
require("datejs");
var multer  = require('multer');
var upload = multer({dest: 'uploads/'});
var fs = require('fs');
var readline = require('readline');

var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});
var PRENOTE_TABLE_NAME = "RFID-Prenote-Data";
var dynamodb = new AWS.DynamoDB({apiVersion: 'latest'});

var loggly = require('loggly');
var loggly_token = process.env.LOGGLY_TOKEN;
var loggly_sub_domain = process.env.LOGGLY_SUB_DOMAIN;

var client = loggly.createClient({
    token: loggly_token,
    subdomain: loggly_sub_domain,
    tags: ["rfid", "prenote"],
    json:true
});

/* Upload pre-note data. */
router.post('/:token', upload.single('file'), function(req, res) {
    var token = req.params.token || " ";
    if (utils.validateSecret(token) != true) {
        res.status(400).send({ "error": "Invalid request. Token mismatched" });
        return;
    }
    else if(req.file == null || req.file == undefined) {
        res.status(400).send({ "error": "Invalid request. No file found"  });
        return;
    }

    res.status(200).send();

    var rd = readline.createInterface({
        input: fs.createReadStream(req.file.path),
        output: process.stdout,
        terminal: false
    });

    var storeidpresent = null;
    var auditIdIndex = 0;
    var prenotevalues = [];

    rd.on('line', function(line) {
        if(line.lastIndexOf(",") != (line.length-1)) {
            var linesplit = line.split(",");
            if(linesplit.length == 10) {
                prenotevalues.push({
                    storeId : linesplit[0].toString(),
                    upc : linesplit[1].toString(),
                    itemNumber : linesplit[2].toString(),
                    dept : linesplit[3].toString(),
                    quantity : linesplit[4].toString(),
                    loadId : linesplit[5].toString(),
                    trailerId : linesplit[6].toString(),
                    tripId : linesplit[7].toString(),
                    shipmentId : linesplit[8].toString(),
                    ts : linesplit[9].toString(),
                });
            }
        }
    });

    rd.on('close', function(data) {
        if(prenotevalues.length > 0) {
           processItems(prenotevalues, PRENOTE_TABLE_NAME, function(error){
                console.log(error);
           }); 
        }
    });
    return;
});

module.exports = router;

// Functions
function processItems(rawItems, tableName, callback) {
    var item_list = [];

    var items = addSimilarItems(rawItems);

    for (var idx in items) {
        var storeId = items[idx].storeId;
        var upc = items[idx].upc;
        var itemNumber = items[idx].itemNumber;
        var dept = items[idx].dept;
        var quantity = items[idx].quantity;
        var loadId = items[idx].loadId;
        var trailerId = items[idx].trailerId;
        var tripId = items[idx].tripId;
        var shipmentId = items[idx].shipmentId;
        var ts = items[idx].ts;

        var partitionKey = storeId;
        var parseDate = Date.parse(ts);
        if (parseDate != null || parseDate != undefined) {
            partitionKey = storeId + "##" + Date.parse(ts).toString("yyyy-MM-dd");
        }

        var sortKey = upc;

        /*
         * Validate mandatory fields
         */
        if (storeId == null || upc == null || itemNumber == null || dept == null || quantity == null ||
            loadId == null || trailerId == null || tripId == null || shipmentId == null || ts == null) {
            callback("All fields (storeId, upc, itemNumber, dept, quantity, loadId, trailerId, tripId, shipmentId, ts) are mandatory");
            return;
        }

        /*
         * Fill in the customer details to update
         */
        var item_details = {
            storeId: { 'S': storeId },
            upc: { 'S': upc },
            itemNumber: { 'S': itemNumber },
            dept: { 'S': dept },
            quantity: { 'S': quantity },
            loadId: { 'S': loadId },
            trailerId: { 'S': trailerId },
            tripId: { 'S': tripId },
            shipmentId: { 'S': shipmentId },
            ts: { 'S': ts },
            sortKey: {'S' : sortKey},
            partitionKey: {'S' : partitionKey}
        }

        var put_request = {
            Item: item_details
        }

        var list_items = {
            PutRequest: put_request
        }

        item_list.push(list_items);
        //console.log(item_details);

        if (item_list.length == utils.getDynamoDBBatchWriteLimit()) {
            utils.batchWrite(item_list, false, callback, tableName);
            item_list = [];
        }
    }

    if (item_list.length > 0) {
        utils.batchWrite(item_list, true, callback, tableName);
    } else {
        return;
    }
}

function addSimilarItems(items) {
    var upcDict = {}
    for (var idx in items) {
        if (upcDict[items[idx].upc] != null && upcDict[items[idx].upc] != undefined) {
            upcDict[items[idx].upc]['quantity'] =
                (parseInt(upcDict[items[idx].upc]['quantity']) +
                    parseInt(items[idx].quantity)).toString();
        } else {
            upcDict[items[idx].upc] = items[idx];
        }
    }
    return upcDict;
}