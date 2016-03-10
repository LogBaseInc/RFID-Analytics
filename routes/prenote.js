var express = require('express');
var router = express.Router();
var utils = require('./utils.js');
require("datejs");

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
router.post('/:token', function(req, res) {
    var token = req.params.token || " ";
    var items = req.body;

    if (utils.validateSecret(token) != true) {
        res.status(400).send({ "error": "Invalid request. Token mismatched" });
        return;
    }

    if (items.length == 0) {
        res.status(400).send({ "error": "Invalid request. No entries provided" });
        return;
    }

    processItems(items, PRENOTE_TABLE_NAME, res);
});

module.exports = router;

// Functions
function processItems(rawItems, tableName, res) {
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
            res.status(400).send({ "error" : "All fields (storeId, upc, itemNumber, dept, quantity, loadId, " +
                "trailerId, tripId, shipmentId, ts) are mandatory"});
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
            utils.batchWrite(item_list, false, res, tableName);
            item_list = [];
        }
    }

    if (item_list.length > 0) {
        console.log(item_list)
        utils.batchWrite(item_list, true, res, tableName);
    } else {
        res.status(200).send();
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