var express = require('express');
var router = express.Router();
var utils = require('./utils.js');
require("datejs");

var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});
var ZEBRA_DATA_TABLE_NAME = "RFID-Smart-Sense";
var HANDHELD_DATA_TABLE_NAME = "RFID-Hand-Held"
var dynamodb = new AWS.DynamoDB({apiVersion: 'latest'});

var loggly = require('loggly');
var loggly_token = process.env.LOGGLY_TOKEN;
var loggly_sub_domain = process.env.LOGGLY_SUB_DOMAIN;

var client = loggly.createClient({
    token: loggly_token,
    subdomain: loggly_sub_domain,
    tags: ["rfid", "zebra"],
    json:true
});

/* Upload zebra smart sense data. */
router.post('/ss/:token', function(req, res) {
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

    processItems(items, ZEBRA_DATA_TABLE_NAME, res);
});

/* Upload zebra hand held data. */
router.post('/hh/:token', function(req, res) {
    var token = req.params.token || " ";
    var items = req.body;

    console.log(req.body);
    if (utils.validateSecret(token) != true) {
        res.status(400).send({ "error": "Invalid request. Token mismatched" });
        return;
    }

    if (items.length == 0) {
        res.status(400).send({ "error": "Invalid request. No entries provided" });
        return;
    }

    processItems(items, HANDHELD_DATA_TABLE_NAME, res);
});

module.exports = router;

// Functions
function processItems(items, tableName, res) {
    var item_list = [];

    for (var idx in items) {
        var auditId = items[idx].auditId;
        var source = items[idx].source;
        var epc = items[idx].epc;
        var ts = items[idx].ts;
        var location = items[idx].location;
        var gtin = items[idx].gtin;
        var group = items[idx].group;
        var x = items[idx].x;
        var y = items[idx].y;
        var z = items[idx].z;
        var region = items[idx].region;
        var storeId = items[idx].storeId;
        var partitionKey = storeId;

        /*
         * Validate mandatory fields
         */
        if (auditId == null || source == null || epc == null || ts == null || location == null ||
            gtin == null || group == null || x == null || y == null || z == null ||
            storeId == null || region == null) {
            res.status(400).send({ "error" : "All fields (auditId, source, epc, ts, " +
                "location, gtin, group, x, y, z, storeId) are mandatory"});
            return;
        }


        var upc = utils.EPC2UPC(epc);

        var parseDate = Date.parse(ts);
        if (parseDate != null || parseDate != undefined) {
            partitionKey = storeId + "##" + Date.parse(ts).toString("yyyy-MM-dd");
        }

        if (tableName == ZEBRA_DATA_TABLE_NAME && source.indexOf("smart sensing") < 0 ||
            tableName == HANDHELD_DATA_TABLE_NAME && source.indexOf("handheld") <=0) {
            res.status(400).send({ "error" : "Invalid value for source - " + source})
            return;
        }

        /*
         * Fill in the customer details to update
         */
        var item_details = {
            auditId: { 'S': auditId },
            source: { 'S': source },
            epc: { 'S': epc },
            ts: { 'S': ts },
            location: { 'S': location },
            gtin: { 'S': gtin },
            group: { 'S': group },
            x: { 'S': x },
            y: { 'S': y },
            z: { 'S': z },
            storeId: { 'S': storeId },
            region: {'S' : region},
            upc: {'S' : upc},
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

        if (idx == items.length - 1) {
            utils.batchWrite(item_list, true, res, tableName);
            item_list = [];
        }

        if (item_list.length == utils.getDynamoDBBatchWriteLimit()) {
            utils.batchWrite(item_list, false, res, tableName);
            item_list = [];
        }
    }
}