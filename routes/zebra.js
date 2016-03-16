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

function processfile(req, res, type) {
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
    var values = [];

    rd.on('line', function(line) {
        if(line.lastIndexOf(",") != (line.length-1)) {
            var linesplit = line.split(",");
            if(linesplit.length >= 11) {
                if(storeidpresent == null) {
                    if(linesplit[0].toLowerCase().indexOf("store") >=0 ) {
                        storeidpresent = true;
                        auditIdIndex = 1;
                    }
                    else {
                        storeidpresent = false;
                        auditIdIndex = 0;
                    }
                } else {
                    values.push({
                        storeId : storeidpresent ? linesplit[0].toString() : "373",
                        auditId : linesplit[auditIdIndex].toString(),
                        source : linesplit[auditIdIndex+1].toString(),
                        epc : linesplit[auditIdIndex+2].toString(),
                        ts : linesplit[auditIdIndex+3].toString(),
                        location : linesplit[auditIdIndex+4].toString(),
                        gtin : linesplit[auditIdIndex+5].toString(),
                        group : linesplit[auditIdIndex+6].toString(),
                        x : linesplit[auditIdIndex+7].toString(),
                        y : linesplit[auditIdIndex+8].toString(),
                        z : linesplit[auditIdIndex+9].toString(),
                        region : linesplit[auditIdIndex+10].toString(),
                    });
                }

            }
        }
    });

    rd.on('close', function(data) {
        if(values.length > 0) {
            if(type == "SS") {
               processItems(values, ZEBRA_DATA_TABLE_NAME, function(error){
                    console.log(error);
               }); 
            }
            else if(type == "HH") {
                processItems(values, HANDHELD_DATA_TABLE_NAME, function(error){
                    console.log(error);
                });
            }
        }
    });
    return;
}

/* Upload zebra smart sense data. */
router.post('/ss/:token', upload.single('file'), function(req, res) {
    processfile(req, res, "SS");
});

/* Upload zebra hand held data. */
router.post('/hh/:token', upload.single('file'), function(req, res) {
    console.log(req.file);

    processfile(req, res, "HH");
});

module.exports = router;

// Functions
function processItems(items, tableName, callback) {
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
            callback("All fields (auditId, source, epc, ts, " + "location, gtin, group, x, y, z, storeId) are mandatory");
            return;
        }


        var upc = utils.EPC2UPC(epc);

        var parseDate = Date.parse(ts);
        if (parseDate != null || parseDate != undefined) {
            partitionKey = storeId + "##" + Date.parse(ts).toString("yyyy-MM-dd");
        }

        if (tableName == ZEBRA_DATA_TABLE_NAME && source.indexOf("smart sensing") < 0 ||
            tableName == HANDHELD_DATA_TABLE_NAME && source.indexOf("handheld") < 0) {
            callback("Invalid value for source - "+source);
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
            utils.batchWrite(item_list, true, callback, tableName);
            item_list = [];
        }

        if (item_list.length == utils.getDynamoDBBatchWriteLimit()) {
            utils.batchWrite(item_list, false, callback, tableName);
            item_list = [];
        }
    }
}