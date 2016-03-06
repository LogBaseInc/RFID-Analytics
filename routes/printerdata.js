var express = require('express');
var router = express.Router();

var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});
var PRINTER_DATA_TABLE_NAME = "PrinterData";
var dynamodb = new AWS.DynamoDB({apiVersion: 'latest'});

var loggly = require('loggly');
var loggly_token = process.env.LOGGLY_TOKEN;
var loggly_sub_domain = process.env.LOGGLY_SUB_DOMAIN;
var secret_key = process.env.SECRET_KEY;

var client = loggly.createClient({
    token: loggly_token,
    subdomain: loggly_sub_domain,
    tags: ["rfid", "printerData"],
    json:true
});

/* Upload printer data. */
router.post('/:token', function(req, res) {
    var token = req.params.token || " ";
    var data = req.body;
    var item_list = [];

    if (token != secret_key) {
        res.status(400).send({ "error" : "Invalid request. Token Mismatched" })
    }

    if (data.length == 0) {
        res.status(400).send({ "error" : "Invalid request. No products provided" })
        return;
    }

    var storeId = data.storeId;
    var upc = data.upc;
    var epc = data.epc;
    var tid = data.tid;
    var ts = data.ts;
    var printerId = data.printerId;
    var userId = data.userId;

    /*
     * Validate mandatory fields
     */
    if (storeId == null || upc == null || epc == null || tid == null ||
        ts == null || printerId == null || user == null) {
        res.status(400).send({ "error" : "All fields (storeId, upc, epc, tid, ts, userId & printerId) are mandatory" });
        return;
    }

    /*
     * Fill in the printer data to update
     */
    var item_details = {
        storeId: { 'N': storeId },
        epc: { 'S': epc },
        upc: { 'S': upc },
        tid: { 'S': tid },
        ts: { 'S': ts},
        printerId: {'S': printerId},
        userId: {'S': userId}
    }

    var put_request = {
        Item: item_details
    }

    var list_items = {
        PutRequest: put_request
    }

    item_list.push(list_items);
    batchWrite(item_list, true, res);
});

module.exports = router;


// Functions
function batchWrite(product_list, complete, res) {
    var params = {};
    params['RequestItems'] = {};
    params.RequestItems[PRINTER_DATA_TABLE_NAME] = product_list;

    dynamodb.batchWriteItem(params, function(err, data) {
        if (err) {
            console.log(err);
            res.status(400).send({ "error" : err.message });
            return;
        } else {
            if (complete) {
                res.status(200).send();
                return;
            }
        }
    });
}
