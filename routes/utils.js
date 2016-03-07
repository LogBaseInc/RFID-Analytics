var express = require('express');
var router = express.Router();
var secret_key = process.env.SECRET_KEY;

var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});
var dynamodb = new AWS.DynamoDB({apiVersion: 'latest'});


var DYNAMODB_BATCH_WRITE_LIMIT = 20;

module.exports = {
    batchWrite: function (product_list, complete, res, table_name) {
        var params = {};
        params['RequestItems'] = {};
        params.RequestItems[table_name] = product_list;

        dynamodb.batchWriteItem(params, function (err, data) {
            if (err) {
                console.log(err);
                res.status(400).send({ "error": err.message });
                return;
            } else {
                if (complete) {
                    res.status(200).send();
                    return;
                }
            }
        });
    },

    validateSecret: function (secret) {
        if (secret == null || secret == undefined) {
            return false;
        }

        if (secret == secret_key) {
            return true;
        } else {
            return false;
        }
    },

    getDynamoDBBatchWriteLimit: function() {
        return DYNAMODB_BATCH_WRITE_LIMIT;
    }
}