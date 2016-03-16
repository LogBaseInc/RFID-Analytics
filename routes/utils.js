var express = require('express');
var router = express.Router();

var math = require('./math.js');

var secret_key = process.env.SECRET_KEY;
var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});
var dynamodb = new AWS.DynamoDB({apiVersion: 'latest'});


var DYNAMODB_BATCH_WRITE_LIMIT = 20;

module.exports = {
    batchWrite: function (product_list, complete, callback, table_name) {
        var params = {};
        params['RequestItems'] = {};
        params.RequestItems[table_name] = product_list;

        dynamodb.batchWriteItem(params, function (err, data) {
            if (err) {
                console.log(err);
                callback(err.message);
                return;
            } else {
                if (complete) {
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
    },

    EPC2UPC: function(EPCvalue){
        var headerValue = "";
        var filterValue = "";
        var partitionValue = "";
        var companyLength = [40, 37, 34, 30, 27, 24, 20, 20];
        var companyPrefix = "";
        var itemRef = "";
        var serialNumber = "";
        var EPCbinary = "";
        var oddSum = 0;
        var evenSum = 0;
        var UPCvalue = "";
        var checkDigit = 0;

        for(var idx=0; idx < EPCvalue.length; idx++) {
            EPCbinary = EPCbinary + math.pad(math.Hex2Bin(EPCvalue[idx]), 4);
        }

        //Split Binary EPC into Sections;
        headerValue = EPCbinary.substr(0, 8);
        filterValue = EPCbinary.substr(8, 3);
        partitionValue = EPCbinary.substr(11, 3);
        companyPrefix = EPCbinary.substr(14, companyLength[math.Bin2Dec(partitionValue)]);
        itemRef = EPCbinary.substr(14 + companyPrefix.length, 44 - companyPrefix.length);
        UPCvalue = EPCbinary.substr(15, 44);
        serialNumber = EPCbinary.substr(59, 38);

        //Convert Company Prefix && Item Reference into Decimal and Add Leading Zeros;
        companyPrefix = math.Bin2Dec(companyPrefix);
        companyPrefix = math.pad(companyPrefix, 6);

        itemRef = math.Bin2Dec(itemRef);
        itemRef = math.pad(itemRef, 5);

        UPCvalue = companyPrefix + itemRef;

        //Add UPC check digit;
        for(var idx=0; idx < UPCvalue.length; idx++){
            if(idx % 2 == 0){
                evenSum = evenSum + parseInt(UPCvalue.substr(idx, 1));
            }else{
                oddSum = oddSum + parseInt(UPCvalue.substr(idx, 1));
            }
        }

        checkDigit = 10 - (((3 * evenSum) + oddSum) % 10);

        if(checkDigit > 9 ){
            checkDigit = checkDigit - 10;
        }

        UPCvalue = UPCvalue + checkDigit;
        return UPCvalue;
    }
};