# passport-inventory-sync-enqueue
Pulls current inventory from Passport (a SQL Database) and creates an SQS queue entry for each item. When all items are enqueued a SNS message is sent.

This program is intended to run on AWS Lambda.

Deployment is handled using Claudia.JS (https://claudiajs.com)