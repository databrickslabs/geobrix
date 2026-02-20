package com.databricks.labs.gbx.ds.whitelist

import org.apache.spark.sql.connector.read.InputPartition

/** Single empty partition for WhitelistBatch; carries no data, used to run one reader task. */
class WhitelistPartition extends InputPartition with Serializable

