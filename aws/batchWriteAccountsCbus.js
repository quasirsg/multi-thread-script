const BatchWriteCommand = require("@aws-sdk/lib-dynamodb").BatchWriteCommand;

const writeRecordsInBatches = async (records, client) => {
  try {
    const batchWriteParams = {
      RequestItems: {
        ["accounts-cbus-table"]: records.map((item) => ({
          PutRequest: {
            Item: item,
          },
        })),
      },
    };

    await client.send(new BatchWriteCommand(batchWriteParams));
    return records.length;
  } catch (error) {
    console.error("Error al escribir registros en DynamoDB:", error);
  }
};

module.exports = {
  writeRecordsInBatches,
};
