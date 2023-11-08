/* eslint-disable no-undef */
const fs = require("fs");
const { parentPort } = require("worker_threads");
const {
  extractDni,
  extractCbu,
  extractAlias,
  extractTypeId,
  extractTypeName,
  extractCurrency,
  extractNumberOfCharacters,
  extractCharactersAfterText,
  extractLetters,
  findFirstNumericSequence,
  extractAndCleanCharacters,
  extractCreatedDate,
} = require("./model/accountCbuDataTransformer");
const { writeRecordsInBatches } = require("./aws/batchWriteAccountsCbus");
const DynamoDBClient = require("@aws-sdk/client-dynamodb").DynamoDBClient;
const DynamoDBDocumentClient =
  require("@aws-sdk/lib-dynamodb").DynamoDBDocumentClient;

const readBlock = (fd, offset, size) => {
  return new Promise((resolve, reject) => {
    const buffer = Buffer.alloc(size);
    fs.read(fd, buffer, 0, size, offset, (error, bytesRead, buffer) => {
      if (error) {
        reject(error);
      } else {
        resolve(buffer.slice(0, bytesRead));
      }
    });
  });
};

const determinateLimitsBytesPerThread = (threadNumber) => {
  const obj = {
    0: { offset: 0, endBytesToRead: 195611379 },
    1: { offset: 195611630, endBytesToRead: 391222642 },
    2: { offset: 391222892, endBytesToRead: 586834575 },
    3: { offset: 586834825, endBytesToRead: 782445166 },
  };

  return obj[threadNumber];
};

const processNextBlock = async (
  fd,
  blockSize,
  bytesToSkipStart,
  threadNumber,
  ddbDocClient
) => {
  try {
    let { offset, endBytesToRead } =
      determinateLimitsBytesPerThread(threadNumber);
    let payloads = [];
    let count;
    if (offset === 0) {
      offset += bytesToSkipStart;
    }

    while (offset < endBytesToRead) {
      const block = await readBlock(fd, offset, blockSize);
      const fullText = block.toString("latin1");
      if (fullText.length === 421 && fullText.endsWith("01")) {
        const dni = extractDni(fullText);
        const cbu = extractCbu(fullText);
        const alias = extractAlias(fullText);
        const typeId = extractTypeId(fullText);
        const typeName = extractTypeName(fullText);
        const currency = extractCurrency(fullText, typeName);
        const bankId = extractNumberOfCharacters(fullText, currency, 3);
        const bankName = extractCharactersAfterText(fullText, bankId);
        const ownerType = extractLetters(fullText, bankName, 1);
        const cuit = findFirstNumericSequence(fullText);
        const fullName = extractAndCleanCharacters(fullText, cuit);
        const createdDate = extractCreatedDate(fullText);

        const payload = {
          customer_id: dni,
          customer_id_cbu: `${dni}:${cbu}`,
          account_bank: {
            type: {
              id: typeId,
              name: typeName,
            },
            currency: currency[0],
            bank: {
              id: bankId,
              name: bankName,
            },
            cbu: cbu,
            alias: alias !== "" ? alias : "",
            owner: {
              type: ownerType,
              fiscal_data: {
                id: null,
                name: "",
                fiscal_key: cuit,
              },
              full_name: fullName,
            },
          },
          created_date: createdDate,
          updated_date: null,
        };
        payloads.push(payload);
        if (payloads.length >= 25) {
          const recordsWrited = await writeRecordsInBatches(
            payloads,
            ddbDocClient
          );
          count += recordsWrited;
          parentPort.postMessage(recordsWrited);
          payloads = [];
        }
      }

      offset += blockSize + 250;
    }

    if (payloads.length > 0) {
      const recordsWrited = await writeRecordsInBatches(payloads, ddbDocClient);
      count += recordsWrited;
      parentPort.postMessage(recordsWrited);
      payloads = [];
    }
    // Reached the end of the file
    fs.closeSync(fd);
    return count;
  } catch (error) {
    console.error("Error reading the block:", error);
    fs.closeSync(fd);
  }
};

// Función manejadora que se ejecutará en el worker
const workerHandler = async (config) => {
  const { binaryFilePath, blockSize, bytesToSkipStart, threadNumber, option } =
    config;
  const client = new DynamoDBClient(option);
  const ddbDocClient = DynamoDBDocumentClient.from(client);

  const fd = fs.openSync(binaryFilePath, "r");
  try {
    const payloadsCount = await processNextBlock(
      fd,
      blockSize,
      bytesToSkipStart,
      threadNumber,
      ddbDocClient
    );
    return payloadsCount;
  } catch (error) {
    console.log(error);
    process.send({ error: error.message });
  }
};

module.exports = workerHandler;
