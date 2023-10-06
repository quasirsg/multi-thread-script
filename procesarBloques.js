const fs = require("fs");
const DynamoDBClient = require("@aws-sdk/client-dynamodb").DynamoDBClient;
const DynamoDBDocumentClient =
  require("@aws-sdk/lib-dynamodb").DynamoDBDocumentClient;
const BatchWriteCommand = require("@aws-sdk/lib-dynamodb").BatchWriteCommand;
const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require("worker_threads");

// aws config
const client = new DynamoDBClient({ region: "us-east-1" });
const ddbDocClient = DynamoDBDocumentClient.from(client);
const tableName = "accounts-cbus-develop-table";
// Path to the binary file
const binaryFilePath = "mctabancaria.bin";
const textFilePath = "results.txt"; // Name of the text file to save the results
const blockSize = 421; // Block size in bytes
const bytesToSkipStart = 1039; // Number of bytes to skip at the beginning
const bytesToSkipAfter = 250; // Number of bytes to skip after each block
const blockLimit = 27; // Maximum number of blocks to save
const numCores = 2; // Desired number of CPU cores

let previousBuffer = Buffer.alloc(0); // Variable to store unprocessed fragments
let savedBlocks = 0; // Variable to count saved blocks
let threadsFinished = 0; // Counter for worker threads that have finished

const extractDni = (dniCbu) => dniCbu.slice(0, dniCbu.length === 31 ? 7 : 8);

const extractCBU = (dniCbu) => dniCbu.slice(-22);

const extractTypeId = (text, dniCbu) => {
  const textAfterDniCbu = text.slice(dniCbu.length);
  const spaceIndex = textAfterDniCbu.indexOf(" "); // Find the index of the first space in the remaining text
  if (spaceIndex !== -1) {
    const textAfterSpace = textAfterDniCbu.slice(spaceIndex + 1); // Extract text after the first space
    const match = textAfterSpace.match(/\S{2}/); // Find the first pair of non-space characters
    if (match) {
      return match[0];
    }
  }
  return null;
};

const extractTextAfterTwoSpaces = (text) => {
  const spacesRegex = /\s{2,}/; // Regular expression to find 2 or more spaces
  const extractedText = text.match(spacesRegex);

  if (extractedText) {
    const start = text.indexOf(extractedText[0]) + extractedText[0].length;
    return text.slice(start);
  }

  return null;
};

// Function to extract the desired number of characters using extractTextAfterTwoSpaces
const extractNumberOfCharacters = (fullText, searchText, characterCount) => {
  const textIndex = fullText.indexOf(searchText);

  if (textIndex !== -1) {
    const textAfter = fullText.slice(textIndex + searchText.length);
    const extractedText = extractTextAfterTwoSpaces(textAfter);

    if (extractedText) {
      return extractedText.substring(0, characterCount) || "";
    }
  }

  return null;
};

const extractCharactersAfterText = (fullText, searchText, dniCbu) => {
  const omittedText = fullText.slice(dniCbu.length); // Skip the first 31 characters
  const textIndex = omittedText.indexOf(searchText);

  if (textIndex !== -1) {
    const textAfter = omittedText.slice(textIndex + searchText.length);
    const extractedCharacters = textAfter.match(/(.+?(?=\s{2,}|\s*$))/);
    if (extractedCharacters) {
      return extractedCharacters[0].trim(); // Return the extracted characters without leading and trailing spaces.
    }
  }

  return null;
};

const nonNumericCharactersUntilSpace = (text) => {
  const match = text.match(/\D\S*\s/); // Find the first non-numeric character followed by any non-space character until the first space
  if (match) {
    return match[0].trim(); // Remove leading and trailing spaces
  }
  return null;
};

const extractLetters = (fullText, searchText, characterCount) => {
  const extractedText = extractNumberOfCharacters(
    fullText,
    searchText,
    characterCount
  );

  if (extractedText) {
    // Filtrar solo las letras del texto extraído
    const lettersOnly = extractedText.replace(/[^a-zA-Z]/g, "");

    if (lettersOnly.length > 0) {
      return lettersOnly;
    }
  }

  return null;
};

const findFirstNumericSequence = (text) => {
  // Omitir los primeros 31 caracteres del texto
  const textAfterOmission = text.slice(31);

  const regex = /(\d{11}[A-Za-z])/;
  const match = textAfterOmission.match(regex);

  if (match) {
    const numbers = match[0].match(/\d{11}/);
    return numbers ? numbers[0] : null;
  }

  return null;
};

const extractAndCleanCharacters = (fullText, searchText, dniCbu) => {
  const extractedText = extractCharactersAfterText(
    fullText,
    searchText,
    dniCbu
  );

  if (extractedText) {
    // Reemplazar comas (',') y barras ('/') con espacios
    const cleanedText = extractedText.replace(/[,/]/g, " ");

    if (cleanedText.length > 0) {
      return cleanedText;
    }
  }

  return null;
};

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

const writeRecordsInBatches = async(records) =>{
  const batchSize = 25; // Tamaño del lote
  const totalRecords = records.length;
  let startIndex = 0;
  
  while (startIndex < totalRecords) {
    const endIndex = Math.min(startIndex + batchSize, totalRecords);
    const batchRecords = records.slice(startIndex, endIndex);
    
    const batchWriteParams = {
      RequestItems: {
        [tableName]: batchRecords.map((item) => ({
          PutRequest: {
            Item: item,
          },
        })),
      },
    };

    try {
      await ddbDocClient.send(new BatchWriteCommand(batchWriteParams));
      console.log(`Escritos ${batchRecords.length} registros en DynamoDB.`);
    } catch (error) {
      console.error("Error al escribir registros en DynamoDB:", error);
      // Puedes agregar lógica de manejo de errores aquí si es necesario
    }

    startIndex += batchSize;
  }
}

if (isMainThread) {
  // Main thread
  let payloads = [];
  // Create a text file to save the results
  const resultsFile = fs.createWriteStream(textFilePath);

  // Create worker threads to process the blocks
  const workers = [];

  for (let i = 0; i < numCores; i++) {
    const offset = i * blockSize * (blockLimit / numCores);
    const worker = new Worker(__filename, { workerData: { offset } });

    worker.on("message", async ({ block, match }) => {
      // Handle the processed block here
      const dniCbu = match[0];
      const fullText = match.input;
      const dni = extractDni(dniCbu);
      const cbu = extractCBU(dniCbu);
      const alias = nonNumericCharactersUntilSpace(fullText);
      const typeId = extractTypeId(fullText, dniCbu);
      const typeName = extractCharactersAfterText(fullText, typeId, dniCbu);
      const typeComplete = `${typeId}${typeName}`;
      const currency = extractNumberOfCharacters(fullText, typeComplete, 2);
      const bankId = extractNumberOfCharacters(fullText, currency, 3);
      const bankName = extractCharactersAfterText(fullText, bankId, dniCbu);
      const ownerType = extractLetters(fullText, bankName, 1);
      const cuit = findFirstNumericSequence(fullText);
      const fullName = extractAndCleanCharacters(fullText, cuit, dniCbu);
      console.log(fullName);
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
          alias: alias !== "" ? alias : null,
          owner: {
            type: ownerType,
            fiscal_data: {
              name: "CUIT",
              fiscal_key: cuit,
            },
            full_name: fullName,
          },
        },
      };
      payloads.push(payload);
      
      resultsFile.write(`Block: ${block}\n`);
    });

    worker.on("error", (error) => {
      console.error("Error in worker thread:", error);
    });
    worker.on("exit", async () => {
      console.log(`Worker thread finished: offset ${offset}`);
      // Check if all threads have finished
      threadsFinished++;
      if (threadsFinished === numCores) {
        // Agregar los payloads actuales al lote de escritura
        console.log(payloads.length);
        await writeRecordsInBatches(payloads);
      }
    });

    workers.push(worker);
  }
} else {
  // Worker thread
  const fd = fs.openSync(binaryFilePath, "r");
  let offset = workerData.offset;
  offset = Math.floor(offset);
  const processNextBlock = async () => {
    try {
      if (savedBlocks >= blockLimit) {
        // Stop reading after 500 saved blocks
        fs.closeSync(fd);
        return;
      }

      if (offset === 0) {
        // If it's the first time, skip the first 1039 bytes
        offset += bytesToSkipStart;
      } else {
        offset += bytesToSkipAfter; // Skip 250 bytes after each block
      }

      const block = await readBlock(fd, offset, blockSize);

      // Combine the current block with the previous one to search for the pattern
      const fullBuffer = Buffer.concat([previousBuffer, block]);

      // Convert the buffer to a text string
      const fullText = fullBuffer.toString("latin1");

      // Check if the block starts with a 30 to 31-character number
      const match = fullText.match(/^\d{30,31}/);
      if (match) {
        // The block meets the criteria, send it to the main thread
        parentPort.postMessage({ block: fullText, match });
      }

      savedBlocks++;

      offset += blockSize;

      if (block.length === blockSize) {
        processNextBlock();
      } else {
        // Reached the end of the file
        fs.closeSync(fd);
      }
    } catch (error) {
      console.error("Error reading the block:", error);
      fs.closeSync(fd);
    }
  };

  processNextBlock();
}
