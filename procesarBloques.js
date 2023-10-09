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
const tableName = "accounts-cbus-table";
// Path to the binary file
const binaryFilePath = "mctabancaria.bin";
const blockSize = 421; // Block size in bytes
const bytesToSkipStart = 1039; // Number of bytes to skip at the beginning
const bytesToSkipAfter = 250; // Number of bytes to skip after each block
const numCores = 3; // Desired number of CPU cores
let previousBuffer = Buffer.alloc(0); // Variable to store unprocessed fragments
let threadsFinished = 0; // Counter for worker threads that have finished

const extractDni = (dniCbu) => dniCbu.slice(0, dniCbu.length === 31 ? 7 : 8);

const extractCBU = (dniCbu) => dniCbu.slice(-22);

const extractTypeId = (text) => {
  const charactersToExtract = text.slice(50, 52); // Extraer los caracteres desde la posición 50 hasta la 52 (incluyendo el 50, excluyendo el 52)
  return charactersToExtract || null; // Devolver los caracteres extraídos o null si no se encuentran suficientes caracteres.
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

const extractAlias = (text) => {
  const startIndex = 30; // Posición de inicio
  const length = 20; // Longitud a extraer

  const aliasText = text.slice(startIndex, startIndex + length).trim();

  if (aliasText !== "") {
    return aliasText;
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

const extractCreatedDate = (inputString) => {
  // Omitir los primeros 169 caracteres
  const textoSinPrimeros169Caracteres = inputString.slice(169);

  // Encontrar la primera secuencia de 8 caracteres numéricos que no sean todos ceros
  const regex = /([1-9]\d{7})/; // Buscar un número que no comience con ceros
  const match = textoSinPrimeros169Caracteres.match(regex);

  // Verificar si se encontró una secuencia numérica
  const secuenciaNumerica = match ? match[1] : null;

  return secuenciaNumerica;
};
const extractCurrency = (inputString) => {
  // Omitir los primeros 50 caracteres
  inputString = inputString.slice(72);

  // Encontrar los primeros dos caracteres que no sean espacios
  let currency = "";
  for (const char of inputString) {
    if (char !== " ") {
      currency += char;
      if (currency.length === 2) {
        break;
      }
    }
  }

  return currency;
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

const writeRecordsInBatches = async (records) => {
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
};

function writeRecordsInBatchesAsync(payloads) {
  return new Promise(async (resolve, reject) => {
    try {
      await writeRecordsInBatches(payloads);
      resolve();
    } catch (error) {
      reject(error);
    }
  });
}
if (isMainThread) {
  // Main thread
  let count = 0;
  // Create a text file to save the results

  // Create worker threads to process the blocks
  const workers = [];

  for (let i = 0; i < numCores; i++) {
    const offset = i * blockSize;
    const worker = new Worker(__filename, { workerData: { offset } });

    worker.on("message", async ({ ccount }) => {
      count += ccount;
    });

    worker.on("error", (error) => {
      console.error("Error in worker thread:", error);
    });
    worker.on("exit", async () => {
      console.log(`Worker thread finished: offset ${offset}`);
      // Check if all threads have finished
      threadsFinished++;
      if (threadsFinished === numCores) {
        console.log(`${count} registros escritos en la dynamodb`);
      }
    });

    workers.push(worker);
  }
} else {
  // Worker thread

  console.log("Procesando bloques");
  let payloads = [];
  let count = 0;
  const fd = fs.openSync(binaryFilePath, "r");
  let offset = workerData.offset;
  offset = Math.floor(offset);
  const processNextBlock = async () => {
    try {
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
        // Handle the processed block here
        const dniCbu = match[0];
        const fullText = match.input;
        const dni = extractDni(dniCbu);
        const cbu = extractCBU(dniCbu);
        const alias = extractAlias(fullText);
        const typeId = extractTypeId(fullText, dniCbu);
        const typeName = extractCharactersAfterText(fullText, typeId, dniCbu);
        const currency = extractCurrency(fullText);
        const bankId = extractNumberOfCharacters(fullText, currency, 3);
        const bankName = extractCharactersAfterText(fullText, bankId, dniCbu);
        const ownerType = extractLetters(fullText, bankName, 1);
        const cuit = findFirstNumericSequence(fullText);
        const fullName = extractAndCleanCharacters(fullText, cuit, dniCbu);
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
            alias: alias !== "" ? alias : null,
            owner: {
              type: ownerType,
              fiscal_data: {
                id: null,
                name: null,
                fiscal_key: cuit,
              },
              full_name: fullName,
            },
          },
          created_date: createdDate,
          updated_date: null,
        };
        payloads.push(payload);
        if (payloads.length === 25) {
          writeRecordsInBatchesAsync(payloads)
            .then(async () => {
              count += payloads.length;
              payloads = [];
            })
            .catch((error) => {
              console.error("Error al escribir registros en DynamoDB:", error);
            });
        }
      }

      offset += blockSize;

      if (block.length === blockSize) {
        processNextBlock();
      } else {
        // Reached the end of the file
        fs.closeSync(fd);

        // Verificar si quedan registros sin procesar
        if (payloads.length > 0) {
          // Escribir los registros restantes
          writeRecordsInBatchesAsync(payloads)
            .then(async () => {
              count += payloads.length;
              payloads = [];
              parentPort.postMessage({ ccount: count });
            })
            .catch((error) => {
              console.error("Error al escribir registros en DynamoDB:", error);
            });
        } else {
          // No hay registros para procesar, simplemente notificar al hilo principal
          parentPort.postMessage({ ccount: count });
        }
      }
    } catch (error) {
      console.error("Error reading the block:", error);
      fs.closeSync(fd);
    }
  };

  processNextBlock();
}