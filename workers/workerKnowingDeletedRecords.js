const fs = require("fs");
const { parentPort } = require("worker_threads");
const {
  extractDni,
  extractCbu,
} = require("../model/accountCbuDataTransformer");

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
    0: { offset: 0, endBytesToRead: 97805749 },
    1: { offset: 97805999, endBytesToRead: 195612050 },
    2: { offset: 195612301, endBytesToRead: 293417681 },
    3: { offset: 293417932, endBytesToRead: 391223312 },
    4: { offset: 391223563, endBytesToRead: 489029614 },
    5: { offset: 489029865, endBytesToRead: 586835916 },
    6: { offset: 586836167, endBytesToRead: 684642222 },
    7: { offset: 684642469, endBytesToRead: 782445567 },
  };

  return obj[threadNumber];
};

const processNextBlock = async (
  fd,
  blockSize,
  bytesToSkipStart,
  threadNumber
) => {
  try {
    let { offset, endBytesToRead } =
      determinateLimitsBytesPerThread(threadNumber);
    let payloads = [];
    let count = 0;
    if (offset === 0) {
      offset += bytesToSkipStart;
    }

    while (offset < endBytesToRead) {
      const block = await readBlock(fd, offset, blockSize);
      const fullText = block.toString("latin1");
      if (fullText.length === 421 && fullText.endsWith("01")) {
        const dni = extractDni(fullText);
        const cbu = extractCbu(fullText);

        const payload = {
          customer_id: dni,
          customer_id_cbu: `${dni}:${cbu}`,
        };

        payloads.push(payload);

        if (payloads.length >= 25) {
          // Aquí se guarda el lote de registros en un archivo JSON
          guardarRegistrosEnJSON(payloads, count);
          count += payloads.length;
          parentPort.postMessage(count);
          payloads = [];
        }
      }

      offset += blockSize + 250;
    }

    if (payloads.length > 0) {
      // Aquí se guarda el lote final de registros en un archivo JSON
      guardarRegistrosEnJSON(payloads, count);
      count += payloads.length;
      parentPort.postMessage(count);
    }
    // Llegó al final del archivo
    fs.closeSync(fd);
    return count;
  } catch (error) {
    console.error("Error al leer el bloque:", error);
    fs.closeSync(fd);
  }
};

// Función para guardar registros en un archivo JSON
const guardarRegistrosEnJSON = (registros, count) => {
  const nombreArchivo = `registros_${count}.json`;
  fs.writeFileSync(nombreArchivo, JSON.stringify(registros, null, 2));
  console.log(`Registros guardados en ${nombreArchivo}`);
};

// Función manejadora que se ejecutará en el worker
const workerHandler = async (config) => {
  const { binaryFilePath, blockSize, bytesToSkipStart, threadNumber } =
    config;

  try {
    const payloadsCount = await processNextBlock(
      fs.openSync(binaryFilePath, "r"),
      blockSize,
      bytesToSkipStart,
      threadNumber
    );
    return payloadsCount;
  } catch (error) {
    console.log(error);
    process.send({ error: error.message });
  }
};

module.exports = workerHandler;
