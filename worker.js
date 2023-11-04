/* eslint-disable no-undef */
const fs = require("fs");
const { parentPort } = require("worker_threads");
let condition = false;
let count = 0;
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

// Función manejadora que se ejecutará en el worker
const workerHandler = async (config) => {
  const {
    binaryFilePath,
    blockSize,
    bytesToSkipStart,
    bytesToSkipAfter,
    threadNumber,
    offset,
  } = config;
  const fd = fs.openSync(binaryFilePath, "r");
  let offsetOfThread = 0;
  let payloadsCount = 0;
  try {
    // Saltar los bytes iniciales
    if (offset == 0) {
      offsetOfThread += bytesToSkipStart;
    } else {
      offsetOfThread = offset;
    }
    console.log("Numero de thread", threadNumber);

    // eslint-disable-next-line no-constant-condition
    while (condition === false) {
      const block = await readBlock(fd, offsetOfThread, blockSize);
      const blockString = block.toString("latin1");
      const match = blockString.match(/^\d{30,31}/);
      if (match) {
        const dniCbu = match[0];
        console.log(dniCbu);
        payloadsCount += 1;
        parentPort.postMessage(1);
      }
      offsetOfThread += bytesToSkipAfter;
      // Enviar el bloque al proceso principal

      // Verificar si llegamos al final del archivo
      if (block.length < blockSize) {
        condition = true;
      }
    }
    return payloadsCount;
  } catch (error) {
    console.log(error);
    process.send({ error: error.message });
  } finally {
    fs.closeSync(fd);
  }
};

module.exports = workerHandler;
