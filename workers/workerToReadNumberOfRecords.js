/* eslint-disable no-undef */
const fs = require("fs");
const { parentPort } = require("worker_threads");

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
    7: { offset: 684642469, endBytesToRead: 783135860 },
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
    let count = 0;
    if (offset === 0) {
      offset += bytesToSkipStart;
    }

    while (offset < endBytesToRead) {
      const block = await readBlock(fd, offset, blockSize);
      const fullText = block.toString("latin1");
      if (fullText.length === 421 && fullText.endsWith("01")) {
        parentPort.postMessage(1);
        count++;
      }

      offset += blockSize + 250;
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
  const { binaryFilePath, blockSize, bytesToSkipStart, threadNumber } =
    config;

  const fd = fs.openSync(binaryFilePath, "r");
  try {
    const payloadsCount = await processNextBlock(
      fd,
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
