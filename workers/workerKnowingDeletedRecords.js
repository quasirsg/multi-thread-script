const fs = require("fs").promises;
const { parentPort } = require("worker_threads");
const { extractDni, extractCbu } = require("../model/accountCbuDataTransformer");

const readBlock = async (fileHandle, offset, size) => {
  const buffer = Buffer.alloc(size);
  const { bytesRead } = await fileHandle.read(buffer, 0, size, offset);
  return buffer.slice(0, bytesRead);
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
  fileHandle,
  blockSize,
  bytesToSkipStart,
  threadNumber
) => {
  try {
    let { offset, endBytesToRead } = determinateLimitsBytesPerThread(
      threadNumber
    );
    let count = 0;

    if (offset === 0) {
      offset += bytesToSkipStart;
    }

    while (offset < endBytesToRead) {
      const block = await readBlock(fileHandle, offset, blockSize);
      const fullText = block.toString("latin1");
      if (fullText.length === 421 && fullText.endsWith("01")) {
        const dni = extractDni(fullText);
        const cbu = extractCbu(fullText);

        const payload = {
          customer_id: dni,
          customer_id_cbu: `${dni}:${cbu}`,
        };

        count++;
        parentPort.postMessage(count);
        await guardarRegistroEnJSON(payload);
      }

      offset += blockSize + 250;
    }

    // Llegó al final del archivo
    await fileHandle.close();

    return count;
  } catch (error) {
    console.error("Error al leer el bloque:", error);
    // Si hay un error, cierra el archivo de todas formas
    await fileHandle.close().catch(() => {});
  }
};

// Función para guardar un solo registro en un archivo JSON en la carpeta "data"
let totalRegistrosGuardados = 0;

const guardarRegistroEnJSON = async (registro) => {
  const nombreArchivo = "data/registros_totales.json";
  try {
    // Escribir el archivo JSON de manera asíncrona, añadiendo al final del archivo
    await fs.writeFile(
      nombreArchivo,
      JSON.stringify(registro, null, 2) + ',\n',  // Agregar un salto de línea
      { flag: "a" }
    );

    totalRegistrosGuardados++; // Incrementar el contador de registros guardados
    console.log(`Registro ${totalRegistrosGuardados} guardado en ${nombreArchivo}`);
  } catch (error) {
    console.error("Error al guardar registro en JSON:", error);
  }
};

// Función manejadora que se ejecutará en el worker
const workerHandler = async (config) => {
  const { binaryFilePath, blockSize, bytesToSkipStart, threadNumber } = config;

  try {
    const fileHandle = await fs.open(binaryFilePath, "r");
    const payloadsCount = await processNextBlock(
      fileHandle,
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
