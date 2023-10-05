const fs = require('fs');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

// Ruta del archivo binario
const rutaArchivoBinario = 'mctabancaria.bin';
const rutaArchivoTexto = 'resultados.txt'; // Nombre del archivo de texto para guardar los resultados
const tamanoBloque = 421; // Tamaño del bloque en bytes
const bytesParaOmitirInicio = 1039; // Cantidad de bytes a omitir al principio
const bytesParaOmitirDespues = 250; // Cantidad de bytes a omitir después de cada bloque
const limiteDeBloques = 1743610; // Número máximo de bloques a guardar
const numNucleos = 3; // Número de núcleos deseados

let bufferPrevio = Buffer.alloc(0); // Variable para almacenar fragmentos no procesados
let bloquesGuardados = 0; // Variable para contar los bloques guardados
let hilosTerminados = 0; // Contador de hilos de trabajadores que han terminado

const leerBloque = (fd, offset, tamano) => {
  return new Promise((resolve, reject) => {
    const buffer = Buffer.alloc(tamano);

    fs.read(fd, buffer, 0, tamano, offset, (error, bytesRead, buffer) => {
      if (error) {
        reject(error);
      } else {
        resolve(buffer.slice(0, bytesRead));
      }
    });
  });
};

if (isMainThread) {
  // Hilo principal

  // Crear archivo de texto para guardar los resultados
  const resultadosFile = fs.createWriteStream(rutaArchivoTexto);

  // Crear hilos de trabajadores para procesar los bloques
  const workers = [];

  for (let i = 0; i < numNucleos; i++) {
    const offset = i * tamanoBloque * (limiteDeBloques / numNucleos);
    const worker = new Worker(__filename, { workerData: { offset } });

    worker.on('message', (message) => {
      // Manejar el bloque procesado aquí
      resultadosFile.write(`Bloque: ${message}\n`);
    });

    worker.on('error', (error) => {
      console.error('Error en el hilo de trabajador:', error);
    });

    worker.on('exit', () => {
      console.log(`Hilo de trabajador terminado: offset ${offset}`);
      
      // Verificar si todos los hilos han terminado
      hilosTerminados++;
      if (hilosTerminados === numNucleos) {
        resultadosFile.end(); // Cerrar el archivo de texto después de que todos los hilos hayan terminado
        console.log('Lectura de bloques completa. Resultados guardados en', rutaArchivoTexto);
      }
    });

    workers.push(worker);
  }
} else {
  // Hilo de trabajador
  const fd = fs.openSync(rutaArchivoBinario, 'r');
  let offset = workerData.offset;

  const procesarSiguienteBloque = async () => {
    try {
      if (bloquesGuardados >= limiteDeBloques) {
        // Detener la lectura después de 500 bloques guardados
        fs.closeSync(fd);
        return;
      }

      if (offset === 0) {
        // Si es la primera vez, omitir los primeros 1039 bytes
        offset += bytesParaOmitirInicio;
      } else {
        offset += bytesParaOmitirDespues; // Omitir 250 bytes después de cada bloque
      }

      const bloque = await leerBloque(fd, offset, tamanoBloque);

      // Combinar el bloque actual con el previo para buscar el patrón
      const bufferCompleto = Buffer.concat([bufferPrevio, bloque]);

      // Convertir el buffer en una cadena de texto
      const textoCompleto = bufferCompleto.toString('latin1');

      // Verificar si el bloque comienza con un número de 30 a 31 caracteres
      const match = textoCompleto.match(/^\d{30,31}/);
      if (match) {
        // El bloque cumple con el filtro, enviarlo al hilo principal
        parentPort.postMessage(textoCompleto);
      }

      bloquesGuardados++;

      offset += tamanoBloque;

      if (bloque.length === tamanoBloque) {
        procesarSiguienteBloque();
      } else {
        // Se ha llegado al final del archivo
        fs.closeSync(fd);
      }
    } catch (error) {
      console.error('Error al leer el bloque:', error);
      fs.closeSync(fd);
    }
  };

  procesarSiguienteBloque();
}