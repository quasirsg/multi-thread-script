const fs = require('fs');

// Ruta del archivo binario
const rutaArchivoBinario = 'mctabancaria.bin';
const rutaArchivoTexto = 'bloques.txt'; // Nombre del archivo de texto
const tamanoBloque = 421; // Tamaño del bloque en bytes
const bytesParaOmitirInicio = 1039; // Cantidad de bytes a omitir al principio
const bytesParaOmitirDespues = 250; // Cantidad de bytes a omitir después de cada bloque
const limiteDeBloques = 5000; // Número máximo de bloques a guardar

let bufferPrevio = Buffer.alloc(0); // Variable para almacenar fragmentos no procesados
let bloquesGuardados = 0; // Variable para contar los bloques guardados

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

const guardarBloqueEnArchivo = (bloque) => {
  if (bloque.trim().length > 0) {
    fs.appendFileSync(rutaArchivoTexto, `Bloque: ${bloque}\n`);
  }
};

fs.open(rutaArchivoBinario, 'r', (error, fd) => {
  if (error) {
    console.error('Error al abrir el archivo:', error);
    return;
  }

  let offset = 0;

  const procesarSiguienteBloque = async () => {
    try {
      if (bloquesGuardados >= limiteDeBloques) {
        // Detener la lectura después de 500 bloques guardados
        fs.close(fd, (error) => {
          if (error) {
            console.error('Error al cerrar el archivo:', error);
          }
        });
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

      // Aquí puedes realizar la búsqueda y procesamiento de tus bloques según tus necesidades
      // Por ejemplo, encontrar patrones de 30 o 31 caracteres seguidos de texto y guardarlos

      guardarBloqueEnArchivo(textoCompleto);
      bloquesGuardados++;

      offset += tamanoBloque;

      if (bloque.length === tamanoBloque) {
        procesarSiguienteBloque();
      } else {
        // Se ha llegado al final del archivo
        fs.close(fd, (error) => {
          if (error) {
            console.error('Error al cerrar el archivo:', error);
          }
        });
      }
    } catch (error) {
      console.error('Error al leer el bloque:', error);
      fs.close(fd, (closeError) => {
        if (closeError) {
          console.error('Error al cerrar el archivo:', closeError);
        }
      });
    }
  };

  // Inicializar el archivo de texto o borrar su contenido si ya existe
  fs.writeFileSync(rutaArchivoTexto, '');

  procesarSiguienteBloque();
});
