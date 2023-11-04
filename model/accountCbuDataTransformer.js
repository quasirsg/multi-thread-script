/**
 * Extrae el DNI de un texto que contiene DNI y CBU.
 * @param {string} dniCbu - Texto que contiene el DNI y el CBU.
 * @returns {string} - El DNI extraído.
 */
const extractDni = (dniCbu) => dniCbu.slice(0, dniCbu.length === 31 ? 7 : 8);

/**
 * Extrae el CBU de un texto que contiene DNI y CBU.
 * @param {string} dniCbu - Texto que contiene el DNI y el CBU.
 * @returns {string} - El CBU extraído.
 */
const extractCBU = (dniCbu) => dniCbu.slice(-22);

/**
 * Extrae caracteres desde una posición específica en un texto.
 * @param {string} text - El texto del cual extraer caracteres.
 * @returns {string|null} - Los caracteres extraídos o null si no se encuentran suficientes caracteres.
 */
const extractTypeId = (text) => {
  const charactersToExtract = text.slice(50, 52);
  return charactersToExtract || null;
};

/**
 * Extrae el texto después de encontrar dos o más espacios.
 * @param {string} text - El texto del cual extraer.
 * @returns {string|null} - El texto extraído o null si no se encuentran suficientes caracteres.
 */
const extractTextAfterTwoSpaces = (text) => {
  const spacesRegex = /\s{2,}/;
  const extractedText = text.match(spacesRegex);

  if (extractedText) {
    const start = text.indexOf(extractedText[0]) + extractedText[0].length;
    return text.slice(start);
  }

  return null;
};

/**
 * Extrae una cantidad específica de caracteres después de una cadena de búsqueda.
 * @param {string} fullText - El texto completo.
 * @param {string} searchText - La cadena de búsqueda.
 * @param {number} characterCount - La cantidad de caracteres a extraer.
 * @returns {string|null} - Los caracteres extraídos o null si no se encuentran suficientes caracteres.
 */
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

/**
 * Extrae caracteres después de una cadena de búsqueda en un texto, omitiendo los primeros caracteres.
 * @param {string} fullText - El texto completo.
 * @param {string} searchText - La cadena de búsqueda.
 * @param {string} dniCbu - DNI o CBU.
 * @returns {string|null} - Los caracteres extraídos o null si no se encuentran suficientes caracteres.
 */
const extractCharactersAfterText = (fullText, searchText, dniCbu) => {
  const omittedText = fullText.slice(dniCbu.length);
  const textIndex = omittedText.indexOf(searchText);

  if (textIndex !== -1) {
    const textAfter = omittedText.slice(textIndex + searchText.length);
    const extractedCharacters = textAfter.match(/(.+?(?=\s{2,}|\s*$))/);
    if (extractedCharacters) {
      return extractedCharacters[0].trim();
    }
  }

  return null;
};

/**
 * Extrae un alias de un texto.
 * @param {string} text - El texto del cual extraer el alias.
 * @returns {string} - El alias extraído.
 */
const extractAlias = (text) => {
  const startIndex = 30;
  const length = 20;
  const aliasText = text.slice(startIndex, startIndex + length).trim();

  if (aliasText !== "") {
    return aliasText;
  }
  return "";
};

/**
 * Extrae letras de un texto después de una cadena de búsqueda.
 * @param {string} fullText - El texto completo.
 * @param {string} searchText - La cadena de búsqueda.
 * @param {number} characterCount - La cantidad de caracteres a extraer.
 * @returns {string} - Las letras extraídas.
 */
const extractLetters = (fullText, searchText, characterCount) => {
  const extractedText = extractNumberOfCharacters(
    fullText,
    searchText,
    characterCount
  );

  if (extractedText) {
    const lettersOnly = extractedText.replace(/[^a-zA-Z]/g, "");

    if (lettersOnly.length > 0) {
      return lettersOnly;
    }
  }

  return "";
};

/**
 * Encuentra la primera secuencia numérica en un texto.
 * @param {string} text - El texto en el que buscar la secuencia numérica.
 * @returns {string|null} - La primera secuencia numérica encontrada o null si no se encuentra.
 */
const findFirstNumericSequence = (text) => {
  const textAfterOmission = text.slice(31);
  const regex = /(\d{11}[A-Za-z])/;
  const match = textAfterOmission.match(regex);

  if (match) {
    const numbers = match[0].match(/\d{11}/);
    return numbers ? numbers[0] : null;
  }

  return null;
};

/**
 * Extrae y limpia caracteres de un texto después de una cadena de búsqueda en el DNI o CBU.
 * @param {string} fullText - El texto completo.
 * @param {string} searchText - La cadena de búsqueda.
 * @param {string} dniCbu - DNI o CBU.
 * @returns {string|null} - Los caracteres extraídos y limpiados o null si no se encuentran suficientes caracteres.
 */
const extractAndCleanCharacters = (fullText, searchText, dniCbu) => {
  const extractedText = extractCharactersAfterText(
    fullText,
    searchText,
    dniCbu
  );

  if (extractedText) {
    const cleanedText = extractedText.replace(/[,/]/g, " ");

    if (cleanedText.length > 0) {
      return cleanedText;
    }
  }

  return null;
};

/**
 * Extrae la fecha de creación de un texto.
 * @param {string} inputString - El texto del cual extraer la fecha de creación.
 * @returns {string|null} - La fecha de creación extraída o null si no se encuentra.
 */
const extractCreatedDate = (inputString) => {
  const textoSinPrimeros169Caracteres = inputString.slice(169);
  const regex = /([1-9]\d{7})/;
  const match = textoSinPrimeros169Caracteres.match(regex);
  const secuenciaNumerica = match ? match[1] : null;

  return secuenciaNumerica;
};

/**
 * Extrae la moneda de un texto.
 * @param {string} inputString - El texto del cual extraer la moneda.
 * @returns {string} - La moneda extraída.
 */
const extractCurrency = (inputString) => {
  inputString = inputString.slice(72);
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

module.exports = {
  extractDni,
  extractCBU,
  extractTypeId,
  extractTextAfterTwoSpaces,
  extractNumberOfCharacters,
  extractCharactersAfterText,
  extractAlias,
  extractLetters,
  findFirstNumericSequence,
  extractAndCleanCharacters,
  extractCreatedDate,
  extractCurrency,
};