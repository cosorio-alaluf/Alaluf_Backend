// Variable en memoria a nivel de módulo para la caché
let cachedUF = null;
let lastFetchDate = null;

const getUfValue = async () => {
  const hoy = new Date().toISOString().split("T")[0]; // Formato YYYY-MM-DD


  if (cachedUF && lastFetchDate === hoy) {
    return { valor: cachedUF, source: "cache" };
  }

  try {
   
    const response = await fetch("https://mindicador.cl/api/uf");
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    
    
    const valorUf = data.serie[0].valor;

    
    cachedUF = valorUf;
    lastFetchDate = hoy;

    return { valor: valorUf, source: "api" };

  } catch (error) {
    console.error("⚠️ Error en ufService consultando mindicador.cl:", error.message);

    
    if (cachedUF) {
      return { valor: cachedUF, source: "fallback_cache" };
    }
    
    throw error;
  }
};

module.exports = { getUfValue };