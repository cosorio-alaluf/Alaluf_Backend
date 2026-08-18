const express = require("express");
const router = express.Router();
const { getUfValue } = require("../utils/ufService");


router.get("/uf", async (req, res) => {
  try {
    const dataUf = await getUfValue();
    return res.json({ success: true, ...dataUf });
  } catch (error) {
    return res.status(502).json({ 
      success: false, 
      message: "Indicador económico no disponible en este momento" 
    });
  }
});


router.post("/leads", async (req, res) => {
  try {
    
    const { 
      razon_social, 
      rut, 
      email, 
      fono, 
      requerimiento, 
      id_tipo_propiedad,
      id_objetivo_llamada,
      fk_comuna,
      id_prop_pw,
      agendamiento,
      fecha_visita_meli,
      hora_visita_meli
    } = req.body;

    
    // =========================================================
    // VALIDACIÓN
    // RUT YA NO ES OBLIGATORIO
    // =========================================================
    if (!razon_social) {
      return res.status(400).json({
        success: false,
        message: "Falta el parámetro obligatorio razon_social"
      });
    }

    
    const payload = {
      razon_social: String(razon_social).trim(),

      // =========================================================
      // RUT OPCIONAL
      // =========================================================
      rut: rut ? String(rut).trim() : "",

      email: email ? String(email).trim() : "",
      fono: fono ? String(fono).trim() : "",

      // Si no viene requerimiento, ponemos un texto por defecto
      requerimiento: requerimiento
        ? String(requerimiento).trim()
        : "Contacto desde Página Web",
      
      
      id_objetivo_llamada: id_objetivo_llamada
        ? Number(id_objetivo_llamada)
        : 2, 

      id_tipo_propiedad: id_tipo_propiedad
        ? Number(id_tipo_propiedad)
        : 1,   

      fk_comuna: fk_comuna
        ? Number(fk_comuna)
        : 0,           

      id_prop_pw: id_prop_pw
        ? String(id_prop_pw)
        : "0",        
      
      
      agendamiento: Boolean(agendamiento)     
    };

    
    if (payload.agendamiento) {
      if (!fecha_visita_meli || !hora_visita_meli) {
        return res.status(400).json({ 
          success: false, 
          message: "Si solicita agendamiento, la fecha y la hora de visita son obligatorias." 
        });
      }

      payload.fecha_visita_meli =
        String(fecha_visita_meli);

      payload.hora_visita_meli =
        String(hora_visita_meli);
    }

    
    const response = await fetch(
      "https://alaluf.cl/api/save_lead.php",
      {
        method: "POST",
        headers: {
          "X-API-KEY": process.env.ALALUF_API_KEY,
          "Content-Type": "application/json", 
          "User-Agent": "Mozilla/5.0"
        },
        body: JSON.stringify(payload) 
      }
    );

    
    const responseText = await response.text();
    let data;

    try {
      data = JSON.parse(responseText);
    } catch (parseError) {
      console.error(
        "⚠️ El servidor de Alaluf no devolvió un JSON válido. Respuesta recibida:",
        responseText
      );

      return res.status(502).json({
        success: false,
        message: "El servidor externo respondió con un formato no válido.",
        errorRaw: responseText
      });
    }

    
    if (!response.ok || data.ok === false) {
      console.error("❌ Error desde Alaluf:", data);

      return res
        .status(
          response.status === 200
            ? 400
            : response.status
        )
        .json({
          success: false,
          message: `Alaluf rechazó la petición: ${JSON.stringify(
            data.errores || data
          )}`,
          data: data
        });
    }

    
    return res.status(201).json({
      success: true,
      data: data
    });

  } catch (error) {
    console.error(
      "❌ Excepción crítica en el controlador de leads:",
      error
    );

    return res.status(500).json({
      success: false,
      message: "Error interno en el servidor puente",
      error: error.message
    });
  }
});


module.exports = router;