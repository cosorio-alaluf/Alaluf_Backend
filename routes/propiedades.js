const express = require('express');
const router = express.Router();
const axios = require('axios');
const https = require('node:https');
const NodeCache = require('node-cache');
const { performance } = require('node:perf_hooks');

// Configuración de caché ajustable por variables de entorno.
// La frescura lógica es corta, pero el dato anterior se conserva como respaldo
// para responder sin demoras mientras se reconstruye el catálogo.
const CATALOG_FRESH_MS = Number(process.env.CATALOG_FRESH_MS || 60_000);
const CATALOG_CACHE_TTL_SECONDS = Number(
    process.env.CATALOG_CACHE_TTL_SECONDS || 21_600
); // 6 horas
const CATALOG_STALE_TTL_SECONDS = Number(
    process.env.CATALOG_STALE_TTL_SECONDS || 86_400
); // 24 horas
const DETAIL_FRESH_MS = Number(process.env.DETAIL_FRESH_MS || 60_000);
const DETAIL_CACHE_TTL_SECONDS = Number(
    process.env.DETAIL_CACHE_TTL_SECONDS || 21_600
); // 6 horas
const CACHE_REFRESH_SCAN_MS = Number(
    process.env.CACHE_REFRESH_SCAN_MS || 15_000
);
const ACTIVE_CATALOG_WINDOW_MS = Number(
    process.env.ACTIVE_CATALOG_WINDOW_MS || 300_000
); // 5 minutos

const searchCache = new NodeCache({
    stdTTL: CATALOG_CACHE_TTL_SECONDS,
    checkperiod: 120,
    useClones: false
});

const staleSearchCache = new NodeCache({
    stdTTL: CATALOG_STALE_TTL_SECONDS,
    checkperiod: 300,
    useClones: false
});

const detailCache = new NodeCache({
    stdTTL: DETAIL_CACHE_TTL_SECONDS,
    checkperiod: 120,
    useClones: false
});

// Evita descargas duplicadas para la misma búsqueda o detalle.
const pendingSearches = new Map();
const pendingDetails = new Map();

// Registra qué catálogos se consultaron recientemente para refrescar solo
// los que realmente están siendo utilizados.
const activeCatalogs = new Map();

// Suscriptores SSE opcionales para avisar al frontend cuando un catálogo cambia.
const cacheEventSubscribers = new Set();

const keepAliveAgent = new https.Agent({
    keepAlive: true,
    maxSockets: 10,
    maxFreeSockets: 10,
    timeout: 10000
});

const alalufAxios = axios.create({
    httpsAgent: keepAliveAgent,
    headers: {
        'X-API-KEY': process.env.ALALUF_API_KEY,
        'User-Agent':
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 ' +
            '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        Accept: 'application/json'
    },
    timeout: 5000,
    validateStatus: (status) => status >= 200 && status < 500
});

const BASE_URL_ALALUF = 'https://alaluf.cl';
const SISTEMA_URL_ALALUF = 'https://sistema.alaluf.com';

// Versión lógica del catálogo. Evita reutilizar snapshots creados con una
// estructura anterior donde los precios definían la operación.
const CATALOG_CACHE_SCHEMA = 'desc-obj-v3-objetivo-3';

const COMUNAS_POR_CODIGO = Object.freeze({
    '1101': 'Iquique',
    '1211': 'Alto Hospicio',
    '1401': 'Pozo Almonte',
    '2101': 'Antofagasta',
    '2301': 'Calama',
    '3101': 'Copiapó',
    '3302': 'Freirina',
    '4101': 'La Serena',
    '4102': 'Coquimbo',
    '4106': 'Paihuano',
    '4203': 'Los Vilos',
    '4204': 'Punitaqui',
    '4301': 'Ovalle',
    '5101': 'Valparaiso',
    '5102': 'Casablanca',
    '5103': 'Concón',
    '5106': 'Quilpué',
    '5107': 'Quintero',
    '5109': 'Viña del Mar',
    '5301': 'Los Andes',
    '5303': 'Rinconada',
    '5501': 'Quillota',
    '5503': 'Hijuelas',
    '5505': 'Limache',
    '5507': 'Olmue',
    '5601': 'San Antonio',
    '5602': 'Algarrobo',
    '5603': 'Cartagena',
    '5604': 'El Quisco',
    '5606': 'Santo Domingo',
    '5607': 'El Tabo',
    '5701': 'San Felipe',
    '5702': 'Catemu',
    '5705': 'Putaendo',
    '6101': 'Rancagua',
    '6102': 'Codegua',
    '6107': 'Las Cabras',
    '6108': 'Machalí',
    '6202': 'La Estrella',
    '6203': 'Litueche',
    '6301': 'San Fernando',
    '6303': 'Chimbarongo',
    '6310': 'Santa Cruz',
    '7101': 'Talca',
    '7102': 'Constitución',
    '7103': 'Romeral',
    '7201': 'Cauquenes',
    '7206': 'Maule',
    '7301': 'Curicó',
    '7401': 'Linares',
    '7403': 'Longaví',
    '7404': 'Parral',
    '8101': 'Concepción',
    '8109': 'San Carlos',
    '8212': 'Hualpén',
    '8301': 'Los Angeles',
    '8401': 'Chillán',
    '9101': 'Temuco',
    '9115': 'Pucón',
    '9120': 'Villarrica',
    '10101': 'Puerto Montt',
    '10105': 'Futrono',
    '10108': 'Panguipulli',
    '10109': 'Puerto Varas',
    '10111': 'Río Bueno',
    '10201': 'Castro',
    '10203': 'Puerto Octay',
    '10301': 'Osorno',
    '10306': 'Llanquihue',
    '10402': 'Chonchi',
    '10406': 'Ancud',
    '10501': 'Valdivia',
    '11101': 'Chile Chico',
    '11201': 'Aysén',
    '12101': 'Natales',
    '12205': 'Punta Arenas',
    '13101': 'Santiago',
    '13102': 'Cerrillos',
    '13103': 'Cerro Navia',
    '13104': 'Conchalí',
    '13105': 'El Bosque',
    '13106': 'Estación Central',
    '13107': 'Huechuraba',
    '13108': 'Independencia',
    '13109': 'La Cisterna',
    '13110': 'La Florida',
    '13111': 'La Granja',
    '13112': 'La Pintana',
    '13113': 'La Reina',
    '13114': 'Las Condes',
    '13115': 'Lo Barnechea',
    '13116': 'Lo Espejo',
    '13117': 'Lo Prado',
    '13118': 'Macul',
    '13119': 'Maipú',
    '13120': 'Ñuñoa',
    '13121': 'Pedro Aguirre Cerda',
    '13122': 'Peñalolén',
    '13123': 'Providencia',
    '13124': 'Pudahuel',
    '13125': 'Quilicura',
    '13126': 'Quinta Normal',
    '13127': 'Recoleta',
    '13128': 'Renca',
    '13129': 'San Joaquín',
    '13130': 'San Miguel',
    '13131': 'San Ramón',
    '13132': 'Vitacura',
    '13134': 'Laguna de Aculeo',
    '13201': 'Puente Alto',
    '13202': 'Pirque',
    '13203': 'San José de Maipo',
    '13301': 'Colina',
    '13302': 'Lampa',
    '13303': 'Tiltil',
    '13401': 'San Bernardo',
    '13402': 'Buin',
    '13403': 'Calera de Tango',
    '13404': 'Paine',
    '13501': 'Melipilla',
    '13502': 'Alhué',
    '13503': 'Curacaví',
    '13504': 'María Pinto',
    '13505': 'San Pedro',
    '13601': 'Talagante',
    '13602': 'El Monte',
    '13603': 'Isla de Maipo',
    '13604': 'Padre Hurtado',
    '13605': 'Peñaflor',
    '13608': 'Llay-Llay',
    '13609': 'La Ligua',
    '13612': 'Requinoa',
    '13617': 'Frutillar',
    '13619': 'Isla de Pascua',
    '13620': 'Puchuncavi',
    '13621': 'Placilla',
    '13622': 'Vallenar',
    '13623': 'Illapel',
    '13624': 'Salamanca',
    '13625': 'La Ligua',
    '13626': 'Pichilemu',
    '13627': 'Zapallar',
    '13628': 'Tome',
    '13629': 'Villa Alemana',
    '13630': 'Arauco',
    '13631': 'Papudo',
    '13632': 'Cochamo',
    '13633': 'Los Muermos',
    '13634': 'San Juan de la Costa',
    '13635': 'Santa Maria',
    '13636': 'Coronel',
    '13637': 'San Pedro de la Paz',
    '13638': 'Navidad',
    '13639': 'Arica',
    '13670': 'Vicuña',
    '13671': 'Talcahuano',
    '13672': 'La Cruz',
    '14201': 'La Unión',
    '16101': 'Nuble',
    '16103': 'San Francisco de Mostazal',
});

// La API soporta limit/offset. Usamos páginas internas más grandes para reducir
// la cantidad de viajes a PHP, pero al frontend solo se le entregan 10/20 ítems.
// La API de Alaluf actualmente limita la respuesta real a cerca de 20
// elementos aunque se solicite un límite mayor. Se solicita 1000 primero
// por si el proveedor habilita nuevamente respuestas grandes.
const CATALOG_PAGE_SIZE = 1000;

// Con 531 propiedades y páginas reales de 20 registros se requieren unas
// 27 llamadas. Ejecutar 7 en paralelo reduce el proceso a aproximadamente
// cinco rondas, en vez de las catorce rondas que producía BATCH_SIZE = 2.
const BATCH_SIZE = 7;
const MAX_OFFSET = 10000;

const limpiarValor = (value) => {
    if (value === undefined || value === null) return '';
    const normalized = String(value).trim();
    return normalized === 'undefined' || normalized === 'null' ? '' : normalized;
};

// Normaliza el campo de video recibido desde la API de Alaluf.
// Soporta URL completa, ruta relativa, iframe, objetos y arreglos.
const resolverUrlMultimedia = (value) => {
    if (value === undefined || value === null || value === '') {
        return null;
    }

    if (Array.isArray(value)) {
        for (const item of value) {
            const resolved = resolverUrlMultimedia(item);
            if (resolved) return resolved;
        }

        return null;
    }

    if (typeof value === 'object') {
        const posiblesValores = [
            value.video_url,
            value.videoUrl,
            value.url_video,
            value.video,
            value.url,
            value.src,
            value.link,
            value.value
        ];

        for (const item of posiblesValores) {
            const resolved = resolverUrlMultimedia(item);
            if (resolved) return resolved;
        }

        return null;
    }

    let raw = limpiarValor(value);
    if (!raw) return null;

    // Algunos proveedores entregan el reproductor como iframe HTML.
    const iframeMatch = raw.match(
        /<iframe[^>]+src=["']([^"']+)["']/i
    );

    if (iframeMatch?.[1]) {
        raw = iframeMatch[1].trim();
    }

    // También acepta un JSON serializado con la URL del video.
    if (
        (raw.startsWith('{') && raw.endsWith('}')) ||
        (raw.startsWith('[') && raw.endsWith(']'))
    ) {
        try {
            const parsed = JSON.parse(raw);
            return resolverUrlMultimedia(parsed);
        } catch {
            // Si no es JSON válido, continúa procesándolo como texto.
        }
    }

    // URL protocol-relative: //www.youtube.com/...
    if (raw.startsWith('//')) {
        return `https:${raw}`;
    }

    // URL absoluta: YouTube, Vimeo, MP4, WebM u otro proveedor.
    if (/^https?:\/\//i.test(raw)) {
        return raw;
    }

    // Algunos registros entregan el ID de YouTube acompañado del proveedor.
    // Ejemplo: K11cNaArmkI;youtube
    const youtubeConProveedor = raw.match(
        /^([a-zA-Z0-9_-]{11})\s*;\s*youtube$/i
    );

    if (youtubeConProveedor?.[1]) {
        return `https://youtu.be/${youtubeConProveedor[1]}`;
    }

    // Algunos registros entregan solamente el ID de YouTube.
    // Ejemplo: F78GcjP8chM
    if (/^[a-zA-Z0-9_-]{11}$/.test(raw)) {
        return `https://youtu.be/${raw}`;
    }

    // También soporta enlaces de YouTube o Vimeo sin protocolo.
    if (
        /^(?:www\.)?(?:youtube\.com|youtu\.be|vimeo\.com|player\.vimeo\.com)\//i.test(raw)
    ) {
        return `https://${raw}`;
    }

    const cleanPath = raw.replace(/^\/+/, '');

    // Cuando la API entrega únicamente el nombre del archivo.
    if (!cleanPath.includes('/')) {
        return `${SISTEMA_URL_ALALUF}/nuevo/uploads/${cleanPath}`;
    }

    // Rutas relativas pertenecientes al sistema de Alaluf.
    if (
        cleanPath.startsWith('nuevo/') ||
        cleanPath.startsWith('uploads/')
    ) {
        const finalPath = cleanPath.startsWith('uploads/')
            ? `nuevo/${cleanPath}`
            : cleanPath;

        return `${SISTEMA_URL_ALALUF}/${finalPath}`;
    }

    return `${BASE_URL_ALALUF}/${cleanPath}`;
};

const normalizarTexto = (value) => {
    return limpiarValor(value)
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
};

const parsePositiveInt = (value, fallback, max = Number.MAX_SAFE_INTEGER) => {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed < 1) return fallback;
    return Math.min(parsed, max);
};

// Valores admitidos por la API de Alaluf:
// obj=1 -> Venta
// obj=2 -> Arriendo
// obj=3 -> Venta o Arriendo
const normalizarObjetivo = (value) => {
    const raw = normalizarTexto(value);

    if (
        ['1', 'venta', 'vender', 'sale'].includes(raw)
    ) {
        return '1';
    }

    if (
        [
            '2',
            'arriendo',
            'arrendar',
            'renta',
            'rent'
        ].includes(raw)
    ) {
        return '2';
    }

    if (
        [
            '3',
            'venta o arriendo',
            'venta y arriendo',
            'venta / arriendo',
            'venta/arriendo',
            'ambas',
            'ambos',
            'todos',
            'todas'
        ].includes(raw)
    ) {
        return '3';
    }

    return '';
};

// desc_obj es el campo mandante para determinar si una propiedad pertenece
// a Venta, Arriendo o a ambas operaciones.
const obtenerEstadoOperacion = (value) => {
    const original = limpiarValor(value);
    const normalizada = normalizarTexto(original);

    return {
        original,
        normalizada,
        permiteVenta: normalizada.includes('venta'),
        permiteArriendo: normalizada.includes('arriendo')
    };
};

const operacionCoincide = (descObj, objetivo) => {
    const objetivoNormalizado =
        normalizarObjetivo(objetivo);

    if (!objetivoNormalizado) return true;

    const estado =
        obtenerEstadoOperacion(descObj);

    if (objetivoNormalizado === '1') {
        return estado.permiteVenta;
    }

    if (objetivoNormalizado === '2') {
        return estado.permiteArriendo;
    }

    // obj=3 corresponde a Venta o Arriendo.
    return (
        estado.permiteVenta ||
        estado.permiteArriendo
    );
};

const crearCacheKey = (prefix, params) => {
    const normalized = Object.entries(params)
        .filter(([, value]) => value !== undefined && value !== null && value !== '')
        .sort(([keyA], [keyB]) => keyA.localeCompare(keyB))
        .map(([key, value]) => `${key}=${String(value).trim().toLowerCase()}`)
        .join('&');

    return `${prefix}|${normalized}`;
};


const obtenerEdadMs = (entry) => {
    if (!entry?.updatedAt) return Number.POSITIVE_INFINITY;
    return Math.max(0, Date.now() - entry.updatedAt);
};

const registrarCatalogoActivo = (cacheKey, catalogQuery) => {
    activeCatalogs.set(cacheKey, {
        cacheKey,
        catalogQuery: { ...catalogQuery },
        lastAccessAt: Date.now()
    });
};

const publicarEventoCache = (event, payload = {}) => {
    const message = `event: ${event}\ndata: ${JSON.stringify({
        ...payload,
        emittedAt: new Date().toISOString()
    })}\n\n`;

    for (const subscriber of cacheEventSubscribers) {
        try {
            subscriber.write(message);
        } catch {
            cacheEventSubscribers.delete(subscriber);
        }
    }
};

const normalizarMoneda = (value) => {
    const normalized = limpiarValor(value)
        .toUpperCase()
        .replace(/\s+/g, '')
        .replace('M²', 'M2');

    if (normalized === '$' || normalized === 'CLP') return 'CLP';
    if (normalized === 'UF/M2' || normalized === 'UFM2') return 'UF/M2';
    if (normalized === 'UF') return 'UF';
    return normalized;
};

const precioCumpleRango = (precio, monedaSolicitada, minimo, maximo) => {
    const valor = Number.parseFloat(precio?.valor);
    if (!Number.isFinite(valor) || valor <= 0) return false;

    if (
        monedaSolicitada &&
        normalizarMoneda(precio?.moneda) !== monedaSolicitada
    ) {
        return false;
    }

    if (Number.isFinite(minimo) && valor < minimo) return false;
    if (Number.isFinite(maximo) && valor > maximo) return false;
    return true;
};

const extraerItems = (payload) => {
    if (Array.isArray(payload)) return payload;
    if (Array.isArray(payload?.data)) return payload.data;
    if (Array.isArray(payload?.resultados)) return payload.resultados;
    if (Array.isArray(payload?.propiedades)) return payload.propiedades;
    return [];
};

const obtenerIdUnico = (prop = {}) => {
    return String(
        prop.id_propiedad ||
        prop.codigo_propiedad ||
        prop.codigo_interno ||
        ''
    );
};

const mapearPropiedad = (prop = {}) => {
    let fotosRaw =
        prop.foto_principal ||
        prop.fotos ||
        prop.foto ||
        prop.foto_portada ||
        prop.imagen ||
        prop.path_foto ||
        prop.img_1 ||
        [];

    if (typeof fotosRaw === 'string' && fotosRaw.length > 0) {
        fotosRaw = fotosRaw.includes(',') ? fotosRaw.split(',') : [fotosRaw];
    }

    const imagenesProcesadas = Array.isArray(fotosRaw)
        ? fotosRaw
            .map((foto) => {
                if (!foto || typeof foto !== 'string') return null;

                let link = foto.trim();
                if (!link) return null;
                if (link.startsWith('http')) return link;

                const cleanLink = link.startsWith('/') ? link.substring(1) : link;

                if (!cleanLink.includes('/')) {
                    return `${SISTEMA_URL_ALALUF}/nuevo/uploads/${cleanLink}`;
                }

                if (!link.startsWith('/')) link = `/${link}`;

                if (link.startsWith('/nuevo') || link.startsWith('/uploads')) {
                    const finalPath = link.startsWith('/uploads')
                        ? `/nuevo${link}`
                        : link;
                    return `${SISTEMA_URL_ALALUF}${finalPath}`;
                }

                return `${BASE_URL_ALALUF}${link}`;
            })
            .filter(Boolean)
        : [];

    const lat = Number.parseFloat(prop.latitud);
    const lng = Number.parseFloat(prop.longitud);

    const extraerCampo = (labelDeseado) => {
        if (!Array.isArray(prop.campos_especificos)) return null;

        const campo = prop.campos_especificos.find((item) => {
            return (
                typeof item?.label === 'string' &&
                normalizarTexto(item.label).includes(
                    normalizarTexto(labelDeseado)
                )
            );
        });

        return campo?.value ?? null;
    };

    const videoUrl = resolverUrlMultimedia(
        prop.video_url ||
        prop.videoUrl ||
        prop.url_video ||
        prop.video ||
        prop.video_propiedad ||
        prop.video_internet ||
        prop.multimedia?.video_url ||
        prop.multimedia?.videoUrl ||
        prop.multimedia?.url_video ||
        prop.multimedia?.video ||
        extraerCampo('video_url') ||
        extraerCampo('url video') ||
        extraerCampo('video')
    );

    const descObjOriginal = limpiarValor(
        prop.desc_obj ||
        prop.operacion ||
        prop.objetivo
    );

    const estadoOperacion = obtenerEstadoOperacion(descObjOriginal);

    const valorVentaRaw = limpiarValor(prop.valor_venta);
    const valorArriendoRaw = limpiarValor(prop.valor_arriendo);

    const tieneValorVenta =
        Number.parseFloat(valorVentaRaw || 0) > 0;

    const tieneValorArriendo =
        Number.parseFloat(valorArriendoRaw || 0) > 0;

    return {
        id: prop.id_propiedad,
        codigo:
            prop.codigo_propiedad ||
            prop.codigo_interno ||
            prop.id_propiedad,
        titulo: prop.desc_tipo_prop || prop.desc_tipo || 'Propiedad',

        // Se conserva el valor original entregado por el CRM.
        desc_obj: descObjOriginal || null,
        operacion: descObjOriginal || 'Sin operación',

        ubicacion: {
            comuna: prop.com_nombre || prop.comuna || 'Sin Comuna',
            codigoComuna: limpiarValor(
                prop.com_codigo ||
                prop.codigo_comuna ||
                prop.cod_comuna ||
                prop.com_cod ||
                prop.id_comuna ||
                prop.comuna_id ||
                prop.com_id
            ) || null,
            sector: prop.sector_cercano || 'Sin Sector',
            region: prop.region || 'Metropolitana',
            direccion: prop.direccion || ''
        },
        coords: {
            lat: Number.isFinite(lat) && lat !== 0 ? lat : null,
            lng: Number.isFinite(lng) && lng !== 0 ? lng : null
        },

        /*
         * Los precios no determinan la operación.
         * Solo se exponen cuando desc_obj permite esa operación.
         */
        precios: {
            venta: {
                valor:
                    estadoOperacion.permiteVenta &&
                    tieneValorVenta
                        ? valorVentaRaw
                        : null,
                moneda:
                    estadoOperacion.permiteVenta
                        ? prop.moneda_venta || 'UF'
                        : null
            },
            arriendo: {
                valor:
                    estadoOperacion.permiteArriendo &&
                    tieneValorArriendo
                        ? valorArriendoRaw
                        : null,
                moneda:
                    estadoOperacion.permiteArriendo
                        ? prop.moneda_arriendo || 'UF/m2'
                        : null
            }
        },
        detalles: {
            superficie:
                Number.parseFloat(
                    prop.m2_utiles ||
                    prop.m2_construidos ||
                    prop.m2_terreno ||
                    extraerCampo('m² Construidos') ||
                    extraerCampo('m² Útiles')
                ) || 0,
            banos:
                Number.parseInt(prop.banos || extraerCampo('Baños'), 10) || 0,
            dormitorios:
                Number.parseInt(
                    prop.dormitorios ||
                    extraerCampo('Dormitorios') ||
                    extraerCampo('Habitaciones'),
                    10
                ) || 0,
            privados:
                Number.parseInt(prop.privados || extraerCampo('Privados'), 10) || 0,
            estacionamientos:
                Number.parseInt(
                    prop.estacionamientos || extraerCampo('Estacionamientos'),
                    10
                ) || 0,
            caracteristicasExtra: prop.campos_especificos || [],
            descripcion:
                prop.caracteristicas_internet || prop.observaciones || ''
        },
        video_url: videoUrl,
        imagenes: imagenesProcesadas
    };
};

const obtenerConsulta = (query) => {
    const esDestacada = limpiarValor(query.destaq).toLowerCase() === 'true';
    const page = parsePositiveInt(query.page, 1);
    const limit = parsePositiveInt(query.limit, esDestacada ? 20 : 10, 50);
    const offset = (page - 1) * limit;

    const tipoProp = limpiarValor(query.tipo_prop || query.tipo);
    const obj = normalizarObjetivo(query.obj || query.objetivo);
    const comunaCodigo = limpiarValor(query.comuna);
    const comunaNombre = limpiarValor(
        query.comuna_nombre ||
        query.comuna_label ||
        COMUNAS_POR_CODIGO[comunaCodigo]
    );
    const precioMinRaw = limpiarValor(query.precio_min || query.precio_desde);
    const precioMaxRaw = limpiarValor(query.precio_max || query.precio_hasta);
    const moneda = normalizarMoneda(query.moneda);
    const destaq = limpiarValor(query.destaq);

    // El CRM de Alaluf necesita la combinación tipo_prop + obj + comuna
    // para devolver el conjunto correcto. Por eso la comuna forma parte del
    // snapshot cuando viene informada. Precio, superficie, orden y paginación
    // continúan procesándose localmente para mantener las búsquedas rápidas.
    // La operación se agrega al solicitar el snapshot porque la API exige
    // obj=1 (venta) u obj=2 (arriendo).
    const catalogQuery = {
        tipo_prop: tipoProp
    };

    if (comunaCodigo) catalogQuery.comuna = comunaCodigo;
    if (destaq) catalogQuery.destaq = destaq;

    return {
        page,
        limit,
        offset,
        catalogQuery,
        orden: limpiarValor(query.orden).toLowerCase(),
        dir: limpiarValor(query.dir).toUpperCase(),
        obj,
        comunaCodigo,
        comunaNombre,
        precioMin: Number.parseFloat(precioMinRaw),
        precioMax: Number.parseFloat(precioMaxRaw),
        moneda,
        supDesde: Number.parseFloat(query.sup_desde),
        supHasta: Number.parseFloat(query.sup_hasta)
    };
};

const solicitarPaginaAlaluf = async (catalogQuery, limit, offset) => {
    const response = await alalufAxios.get(`${BASE_URL_ALALUF}/api/res.php`, {
        params: {
            ...catalogQuery,
            limit,
            offset
        }
    });

    if (response.status >= 400) {
        const error = new Error(`La API Alaluf respondió HTTP ${response.status}`);
        error.response = response;
        throw error;
    }

    return extraerItems(response.data);
};

const construirCatalogo = async (catalogQuery, cacheKey) => {
    const apiStartedAt = performance.now();
    let peticionesApi = 0;
    let catalogoCompleto = true;

    const primeraPagina = await solicitarPaginaAlaluf(
        catalogQuery,
        CATALOG_PAGE_SIZE,
        0
    );
    peticionesApi += 1;

    const catalogo = [];
    const idsVistos = new Set();

    const agregarItems = (items) => {
        for (const item of items) {
            const id = obtenerIdUnico(item);

            if (!id) {
                catalogo.push(item);
                continue;
            }

            if (!idsVistos.has(id)) {
                idsVistos.add(id);
                catalogo.push(item);
            }
        }
    };

    agregarItems(primeraPagina);

    if (primeraPagina.length > 0) {
        const effectivePageSize = primeraPagina.length;
        let currentOffset = effectivePageSize;
        let hayMas = true;

        while (hayMas && currentOffset <= MAX_OFFSET) {
            const offsets = Array.from(
                { length: BATCH_SIZE },
                (_, index) => currentOffset + index * effectivePageSize
            );

            const respuestas = await Promise.allSettled(
                offsets.map((requestOffset) =>
                    solicitarPaginaAlaluf(
                        catalogQuery,
                        CATALOG_PAGE_SIZE,
                        requestOffset
                    )
                )
            );

            peticionesApi += respuestas.length;

            let encontroPaginaFinal = false;
            let huboError = false;

            respuestas.forEach((resultado, index) => {
                if (resultado.status === 'rejected') {
                    huboError = true;
                    console.error(
                        `Error descargando offset ${offsets[index]}:`,
                        resultado.reason?.message || resultado.reason
                    );
                    return;
                }

                const items = resultado.value;

                if (!Array.isArray(items) || items.length === 0) {
                    encontroPaginaFinal = true;
                    return;
                }

                agregarItems(items);

                if (items.length < effectivePageSize) {
                    encontroPaginaFinal = true;
                }
            });

            if (huboError) {
                throw new Error(
                    'No fue posible completar todas las páginas del catálogo.'
                );
            }

            if (encontroPaginaFinal) {
                hayMas = false;
            }

            currentOffset += effectivePageSize * BATCH_SIZE;
        }
    }

    const result = {
        // Se mapea una sola vez al construir el snapshot para que cada búsqueda
        // posterior solo filtre, ordene y pagine en memoria.
        items: catalogo.map(mapearPropiedad),
        rawItemsCount: catalogo.length,
        apiMs: performance.now() - apiStartedAt,
        peticionesApi,
        catalogoCompleto,
        updatedAt: Date.now()
    };

    // Solo se promociona a caché fresca cuando el catálogo terminó de forma
    // correcta. El respaldo conserva el último catálogo utilizable.
    if (catalogoCompleto) {
        searchCache.set(cacheKey, result);
        staleSearchCache.set(cacheKey, result);

        publicarEventoCache('catalog-updated', {
            cacheKey,
            items: result.items.length,
            updatedAt: new Date(result.updatedAt).toISOString()
        });
    }

    return result;
};

const iniciarActualizacionCatalogo = (catalogQuery, cacheKey) => {
    if (pendingSearches.has(cacheKey)) {
        return pendingSearches.get(cacheKey);
    }

    const requestPromise = construirCatalogo(catalogQuery, cacheKey)
        .finally(() => {
            pendingSearches.delete(cacheKey);
        });

    pendingSearches.set(cacheKey, requestPromise);
    return requestPromise;
};

const descargarCatalogoCompleto = async (catalogQuery, options = {}) => {
    const { forceRefresh = false } = options;
    const cacheKey = crearCacheKey(`catalog:${CATALOG_CACHE_SCHEMA}`, catalogQuery);
    registrarCatalogoActivo(cacheKey, catalogQuery);

    const cached = searchCache.get(cacheKey);

    if (cached && !forceRefresh) {
        const stale = obtenerEdadMs(cached) > CATALOG_FRESH_MS;
        let refreshing = false;

        if (stale) {
            refreshing = true;
            iniciarActualizacionCatalogo(catalogQuery, cacheKey).catch((error) => {
                console.error(
                    `No fue posible actualizar ${cacheKey} en segundo plano:`,
                    error.message
                );
            });
        }

        return {
            ...cached,
            cacheHit: true,
            stale,
            refreshing,
            cacheAgeMs: obtenerEdadMs(cached)
        };
    }

    if (forceRefresh) {
        const freshResult = await iniciarActualizacionCatalogo(
            catalogQuery,
            cacheKey
        );

        return {
            ...freshResult,
            cacheHit: Boolean(cached),
            stale: false,
            refreshing: false,
            forcedRefresh: true,
            cacheAgeMs: 0
        };
    }

    const staleCached = staleSearchCache.get(cacheKey);

    if (staleCached) {
        iniciarActualizacionCatalogo(catalogQuery, cacheKey).catch((error) => {
            console.error(
                `No fue posible recuperar ${cacheKey} en segundo plano:`,
                error.message
            );
        });

        return {
            ...staleCached,
            cacheHit: true,
            stale: true,
            refreshing: true,
            fallbackCache: true,
            cacheAgeMs: obtenerEdadMs(staleCached)
        };
    }

    if (pendingSearches.has(cacheKey)) {
        const pendingResult = await pendingSearches.get(cacheKey);
        return {
            ...pendingResult,
            cacheHit: true,
            stale: false,
            sharedRequest: true,
            cacheAgeMs: 0
        };
    }

    const result = await iniciarActualizacionCatalogo(catalogQuery, cacheKey);

    return {
        ...result,
        cacheHit: false,
        stale: false,
        cacheAgeMs: 0
    };
};


const seleccionarOperacionMandante = (actual, nueva) => {
    const operacionActual = limpiarValor(
        actual?.desc_obj ||
        actual?.operacion
    );

    const operacionNueva = limpiarValor(
        nueva?.desc_obj ||
        nueva?.operacion
    );

    if (!operacionActual) return operacionNueva;
    if (!operacionNueva) return operacionActual;

    const estadoActual = obtenerEstadoOperacion(operacionActual);
    const estadoNuevo = obtenerEstadoOperacion(operacionNueva);

    if (estadoActual.normalizada === estadoNuevo.normalizada) {
        return operacionNueva;
    }

    /*
     * Si uno de los snapshots informa una operación única y el otro una
     * operación doble, se prefiere la operación única para no conservar
     * un estado anterior de Venta / Arriendo.
     */
    const actualEsDoble =
        estadoActual.permiteVenta &&
        estadoActual.permiteArriendo;

    const nuevaEsDoble =
        estadoNuevo.permiteVenta &&
        estadoNuevo.permiteArriendo;

    if (actualEsDoble && !nuevaEsDoble) {
        return operacionNueva;
    }

    if (!actualEsDoble && nuevaEsDoble) {
        return operacionActual;
    }

    // Si ambos valores son válidos pero distintos, se utiliza el snapshot
    // procesado más recientemente sin inventar una operación combinada.
    return operacionNueva;
};

const combinarPropiedadOperacion = (actual, nueva) => {
    if (!actual) return nueva;

    const operacionMandante =
        seleccionarOperacionMandante(actual, nueva);

    const estadoOperacion =
        obtenerEstadoOperacion(operacionMandante);

    const ventaActual = actual.precios?.venta;
    const ventaNueva = nueva.precios?.venta;
    const arriendoActual = actual.precios?.arriendo;
    const arriendoNuevo = nueva.precios?.arriendo;

    const ventaSeleccionada =
        Number.parseFloat(ventaNueva?.valor || 0) > 0
            ? ventaNueva
            : ventaActual;

    const arriendoSeleccionado =
        Number.parseFloat(arriendoNuevo?.valor || 0) > 0
            ? arriendoNuevo
            : arriendoActual;

    return {
        ...actual,
        ...nueva,

        desc_obj: operacionMandante || null,
        operacion: operacionMandante || 'Sin operación',

        precios: {
            venta: {
                valor:
                    estadoOperacion.permiteVenta &&
                    Number.parseFloat(ventaSeleccionada?.valor || 0) > 0
                        ? ventaSeleccionada.valor
                        : null,
                moneda:
                    estadoOperacion.permiteVenta
                        ? ventaSeleccionada?.moneda || null
                        : null
            },
            arriendo: {
                valor:
                    estadoOperacion.permiteArriendo &&
                    Number.parseFloat(arriendoSeleccionado?.valor || 0) > 0
                        ? arriendoSeleccionado.valor
                        : null,
                moneda:
                    estadoOperacion.permiteArriendo
                        ? arriendoSeleccionado?.moneda || null
                        : null
            }
        },

        imagenes:
            Array.isArray(nueva.imagenes) &&
            nueva.imagenes.length > (actual.imagenes?.length || 0)
                ? nueva.imagenes
                : actual.imagenes,

        video_url:
            nueva.video_url ||
            actual.video_url,

        ubicacion: {
            ...actual.ubicacion,
            ...nueva.ubicacion
        },

        detalles: {
            ...actual.detalles,
            ...nueva.detalles
        }
    };
};

const combinarResultadosCatalogo = (results) => {
    const propiedadesPorId = new Map();

    for (const result of results) {
        for (const item of result.items || []) {
            const key = limpiarValor(item.id || item.codigo);

            if (!key) {
                propiedadesPorId.set(
                    `sin-id:${propiedadesPorId.size}`,
                    item
                );
                continue;
            }

            propiedadesPorId.set(
                key,
                combinarPropiedadOperacion(
                    propiedadesPorId.get(key),
                    item
                )
            );
        }
    }

    const updatedTimes = results
        .map((result) => Number(result.updatedAt || 0))
        .filter((value) => value > 0);

    return {
        items: [...propiedadesPorId.values()],
        rawItemsCount: results.reduce(
            (total, result) => total + Number(result.rawItemsCount || 0),
            0
        ),
        apiMs: results.reduce(
            (total, result) => total + Number(result.apiMs || 0),
            0
        ),
        peticionesApi: results.reduce(
            (total, result) => total + Number(result.peticionesApi || 0),
            0
        ),
        catalogoCompleto: results.every(
            (result) => result.catalogoCompleto !== false
        ),
        updatedAt: updatedTimes.length
            ? Math.min(...updatedTimes)
            : Date.now(),
        cacheHit: results.every((result) => Boolean(result.cacheHit)),
        stale: results.some((result) => Boolean(result.stale)),
        refreshing: results.some((result) => Boolean(result.refreshing)),
        fallbackCache: results.some(
            (result) => Boolean(result.fallbackCache)
        ),
        sharedRequest: results.some(
            (result) => Boolean(result.sharedRequest)
        ),
        cacheAgeMs: Math.max(
            0,
            ...results.map((result) => Number(result.cacheAgeMs || 0))
        ),
        catalogKeys: results.map((result) => result.cacheKey).filter(Boolean)
    };
};

// La API de Alaluf requiere obj.
// Cuando el frontend no informa una operación se utiliza obj=3,
// que corresponde a Venta o Arriendo.
const descargarCatalogosPorObjetivo = async (
    catalogQuery,
    obj,
    options = {}
) => {
    const objetivoNormalizado =
        normalizarObjetivo(obj) || '3';

    const objetivos = [
        objetivoNormalizado
    ];

    const settled = await Promise.allSettled(
        objetivos.map(async (objetivo) => {
            const queryPorObjetivo = {
                ...catalogQuery,
                obj: objetivo
            };

            const result = await descargarCatalogoCompleto(
                queryPorObjetivo,
                options
            );

            return {
                ...result,
                cacheKey: crearCacheKey(
                    `catalog:${CATALOG_CACHE_SCHEMA}`,
                    queryPorObjetivo
                ),
                objetivo
            };
        })
    );

    const fulfilled = settled
        .filter((result) => result.status === 'fulfilled')
        .map((result) => result.value);

    const rejected = settled.filter(
        (result) => result.status === 'rejected'
    );

    for (const failure of rejected) {
        console.error(
            'No fue posible cargar uno de los objetivos del catálogo:',
            failure.reason?.message || failure.reason
        );
    }

    if (fulfilled.length === 0) {
        throw rejected[0]?.reason || new Error(
            'No fue posible cargar el catálogo solicitado.'
        );
    }

    return combinarResultadosCatalogo(fulfilled);
};

const marcarCatalogosComoDesactualizados = (tipoProp = '') => {
    const tipoNormalizado = limpiarValor(tipoProp).toLowerCase();
    let marcados = 0;

    for (const cacheKey of searchCache.keys()) {
        if (
            tipoNormalizado &&
            !cacheKey.includes(`tipo_prop=${tipoNormalizado}`)
        ) {
            continue;
        }

        const entry = searchCache.get(cacheKey);
        if (!entry) continue;

        const staleEntry = { ...entry, updatedAt: 0 };
        searchCache.set(cacheKey, staleEntry);
        staleSearchCache.set(cacheKey, staleEntry);
        marcados += 1;
    }

    return marcados;
};

const refrescarCatalogosActivos = async ({ force = false } = {}) => {
    const now = Date.now();
    const jobs = [];

    for (const [cacheKey, activity] of activeCatalogs.entries()) {
        if (now - activity.lastAccessAt > ACTIVE_CATALOG_WINDOW_MS) {
            activeCatalogs.delete(cacheKey);
            continue;
        }

        const cached = searchCache.get(cacheKey);
        const needsRefresh =
            force || !cached || obtenerEdadMs(cached) > CATALOG_FRESH_MS;

        if (!needsRefresh || pendingSearches.has(cacheKey)) continue;

        // Se ejecuta secuencialmente para evitar golpear al CRM con varias
        // categorías completas al mismo tiempo.
        jobs.push(async () => {
            try {
                await iniciarActualizacionCatalogo(
                    activity.catalogQuery,
                    cacheKey
                );
            } catch (error) {
                console.error(
                    `Refresco automático falló para ${cacheKey}:`,
                    error.message
                );
            }
        });
    }

    for (const job of jobs) {
        await job();
    }

    return jobs.length;
};

const refreshTimer = setInterval(() => {
    refrescarCatalogosActivos().catch((error) => {
        console.error('Error en mantenimiento de caché:', error.message);
    });
}, CACHE_REFRESH_SCAN_MS);

// Permite que Node finalice normalmente durante pruebas o despliegues.
refreshTimer.unref?.();

const obtenerPrecioComparable = (item) => {
    const venta = Number.parseFloat(item.precios?.venta?.valor || 0);
    const arriendo = Number.parseFloat(item.precios?.arriendo?.valor || 0);
    return Math.max(venta, arriendo);
};

const filtrarPorOperacionYPrecio = (
    propiedades,
    { obj, precioMin, precioMax, moneda }
) => {
    const objetivo = normalizarObjetivo(obj);
    const tieneRango =
        Number.isFinite(precioMin) ||
        Number.isFinite(precioMax);

    return propiedades.filter((item) => {
        const descObj =
            item.desc_obj ||
            item.operacion;

        /*
         * desc_obj define si la propiedad pertenece a Venta o Arriendo.
         * La existencia de un precio no puede cambiar su operación.
         */
        if (
            objetivo &&
            !operacionCoincide(descObj, objetivo)
        ) {
            return false;
        }

        const venta = item.precios?.venta;
        const arriendo = item.precios?.arriendo;

        const permiteVenta =
            operacionCoincide(descObj, '1');

        const permiteArriendo =
            operacionCoincide(descObj, '2');

        if (objetivo === '1') {
            if (!tieneRango && !moneda) {
                return true;
            }

            return (
                permiteVenta &&
                precioCumpleRango(
                    venta,
                    moneda,
                    precioMin,
                    precioMax
                )
            );
        }

        if (objetivo === '2') {
            if (!tieneRango && !moneda) {
                return true;
            }

            return (
                permiteArriendo &&
                precioCumpleRango(
                    arriendo,
                    moneda,
                    precioMin,
                    precioMax
                )
            );
        }

        if (objetivo === '3') {
            if (!tieneRango && !moneda) {
                return (
                    permiteVenta ||
                    permiteArriendo
                );
            }

            return (
                (
                    permiteVenta &&
                    precioCumpleRango(
                        venta,
                        moneda,
                        precioMin,
                        precioMax
                    )
                ) ||
                (
                    permiteArriendo &&
                    precioCumpleRango(
                        arriendo,
                        moneda,
                        precioMin,
                        precioMax
                    )
                )
            );
        }

        if (tieneRango || moneda) {
            return (
                (
                    permiteVenta &&
                    precioCumpleRango(
                        venta,
                        moneda,
                        precioMin,
                        precioMax
                    )
                ) ||
                (
                    permiteArriendo &&
                    precioCumpleRango(
                        arriendo,
                        moneda,
                        precioMin,
                        precioMax
                    )
                )
            );
        }

        return (
            permiteVenta ||
            permiteArriendo
        );
    });
};

const ordenarPropiedades = (propiedades, orden, dir) => {
    const direccion = dir === 'DESC' ? 'DESC' : 'ASC';

    if (orden === 'asc' || orden === 'desc') {
        const sentido = orden === 'desc' ? -1 : 1;
        propiedades.sort(
            (a, b) =>
                (obtenerPrecioComparable(a) - obtenerPrecioComparable(b)) * sentido
        );
        return;
    }

    if (orden === 'reciente' || orden === 'nuevas') {
        propiedades.sort((a, b) => {
            const idA = Number.parseInt(a.id, 10) || 0;
            const idB = Number.parseInt(b.id, 10) || 0;
            return direccion === 'ASC' ? idA - idB : idB - idA;
        });
        return;
    }

    // Comportamiento por defecto: propiedades más nuevas primero.
    propiedades.sort((a, b) => {
        const idA = Number.parseInt(a.id, 10) || 0;
        const idB = Number.parseInt(b.id, 10) || 0;
        return idB - idA;
    });
};

// Búsqueda paginada con catálogo completo en caché.
router.get('/buscar', async (req, res) => {
    const totalStartedAt = performance.now();

    try {
        const {
            page,
            limit,
            offset,
            catalogQuery,
            orden,
            dir,
            obj,
            comunaCodigo,
            comunaNombre,
            precioMin,
            precioMax,
            moneda,
            supDesde,
            supHasta
        } = obtenerConsulta(req.query);

        if (!catalogQuery.tipo_prop) {
            return res.status(400).json({
                error: 'Debes indicar el tipo de propiedad.'
            });
        }

        const searchResult = await descargarCatalogosPorObjetivo(
            catalogQuery,
            obj
        );

        const processingStartedAt = performance.now();
        let propiedades = [...searchResult.items];

        // desc_obj es el campo mandante para operación. Precio y moneda se
        // aplican después, sin permitir que un valor antiguo cambie Venta por
        // Arriendo o viceversa.
        propiedades = filtrarPorOperacionYPrecio(propiedades, {
            obj,
            precioMin,
            precioMax,
            moneda
        });

        // La API externa puede ignorar el filtro comuna. Por eso se aplica
        // nuevamente en Node sobre el catálogo base, usando primero el código
        // disponible y, como respaldo, el nombre normalizado de la comuna.
        if (comunaCodigo || comunaNombre) {
            const comunaNombreNormalizada = normalizarTexto(comunaNombre);

            propiedades = propiedades.filter((item) => {
                const codigoItem = limpiarValor(item.ubicacion?.codigoComuna);

                if (comunaCodigo && codigoItem && codigoItem === comunaCodigo) {
                    return true;
                }

                if (comunaNombreNormalizada) {
                    return (
                        normalizarTexto(item.ubicacion?.comuna) ===
                        comunaNombreNormalizada
                    );
                }

                return false;
            });
        }

        // Al tener el catálogo completo, el filtro de superficie y su total
        // quedan correctamente calculados.
        if (Number.isFinite(supDesde)) {
            propiedades = propiedades.filter(
                (item) => item.detalles.superficie >= supDesde
            );
        }

        if (Number.isFinite(supHasta)) {
            propiedades = propiedades.filter(
                (item) => item.detalles.superficie <= supHasta
            );
        }

        ordenarPropiedades(propiedades, orden, dir);

        const totalPropiedades = propiedades.length;
        const totalPaginas = Math.max(1, Math.ceil(totalPropiedades / limit));
        const paginaNormalizada = Math.min(page, totalPaginas);
        const startIndex = (paginaNormalizada - 1) * limit;
        const resultados = propiedades.slice(startIndex, startIndex + limit);

        const processingMs = performance.now() - processingStartedAt;
        const totalMs = performance.now() - totalStartedAt;

        console.table({
            endpoint: '/api/propiedades/buscar',
            cache: searchResult.cacheHit,
            compartida: Boolean(searchResult.sharedRequest),
            apiMs: Math.round(searchResult.apiMs || 0),
            procesamientoMs: Math.round(processingMs),
            totalMs: Math.round(totalMs),
            catalogo: searchResult.items.length,
            totalFiltrado: totalPropiedades,
            objetivo: obj === '1'
                ? 'VENTA'
                : obj === '2'
                    ? 'ARRIENDO'
                    : 'VENTA O ARRIENDO',
            campoMandante: 'desc_obj',
            comuna: comunaNombre || comunaCodigo || 'GENERAL',
            entregadas: resultados.length,
            pagina: paginaNormalizada,
            totalPaginas,
            peticionesApi: searchResult.peticionesApi || 0,
            catalogoCompleto: searchResult.catalogoCompleto !== false,
            stale: Boolean(searchResult.stale)
        });

        return res.json({
            data: resultados,
            paginacion: {
                totalPropiedades,
                paginaActual: paginaNormalizada,
                totalPaginas,
                propiedadesPorPagina: limit,
                tienePaginaSiguiente: paginaNormalizada < totalPaginas
            },
            meta: {
                cache: searchResult.cacheHit,
                solicitudCompartida: Boolean(searchResult.sharedRequest),
                apiMs: Math.round(searchResult.apiMs || 0),
                procesamientoMs: Math.round(processingMs),
                tiempoTotalMs: Math.round(totalMs),
                catalogoDescargado: searchResult.items.length,
                filtroComuna: comunaNombre || comunaCodigo || null,
                objetivo:
                    obj === '1'
                        ? 'Venta'
                        : obj === '2'
                            ? 'Arriendo'
                            : 'Venta o Arriendo',
                campoOperacionMandante: 'desc_obj',
                peticionesApi: searchResult.peticionesApi || 0,
                catalogoCompleto: searchResult.catalogoCompleto !== false,
                cacheStale: Boolean(searchResult.stale),
                actualizandoCache: Boolean(searchResult.refreshing),
                cacheAgeSeconds: Math.round(
                    (searchResult.cacheAgeMs || 0) / 1000
                ),
                cacheUpdatedAt: searchResult.updatedAt
                    ? new Date(searchResult.updatedAt).toISOString()
                    : null,
                cacheFreshSeconds: Math.round(CATALOG_FRESH_MS / 1000)
            }
        });
    } catch (error) {
        console.error('Error en /buscar:', {
            mensaje: error.message,
            status: error.response?.status,
            respuestaAlaluf: error.response?.data,
            parametros: error.response?.config?.params
        });

        return res.status(502).json({
            error: 'No fue posible consultar las propiedades.',
            detalle:
                process.env.NODE_ENV === 'development'
                    ? {
                        mensaje: error.message,
                        statusAlaluf: error.response?.status,
                        respuestaAlaluf: error.response?.data
                    }
                    : undefined
        });
    }
});

const guardarDetalleEnCache = (propiedad, additionalKeys = []) => {
    if (!propiedad) return;

    const entry = {
        propiedad,
        updatedAt: Date.now()
    };

    const keys = new Set(additionalKeys.filter(Boolean));

    if (propiedad.id) keys.add(`detail:id:${propiedad.id}`);
    if (propiedad.codigo) keys.add(`detail:code:${propiedad.codigo}`);

    for (const key of keys) {
        detailCache.set(key, entry);
    }

    publicarEventoCache('property-updated', {
        id: propiedad.id || null,
        codigo: propiedad.codigo || null,
        updatedAt: new Date(entry.updatedAt).toISOString()
    });
};

const solicitarDetallePorId = async (idPropiedad) => {
    const response = await alalufAxios.get(
        `${BASE_URL_ALALUF}/api/propiedad.php`,
        {
            params: {
                id_propiedad: idPropiedad
            }
        }
    );

    if (response.status >= 400 || !response.data?.data) {
        return null;
    }

    return mapearPropiedad(response.data.data);
};

const iniciarActualizacionDetallePorId = (idPropiedad) => {
    const cacheKey = `detail:id:${idPropiedad}`;

    if (pendingDetails.has(cacheKey)) {
        return pendingDetails.get(cacheKey);
    }

    const requestPromise = (async () => {
        const propiedad = await solicitarDetallePorId(idPropiedad);

        if (propiedad) {
            guardarDetalleEnCache(propiedad, [cacheKey]);
        }

        return propiedad;
    })().finally(() => {
        pendingDetails.delete(cacheKey);
    });

    pendingDetails.set(cacheKey, requestPromise);
    return requestPromise;
};

const consultarDetallePorId = async (
    idPropiedad,
    { forceRefresh = false } = {}
) => {
    const cacheKey = `detail:id:${idPropiedad}`;
    const cached = detailCache.get(cacheKey);

    if (cached && !forceRefresh) {
        const stale = obtenerEdadMs(cached) > DETAIL_FRESH_MS;

        if (stale) {
            iniciarActualizacionDetallePorId(idPropiedad).catch((error) => {
                console.error(
                    `No fue posible refrescar detalle ${idPropiedad}:`,
                    error.message
                );
            });
        }

        return {
            propiedad: cached.propiedad,
            cacheHit: true,
            stale,
            refreshing: stale,
            cacheAgeMs: obtenerEdadMs(cached),
            updatedAt: cached.updatedAt
        };
    }

    if (pendingDetails.has(cacheKey)) {
        return {
            propiedad: await pendingDetails.get(cacheKey),
            cacheHit: Boolean(cached),
            sharedRequest: true,
            stale: false,
            cacheAgeMs: 0
        };
    }

    const propiedad = await iniciarActualizacionDetallePorId(idPropiedad);

    return {
        propiedad,
        cacheHit: Boolean(cached),
        stale: false,
        cacheAgeMs: 0,
        updatedAt: Date.now()
    };
};

const iniciarActualizacionDetallePorCodigo = (codigo) => {
    const pendingKey = `detail:lookup:${codigo}`;

    if (pendingDetails.has(pendingKey)) {
        return pendingDetails.get(pendingKey);
    }

    const requestPromise = (async () => {
        const searchResponse = await alalufAxios.get(
            `${BASE_URL_ALALUF}/api/res.php`,
            {
                params: {
                    q: codigo,
                    limit: 20,
                    offset: 0
                }
            }
        );

        const matches = extraerItems(searchResponse.data);
        const match = matches.find((item) => {
            return [
                item?.codigo_propiedad,
                item?.codigo_interno,
                item?.id_propiedad
            ].some((value) => String(value || '') === codigo);
        });

        const idPropiedad = match?.id_propiedad || codigo;
        const propiedad = await iniciarActualizacionDetallePorId(idPropiedad);

        if (propiedad) {
            guardarDetalleEnCache(propiedad, [`detail:code:${codigo}`]);
        }

        return propiedad;
    })().finally(() => {
        pendingDetails.delete(pendingKey);
    });

    pendingDetails.set(pendingKey, requestPromise);
    return requestPromise;
};

const verificarAdminCache = (req, res, next) => {
    const configuredToken = limpiarValor(process.env.CACHE_ADMIN_TOKEN);

    if (!configuredToken) {
        return res.status(503).json({
            error: 'CACHE_ADMIN_TOKEN no está configurado.'
        });
    }

    const suppliedToken = limpiarValor(
        req.get('x-cache-admin-token') ||
        req.get('authorization')?.replace(/^Bearer\s+/i, '')
    );

    if (suppliedToken !== configuredToken) {
        return res.status(401).json({
            error: 'No autorizado.'
        });
    }

    return next();
};

// Eventos ligeros para que el frontend pueda enterarse de refrescos sin usar
// Socket.IO. Se puede consumir con EventSource desde el navegador.
router.get('/cache/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();

    res.write(`event: connected\ndata: ${JSON.stringify({
        connectedAt: new Date().toISOString()
    })}\n\n`);

    cacheEventSubscribers.add(res);

    const keepAlive = setInterval(() => {
        res.write(': keep-alive\n\n');
    }, 25_000);

    req.on('close', () => {
        clearInterval(keepAlive);
        cacheEventSubscribers.delete(res);
    });
});

router.get('/cache/status', verificarAdminCache, (req, res) => {
    const now = Date.now();

    const catalogs = searchCache.keys().map((cacheKey) => {
        const entry = searchCache.get(cacheKey);
        const activity = activeCatalogs.get(cacheKey);

        return {
            cacheKey,
            items: entry?.items?.length || 0,
            updatedAt: entry?.updatedAt
                ? new Date(entry.updatedAt).toISOString()
                : null,
            ageSeconds: entry?.updatedAt
                ? Math.round((now - entry.updatedAt) / 1000)
                : null,
            fresh: entry
                ? obtenerEdadMs(entry) <= CATALOG_FRESH_MS
                : false,
            refreshing: pendingSearches.has(cacheKey),
            active: Boolean(
                activity &&
                now - activity.lastAccessAt <= ACTIVE_CATALOG_WINDOW_MS
            ),
            lastAccessAt: activity?.lastAccessAt
                ? new Date(activity.lastAccessAt).toISOString()
                : null
        };
    });

    return res.json({
        configuration: {
            catalogFreshSeconds: Math.round(CATALOG_FRESH_MS / 1000),
            detailFreshSeconds: Math.round(DETAIL_FRESH_MS / 1000),
            refreshScanSeconds: Math.round(CACHE_REFRESH_SCAN_MS / 1000),
            activeWindowSeconds: Math.round(
                ACTIVE_CATALOG_WINDOW_MS / 1000
            )
        },
        catalogs,
        pendingCatalogRefreshes: pendingSearches.size,
        detailEntries: detailCache.keys().length,
        pendingDetailRefreshes: pendingDetails.size,
        subscribers: cacheEventSubscribers.size
    });
});

router.post('/cache/refresh', verificarAdminCache, async (req, res) => {
    const tipoProp = limpiarValor(req.body?.tipo_prop || req.query?.tipo_prop);
    const obj = normalizarObjetivo(req.body?.obj || req.query?.obj);
    const comuna = limpiarValor(req.body?.comuna || req.query?.comuna);
    const destaq = limpiarValor(req.body?.destaq || req.query?.destaq);

    if (tipoProp) {
        const catalogQuery = { tipo_prop: tipoProp };
        if (comuna) catalogQuery.comuna = comuna;
        if (destaq) catalogQuery.destaq = destaq;

        try {
            const result = await descargarCatalogosPorObjetivo(
                catalogQuery,
                obj,
                { forceRefresh: true }
            );

            return res.json({
                success: true,
                refreshed: 1,
                objetivo:
                    normalizarObjetivo(obj) || '3',
                catalogKeys: result.catalogKeys,
                items: result.items.length,
                updatedAt: new Date(result.updatedAt).toISOString()
            });
        } catch (error) {
            return res.status(502).json({
                success: false,
                error: error.message
            });
        }
    }

    const refreshed = await refrescarCatalogosActivos({ force: true });

    return res.json({
        success: true,
        refreshed
    });
});

router.post('/cache/clear', verificarAdminCache, (req, res) => {
    searchCache.flushAll();
    staleSearchCache.flushAll();
    detailCache.flushAll();
    activeCatalogs.clear();

    return res.json({
        success: true,
        message: 'Caché eliminada.'
    });
});

// Búsqueda por código comercial.
router.get('/codigo/:codigo', async (req, res) => {
    const codigo = limpiarValor(req.params.codigo);
    const totalStartedAt = performance.now();

    if (!codigo) {
        return res.status(400).json({
            error: 'Código inválido.'
        });
    }

    const codeCacheKey = `detail:code:${codigo}`;
    const cached = detailCache.get(codeCacheKey);

    if (cached) {
        const stale = obtenerEdadMs(cached) > DETAIL_FRESH_MS;

        if (stale) {
            iniciarActualizacionDetallePorCodigo(codigo).catch((error) => {
                console.error(
                    `No fue posible refrescar código ${codigo}:`,
                    error.message
                );
            });
        }

        return res.json({
            ...cached.propiedad,
            meta: {
                cache: true,
                cacheStale: stale,
                actualizandoCache: stale,
                cacheAgeSeconds: Math.round(obtenerEdadMs(cached) / 1000),
                cacheUpdatedAt: new Date(cached.updatedAt).toISOString(),
                tiempoTotalMs: Math.round(
                    performance.now() - totalStartedAt
                )
            }
        });
    }

    try {
        const propiedad = await iniciarActualizacionDetallePorCodigo(codigo);

        if (!propiedad) {
            return res.status(404).json({
                error: 'Propiedad no encontrada.'
            });
        }

        return res.json({
            ...propiedad,
            meta: {
                cache: false,
                cacheStale: false,
                actualizandoCache: false,
                cacheAgeSeconds: 0,
                tiempoTotalMs: Math.round(
                    performance.now() - totalStartedAt
                )
            }
        });
    } catch (error) {
        console.error(
            `Error buscando código ${codigo}:`,
            error.response?.data || error.message
        );

        return res.status(502).json({
            error: 'No fue posible consultar la propiedad.'
        });
    }
});

// Ruta individual por ID interno. Debe ir al final.
router.get('/:id', async (req, res) => {
    const id = limpiarValor(req.params.id);

    try {
        const detailResult = await consultarDetallePorId(id);

        if (!detailResult.propiedad) {
            return res.status(404).json({
                error: 'Propiedad no encontrada.'
            });
        }

        res.setHeader(
            'X-Cache-Source',
            detailResult.cacheHit
                ? detailResult.stale
                    ? 'stale-cache'
                    : 'fresh-cache'
                : 'api'
        );
        res.setHeader(
            'X-Cache-Updating',
            String(Boolean(detailResult.refreshing))
        );

        return res.json(detailResult.propiedad);
    } catch (error) {
        console.error(
            `Error obteniendo propiedad ${id}:`,
            error.response?.data || error.message
        );

        return res.status(502).json({
            error: 'No fue posible consultar la propiedad.'
        });
    }
});

const cacheService = {
    marcarCatalogosComoDesactualizados,
    refrescarCatalogosActivos,
    invalidateAllDetails: () => detailCache.flushAll(),
    invalidateDetail: (value) => {
        const normalized = limpiarValor(value);
        if (!normalized) return 0;

        let deleted = 0;
        for (const key of [
            `detail:id:${normalized}`,
            `detail:code:${normalized}`,
            `detail:lookup:${normalized}`
        ]) {
            if (detailCache.del(key)) deleted += 1;
        }
        return deleted;
    },
    getStatus: () => ({
        catalogs: searchCache.keys().length,
        details: detailCache.keys().length,
        pendingCatalogs: pendingSearches.size,
        pendingDetails: pendingDetails.size
    })
};

router.cacheService = cacheService;

module.exports = router;
