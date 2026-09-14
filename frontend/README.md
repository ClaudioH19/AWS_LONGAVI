# Frontend BioVision

Interfaz React/Vite del panel meteorológico. Consume la API Flask desde el mismo origen y usa Socket.IO para actualizaciones inmediatas, con polling periódico como reconciliación.

Tailwind v4 se procesa mediante el plugin de Vite y todos los estilos globales
viven en `src/App.css`. Las cargas iniciales usan `boneyard-js`; su configuración
responsive está en `boneyard.config.json`.

```sh
npm ci
npm test
npm run lint
npm run build
```

La vista adapta los gráficos automáticamente: en teléfonos muestra el comparador
seleccionable y desde tablet/escritorio muestra cuatro gráficos fijos de siete
días. Para una pantalla de operación o TV, abrir la URL con `?display=tv` (por
ejemplo, `https://meteo.ejemplo.cl/?display=tv`). Este modo fuerza los cuatro
gráficos y aumenta la escala tipográfica; en resoluciones 4K los distribuye en
una sola fila.

Para regenerar esqueletos pixel-perfect mientras corre `npm run dev`:

```sh
npx boneyard-js build http://127.0.0.1:5173
```

La aplicación mantiene un fallback animado si todavía no existe un registro de
bones generado.

El frontend de producción se construye dentro del Dockerfile raíz. Las rutas API se mantienen relativas para que el proxy HTTPS de la VPS pueda servir toda la aplicación bajo un solo dominio.
