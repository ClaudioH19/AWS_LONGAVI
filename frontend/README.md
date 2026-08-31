# Frontend BioVision

Interfaz React/Vite del panel meteorológico. Consume la API Flask desde el mismo origen y usa Socket.IO para actualizaciones inmediatas, con polling periódico como reconciliación.

```sh
npm ci
npm run lint
npm run build
```

El frontend de producción se construye dentro del Dockerfile raíz. Las rutas API se mantienen relativas para que el proxy HTTPS de la VPS pueda servir toda la aplicación bajo un solo dominio.
