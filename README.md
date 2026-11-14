# ms-auditoria (implementación parcial)

Base local del servicio de auditoría. Valida sobres de eventos y guarda en PostgreSQL una traza por `eventId`; detecta reutilización de un ID con contenido diferente. Incluye dos consumidores KCL independientes, **desactivados por defecto**, todavía sin validar contra AWS. La API local es de solo lectura y el servicio aún no está listo para desplegar.

`GET /api/auditoria/eventos` ofrece filtros opcionales `desde` (incluyente), `hasta` (excluyente), `tipo`, `productor`, `correlacion`, `pagina` y `tamano` (máximo 100). `GET /api/auditoria/eventos/{eventId}` consulta un evento. Se exponen solo metadatos, no el payload. Ambos endpoints requieren access token Cognito firmado RS256 con issuer y cliente/audiencia configurados y pertenencia al grupo `ADMIN` de `cognito:groups`; `custom:role` por sí solo no concede acceso. Esto debe comprobarse con los tokens reales antes del despliegue. Las rutas mutables y demás rutas están denegadas.

`GET /api/auditoria/cuarentena` y `GET /api/auditoria/cuarentena/{id}` permiten al mismo grupo ADMIN consultar metadatos de incidentes, incluido si existe una corrección registrada. El listado admite `tipo` (`EVENTO_INVALIDO` o `COLISION_ID`), `pagina` y `tamano` (máximo 100). No se devuelve el contenido original y no existe una operación HTTP de resolución o replay.

El arranque requiere `COGNITO_ISSUER_URI`, `COGNITO_JWK_SET_URI` y `COGNITO_AUDIENCE`; no admite valores vacíos. Para las pruebas de integración se simula el decoder JWT; las pruebas de seguridad también verifican firmas y claims con un JWKS temporal local. La configuración real de Cognito y el BFF siguen pendientes; no cambiarlos en AWS hasta coordinar el despliegue.

Los consumidores usan por defecto `agrocenter-auditoria-negocio` y `agrocenter-auditoria-stock` como aplicaciones KCL separadas. Un fallo transitorio al procesar se reintenta sin avanzar checkpoint. Si el evento es inválido o presenta colisión de ID, se guarda su contenido original en `eventos_cuarentena` y se detiene el consumidor **sin avanzar el checkpoint**; si falla la escritura en cuarentena también se reintenta sin confirmar. Reiniciarlo sin resolver el registro lo volverá a bloquear. Un fallo de checkpoint detiene ese consumidor sin recuperación automática. Aún no hay interfaz o procedimiento de revisión/replay ni alertas; una detención prolongada puede exceder las 24 horas de retención del stream. No activar hasta resolver esas brechas y verificar costos y permisos de DynamoDB.

Los checkpoints se reintentan hasta tres veces ante excepciones que KCL clasifica como recuperables, tanto tras procesar un lote como al finalizar un shard. El reintento del checkpoint no repite el efecto de auditoría. Si se agotan los intentos o se pierde el lease, el consumidor se detiene; el reinicio externo y la recuperación real aún deben validarse contra AWS.

La lógica interna de resolución permanece desactivada por defecto (`AUDIT_RESOLUTION_ENABLED=false`) y sin endpoint HTTP. Registra una corrección nueva junto con operador y motivo en una transacción. En la relectura solo se omite el registro cuando coincide byte por byte con el original en cuarentena y su corrección sigue existiendo en auditoría. La herramienta separada `ResolucionCommand` inicia esa operación sin HTTP, Flyway ni KCL y obtiene la identidad del operador desde `current_user` de PostgreSQL, no de un argumento libre. Usa un rol individual restringido distinto del consumidor; el rol runtime sigue sin permiso de actualización. La confirmación de consumidores detenidos es una declaración operativa, no una comprobación automática. No conceder UPDATE al runtime ni habilitar resolución en el servicio normal. La consulta adicional a BD por registro requiere medir su impacto. El procedimiento y las limitaciones están en [operacion/recuperacion-cuarentena.md](operacion/recuperacion-cuarentena.md); aún no se ha probado la recuperación con KCL real.

`GET /actuator/health` expone únicamente el estado general, sin detalles. Con KCL habilitado, ambos consumidores deben tener lecturas recientes y sin retrasos elevados para indicar UP; los umbrales predeterminados son dos minutos sin lecturas y una hora de retraso, configurables mediante `KINESIS_MAX_SILENCE_MS` y `KINESIS_MAX_LAG_MS`. La señal es local a una instancia y no reemplaza alarmas externas. Con KCL deshabilitado no se crea el indicador KCL: un UP de la aplicación **no significa que se estén consumiendo eventos**. La ausencia de un shard se considera no saludable en este diseño de una sola instancia; revisar antes de escalar.

Las tablas de auditoría y cuarentena contienen el payload completo, que puede incluir datos personales: Base64 no es cifrado. No usar datos reales hasta definir controles de acceso, retención, redacción y separación de roles de migración/ejecución. Las pruebas de PostgreSQL usan un usuario con permisos de borrado en una base temporal; eso no es una configuración de producción. La tabla no garantiza inmutabilidad frente a administradores de base de datos.

En el perfil `prod`, Flyway usa `MIGRATION_DB_USERNAME` y `MIGRATION_DB_PASSWORD`, mientras la aplicación usa `DB_USERNAME` y `DB_PASSWORD`. Los usuarios deben ser distintos y apuntar a la misma base de auditoría de EC2; la aplicación rechaza usuarios iguales o credenciales de migración vacías antes de crear conexiones. El script `operacion/permisos_runtime.sql` concede `SELECT`/`INSERT` al rol de ejecución sobre las tablas, después de aplicar las migraciones. Es una plantilla para coordinar durante el despliegue: no crea usuarios, no evita permisos heredados de otros roles ni revoca derechos de PUBLIC. El rol runtime no debe ser dueño de las tablas, superusuario ni miembro de roles privilegiados; se deben comprobar esos puntos y los permisos efectivos en la EC2 antes de habilitar el servicio.

## Pruebas

El workflow `Verificacion manual ms-auditoria` se puede ejecutar manualmente desde GitHub Actions una vez creado el repositorio. Ejecuta las pruebas disponibles sin PostgreSQL externo y construye una imagen solo en el runner; no publica a Docker Hub ni despliega en EC2. Las pruebas que requieren PostgreSQL se omiten y deben validarse por separado. El workflow aún no se ha ejecutado en GitHub.

`docker build -t ms-auditoria:local .` construye la imagen Java 21 sin usuario root. El healthcheck usa el puerto interno 8086; si se cambia `SERVER_PORT`, debe ajustarse también el healthcheck del despliegue. KCL está desactivado por defecto y `UP` no demuestra consumo. La configuración real de BD y Cognito sigue pendiente; no desplegar aún.

`mvn test` ejecuta las pruebas unitarias; la prueba de integración PostgreSQL se omite salvo que `AUDIT_PG_TEST=true`. Para ejecutarla se requiere una **base temporal dedicada**, porque la prueba borra filas de `eventos_auditoria`:

```sh
AUDIT_PG_TEST=true DB_URL=jdbc:postgresql://localhost:5432/auditoria_test \
  DB_USERNAME=postgres DB_PASSWORD=localtest mvn test
```

No apuntar estas pruebas a una base con datos que se quieran conservar.

La prueba `RolRuntimePostgresIntegrationTest` se activa únicamente con `AUDIT_RESTRICTED_PG_TEST=true`: necesita una base temporal migrada, un usuario temporal `auditoria_runtime_test` con el script de permisos aplicado y el perfil `prod` con credenciales de migración distintas. No apuntarla a la EC2 ni a datos reales.

La prueba `ResolucionCommandPostgresTest` se activa con `AUDIT_OPERATOR_PG_TEST=true`: exige una BD temporal local `auditoria_test`, credenciales de operador `auditoria_operator_test` y `AUDIT_CONSUMER_DB_USERNAME` con el nombre del rol consumidor. Su preparación administrativa usa `postgres/localtest` únicamente para esa BD de pruebas y limpia sus propios registros; no ejecutar con otros entornos. La suite normal omite las pruebas de operador y runtime restringido salvo activación explícita.
