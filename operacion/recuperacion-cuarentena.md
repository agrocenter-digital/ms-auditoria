# Recuperación de cuarentena — validación local, no autorizada aún para EC2

Esta operación solo corrige la representación **en auditoría**. No modifica ventas,
compras ni stock, no republica eventos y no repara otros consumidores.

## Condiciones previas

1. Identificar el incidente por su ID, stream, shard, secuencia y subsecuencia.
2. Detener y verificar **todos** los consumidores de auditoría. La bandera del comando
   es una declaración del operador: no comprueba el estado de otras instancias.
3. Revisar el contenido original mediante acceso restringido a BD y preparar el JSON
   corregido en un archivo privado. No pegar datos personales en consola, logs o Git.
4. Conservar ID cuando sea válido y no esté ocupado. Ante una colisión, revisar el
   productor y asignar una identidad nueva aprobada; nunca sobrescribir la traza previa.
5. Preparar un rol operador individual, no compartido con el consumidor, con el script
   `permisos_operador.sql`. No usar el propietario, un superusuario ni roles elevados.
   El script no elimina permisos heredados ni de PUBLIC: comprobar los permisos efectivos.
6. Las migraciones deben estar aplicadas por separado. La herramienta no ejecuta Flyway.

## Ejecución local

Construir la aplicación y escribir su classpath en un archivo local:

```sh
mvn -DskipTests package dependency:build-classpath -Dmdep.outputFile=target/operator-classpath.txt
```

Configurar de manera privada `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` y
`AUDIT_CONSUMER_DB_USERNAME`. El primero debe apuntar explícitamente a la base de
auditoría; `DB_USERNAME` es el operador y el último identifica el rol consumidor real.
No poner contraseñas en argumentos ni guardarlas en archivos versionados.

```sh
java -cp "target/classes:$(cat target/operator-classpath.txt)" \
  com.agrocenter.auditoria.operacion.ResolucionCommand \
  --confirmo-consumidores-detenidos ID_CUARENTENA /ruta/privada/corregido.json \
  'Motivo de la correccion revisada'
```

El comando no levanta HTTP, consumidores KCL ni migraciones. Registra `db:<current_user>`
como operador, conserva el original y guarda corrección y resolución juntas. Rechaza
una corrección inválida, ya auditada o una cuarentena resuelta. Un fallo deja la operación
sin resolver. El motivo es un argumento visible para usuarios locales con acceso a procesos:
no incluir información sensible en él.

## Verificación posterior

- Consultar los metadatos del incidente y la nueva traza con un usuario autorizado.
- Comprobar identidad de operador, motivo y correspondencia de la corrección; una cuenta
  compartida solo identifica el rol de BD, no a la persona. La aprobación humana no está automatizada.
- Solo después, reiniciar auditoría de forma controlada: el callback omite únicamente
  los bytes originales que coinciden con la resolución confirmada. No editar checkpoints
  DynamoDB manualmente.
- Verificar lecturas, retraso, siguiente evento y ausencia de nuevos incidentes.
- Eliminar de forma segura archivos temporales sensibles siguiendo la política acordada.

## Límites pendientes

Se han probado PostgreSQL y callbacks simulados, **no recuperación real en AWS**.
Si el registro expiró de las 24 horas de retención, la corrección de BD no demuestra
que todos los eventos intermedios fueron recuperados: se requiere reconciliación con
S3/outbox y un procedimiento distinto. Una resolución errónea requiere otro procedimiento
de rectificación; no deshacerla borrando o modificando trazas auditadas.
