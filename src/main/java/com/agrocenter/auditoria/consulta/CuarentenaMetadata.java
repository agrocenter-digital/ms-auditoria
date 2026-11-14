package com.agrocenter.auditoria.consulta;

import java.time.Instant;

/** Datos diagnósticos; el contenido original no se expone por API. */
public record CuarentenaMetadata(String id, String stream, String shard, String sequenceNumber,
                                 long subSequenceNumber, String tipoError, Instant createdAt,
                                 Instant resueltoEn, String eventIdCorregido) { }
