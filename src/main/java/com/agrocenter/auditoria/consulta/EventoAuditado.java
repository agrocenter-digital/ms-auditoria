package com.agrocenter.auditoria.consulta;

import java.time.Instant;

/** Solo metadatos: el payload puede contener datos personales y aún no se expone. */
public record EventoAuditado(String eventId, Instant occurredAt, Instant receivedAt, String eventType,
        String producer, String aggregateType, String aggregateId, String correlationId, int schemaVersion,
        String stream, String shard, String sequenceNumber, long subSequenceNumber) { }
