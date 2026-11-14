package com.agrocenter.auditoria.eventos;

import java.time.Instant;

public record RegistroAuditoria(String eventId, Instant occurredAt, Instant receivedAt, String eventType,
        String producer, String aggregateType, String aggregateId, String correlationId, int schemaVersion,
        String stream, String shard, String sequenceNumber, long subSequenceNumber, String payloadJson) { }
