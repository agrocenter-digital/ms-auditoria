package com.agrocenter.auditoria.eventos;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuditoriaProcessor {
    private final ObjectMapper mapper;
    private final AuditoriaRepository repository;
    public AuditoriaProcessor(ObjectMapper mapper, AuditoriaRepository repository) {
        this.mapper = mapper; this.repository = repository;
    }

    @Transactional
    public boolean procesar(String stream, String shard, String sequence, long subSequence, byte[] bytes) {
        validarMetadata(stream, 128); validarMetadata(shard, 128); validarMetadata(sequence, 128);
        if (subSequence < 0 || bytes == null) throw new EventoInvalidoException("Metadatos de registro invalidos");
        try {
            JsonNode n = mapper.readTree(bytes);
            if (n == null || !n.isObject() || !n.path("payload").isObject()
                    || !n.path("schemaVersion").isIntegralNumber() || !n.path("schemaVersion").canConvertToInt()
                    || n.path("schemaVersion").intValue() < 1)
                throw new EventoInvalidoException("Sobre de evento invalido");
            String id = texto(n, "eventId", 100);
            String tipo = texto(n, "eventType", 100);
            String productor = texto(n, "producer", 100);
            String agregado = texto(n, "aggregateType", 100);
            String agregadoId = texto(n, "aggregateId", 100);
            String correlacion = n.hasNonNull("correlationId") ? texto(n, "correlationId", 100) : null;
            Instant occurredAt = Instant.parse(texto(n, "occurredAt", 100));
            var evento = new RegistroAuditoria(id, occurredAt, Instant.now(), tipo, productor, agregado,
                    agregadoId, correlacion, n.path("schemaVersion").intValue(), stream, shard, sequence,
                    subSequence, mapper.writeValueAsString(n.path("payload")));
            return repository.insertar(evento);
        } catch (java.io.IOException | DateTimeParseException e) {
            throw new EventoInvalidoException("JSON o fecha invalida", e);
        }
    }

    private static String texto(JsonNode node, String campo, int max) {
        JsonNode valor = node.path(campo);
        if (!valor.isTextual() || valor.textValue().isBlank() || valor.textValue().length() > max)
            throw new EventoInvalidoException("Campo invalido: " + campo);
        return valor.textValue();
    }

    private static void validarMetadata(String value, int max) {
        if (value == null || value.isBlank() || value.length() > max)
            throw new EventoInvalidoException("Metadatos de registro invalidos");
    }
}
