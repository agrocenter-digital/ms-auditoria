package com.agrocenter.auditoria.eventos;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Ejecutar exclusivamente sobre una base PostgreSQL temporal vacía y dedicada. */
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "CONTRATOS_DIR", matches = ".+")
class ContratoProductoresPostgresTest {
    @Autowired AuditoriaProcessor processor;
    @Autowired JdbcTemplate jdbc;
    @MockitoBean JwtDecoder decoder;

    @Test void auditaSobresRealesSinDuplicarlos() throws Exception {
        var dir = Path.of(System.getenv("CONTRATOS_DIR"));
        var venta = Files.readAllBytes(dir.resolve("venta.json"));
        var stock = Files.readAllBytes(dir.resolve("stock.json"));
        assertThat(processor.procesar("negocio", "shard", "1", 0, venta)).isTrue();
        assertThat(processor.procesar("negocio", "shard", "1", 0, venta)).isFalse();
        assertThat(processor.procesar("stock", "shard", "2", 0, stock)).isTrue();
        assertThat(processor.procesar("stock", "shard", "2", 0, stock)).isFalse();
        int secuencia = 3;
        for (var archivo : java.util.List.of("b2b-creada.json", "b2b-cambio.json", "CompraCreada.json", "CompraConfirmada.json", "CompraCompletada.json", "CompraCancelada.json", "ReabastecimientoSolicitado.json")) {
            var bytes = Files.readAllBytes(dir.resolve(archivo));
            String numero = Integer.toString(secuencia++);
            assertThat(processor.procesar("negocio", "shard", numero, 0, bytes)).isTrue();
            assertThat(processor.procesar("negocio", "shard", numero, 0, bytes)).isFalse();
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isEqualTo(9);
    }
}
