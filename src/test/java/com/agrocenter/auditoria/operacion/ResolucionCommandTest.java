package com.agrocenter.auditoria.operacion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.nio.file.Files;

class ResolucionCommandTest {
    @TempDir Path temp;

    @Test void archivoAusenteVacioOGrandeSeRechazaAntesDeConectar() throws Exception {
        String id = "a".repeat(64);
        Path archivo = temp.resolve("evento.json");
        String[] args = {"--confirmo-consumidores-detenidos", id, archivo.toString(), "revisado"};
        assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalArgumentException.class);
        Files.writeString(archivo, "");
        assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalArgumentException.class);
        Files.write(archivo, new byte[1024 * 1024 + 1]);
        assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void exigeConfirmacionIdYMotivo() {
        String id = "a".repeat(64);
        assertThatThrownBy(() -> ResolucionCommand.Entrada.parse(new String[] {id, "evento.json", "motivo"}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ResolucionCommand.Entrada.parse(new String[] {"--confirmo-consumidores-detenidos", "incorrecto", "evento.json", "motivo"}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ResolucionCommand.Entrada.parse(new String[] {"--confirmo-consumidores-detenidos", id, "evento.json", " "}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(ResolucionCommand.Entrada.parse(new String[] {"--confirmo-consumidores-detenidos", id, "evento.json", "revisado"}).id()).isEqualTo(id);
    }
}
