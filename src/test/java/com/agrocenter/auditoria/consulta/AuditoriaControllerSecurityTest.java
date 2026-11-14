package com.agrocenter.auditoria.consulta;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.agrocenter.auditoria.security.SeguridadConfig;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(AuditoriaController.class)
@Import(SeguridadConfig.class)
class AuditoriaControllerSecurityTest {
    @Autowired MockMvc mvc;
    @MockitoBean ConsultaAuditoriaRepository repository;
    @MockitoBean JwtDecoder decoder;

    @Test void sinTokenOClienteNoAcceden() throws Exception {
        mvc.perform(get("/api/auditoria/eventos")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/auditoria/eventos").with(jwt().authorities(new SimpleGrantedAuthority("ROLE_CLIENTE"))))
                .andExpect(status().isForbidden());
        verifyNoInteractions(repository);
    }

    @Test void adminPuedeConsultarPeroNoModificar() throws Exception {
        var evento = new EventoAuditado("evento-1", Instant.parse("2026-11-12T22:00:00Z"), Instant.now(),
                "VentaConfirmada", "ms-ventas", "Venta", "1", "trace-1", 1, "negocio", "shard", "123", 0);
        when(repository.listar(any(), any(), any(), any(), any(), eq(0), eq(1))).thenReturn(List.of(evento, evento));
        when(repository.buscar("evento-1")).thenReturn(Optional.of(evento));
        var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
        mvc.perform(get("/api/auditoria/eventos").param("tamano", "1").with(admin))
                .andExpect(status().isOk()).andExpect(jsonPath("$.elementos[0].eventId").value("evento-1"))
                .andExpect(jsonPath("$.elementos[0].payload").doesNotExist()).andExpect(jsonPath("$.hayMas").value(true));
        mvc.perform(get("/api/auditoria/eventos/evento-1").with(admin)).andExpect(status().isOk())
                .andExpect(jsonPath("$.payload").doesNotExist());
        mvc.perform(post("/api/auditoria/eventos").with(admin)).andExpect(status().isForbidden());
        mvc.perform(patch("/api/auditoria/eventos/evento-1").with(admin)).andExpect(status().isForbidden());
        mvc.perform(delete("/api/auditoria/eventos/evento-1").with(admin)).andExpect(status().isForbidden());
    }

    @Test void grupoCognitoAdminSeConvierteEnRolYCustomRoleNoEscala() throws Exception {
        Jwt grupo = Jwt.withTokenValue("grupo").header("alg", "RS256").claim("sub", "u")
                .claim("cognito:groups", List.of("ADMIN")).build();
        Jwt atributo = Jwt.withTokenValue("atributo").header("alg", "RS256").claim("sub", "u")
                .claim("custom:role", "ADMIN").build();
        when(decoder.decode("grupo")).thenReturn(grupo);
        when(decoder.decode("atributo")).thenReturn(atributo);
        when(repository.listar(any(), any(), any(), any(), any(), eq(0), eq(25))).thenReturn(List.of());
        mvc.perform(get("/api/auditoria/eventos").header("Authorization", "Bearer grupo")).andExpect(status().isOk());
        mvc.perform(get("/api/auditoria/eventos").header("Authorization", "Bearer atributo")).andExpect(status().isForbidden());
    }

    @Test void limitesYFechaInvalidaSeRechazan() throws Exception {
        var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
        mvc.perform(get("/api/auditoria/eventos").param("tamano", "101").with(admin)).andExpect(status().isBadRequest());
        mvc.perform(get("/api/auditoria/eventos").param("desde", "2026-11-13T00:00:00Z")
                .param("hasta", "2026-11-12T00:00:00Z").with(admin)).andExpect(status().isBadRequest());
        mvc.perform(get("/api/auditoria/eventos/ausente").with(admin)).andExpect(status().isNotFound());
    }
}
