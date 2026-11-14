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
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(CuarentenaController.class)
@Import(SeguridadConfig.class)
class CuarentenaControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean ConsultaCuarentenaRepository repository;
    @MockitoBean JwtDecoder decoder;

    @Test void soloAdminLeeMetadatosYNoPuedeModificar() throws Exception {
        String id = "a".repeat(64);
        var metadata = new CuarentenaMetadata(id, "negocio", "shard", "123", 2, "EVENTO_INVALIDO", Instant.now(), null, null);
        when(repository.listar(eq("EVENTO_INVALIDO"), eq(0), eq(1))).thenReturn(List.of(metadata, metadata));
        when(repository.buscar(id)).thenReturn(Optional.of(metadata));
        var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
        mvc.perform(get("/api/auditoria/cuarentena")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/auditoria/cuarentena").with(jwt().authorities(new SimpleGrantedAuthority("ROLE_CLIENTE"))))
                .andExpect(status().isForbidden());
        mvc.perform(get("/api/auditoria/cuarentena").param("tipo", "EVENTO_INVALIDO").param("tamano", "1").with(admin))
                .andExpect(status().isOk()).andExpect(jsonPath("$.hayMas").value(true))
                .andExpect(jsonPath("$.elementos[0].id").value(id)).andExpect(jsonPath("$.elementos[0].payloadBase64").doesNotExist());
        mvc.perform(get("/api/auditoria/cuarentena/" + id).with(admin)).andExpect(status().isOk())
                .andExpect(jsonPath("$.payloadBase64").doesNotExist());
        mvc.perform(post("/api/auditoria/cuarentena/" + id).with(admin)).andExpect(status().isForbidden());
        mvc.perform(delete("/api/auditoria/cuarentena/" + id).with(admin)).andExpect(status().isForbidden());
    }

    @Test void parametrosInvalidosYAusentesSonRechazados() throws Exception {
        var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
        mvc.perform(get("/api/auditoria/cuarentena").param("tipo", "otro").with(admin)).andExpect(status().isBadRequest());
        mvc.perform(get("/api/auditoria/cuarentena").param("tamano", "101").with(admin)).andExpect(status().isBadRequest());
        mvc.perform(get("/api/auditoria/cuarentena/no-es-id").with(admin)).andExpect(status().isBadRequest());
        mvc.perform(get("/api/auditoria/cuarentena/" + "b".repeat(64)).with(admin)).andExpect(status().isNotFound());
    }
}
