package com.agrocenter.auditoria.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;

class SeguridadJwtTest {
    private static RSAKey key;
    private static HttpServer server;
    private static JwtDecoder decoder;

    @BeforeAll static void preparar() throws Exception {
        key = new RSAKeyGenerator(2048).keyID("test-key").generate();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/jwks", exchange -> {
            byte[] json = new JWKSet(key.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, json.length);
            try (var out = exchange.getResponseBody()) { out.write(json); }
        });
        server.start();
        decoder = new SeguridadConfig().auditoriaJwtDecoder("https://issuer.example", "http://127.0.0.1:" + server.getAddress().getPort() + "/jwks", "client-123");
    }

    @AfterAll static void detener() { if (server != null) server.stop(0); }

    @Test void validaFirmaIssuerTipoYAudiência() throws Exception {
        assertThat(decoder.decode(token(key, "https://issuer.example", "access", "client-123", Instant.now().plusSeconds(300))).getSubject()).isEqualTo("user");
        assertThatThrownBy(() -> decoder.decode(token(key, "https://otro.example", "access", "client-123", Instant.now().plusSeconds(300))))
                .isInstanceOf(JwtException.class);
        assertThatThrownBy(() -> decoder.decode(token(key, "https://issuer.example", "id", "client-123", Instant.now().plusSeconds(300))))
                .isInstanceOf(JwtException.class);
        assertThatThrownBy(() -> decoder.decode(token(key, "https://issuer.example", "access", "otro", Instant.now().plusSeconds(300))))
                .isInstanceOf(JwtException.class);
        assertThatThrownBy(() -> decoder.decode(token(key, "https://issuer.example", "access", "client-123", Instant.now().minusSeconds(600))))
                .isInstanceOf(JwtException.class);
        RSAKey impostor = new RSAKeyGenerator(2048).keyID("test-key").generate();
        assertThatThrownBy(() -> decoder.decode(token(impostor, "https://issuer.example", "access", "client-123", Instant.now().plusSeconds(300))))
                .isInstanceOf(JwtException.class);
    }

    @Test void configuracionIncompletaNoPermiteCrearDecoder() {
        assertThatThrownBy(() -> new SeguridadConfig().auditoriaJwtDecoder("", "", ""))
                .isInstanceOf(IllegalStateException.class);
    }

    private String token(RSAKey signer, String issuer, String tokenUse, String client, Instant expiry) throws Exception {
        var claims = new JWTClaimsSet.Builder().issuer(issuer).subject("user").expirationTime(Date.from(expiry))
                .claim("token_use", tokenUse).claim("client_id", client).build();
        var jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test-key").build(), claims);
        jwt.sign(new RSASSASigner(signer));
        return jwt.serialize();
    }
}
