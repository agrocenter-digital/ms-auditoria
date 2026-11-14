package com.agrocenter.auditoria.security;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class SeguridadConfig {
    @Bean SecurityFilterChain securityFilterChain(HttpSecurity http, JwtAuthenticationConverter converter) throws Exception {
        return http.csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(a -> a
                        .requestMatchers(HttpMethod.GET, "/actuator/health").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/auditoria/eventos", "/api/auditoria/eventos/*").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.GET, "/api/auditoria/cuarentena", "/api/auditoria/cuarentena/*", "/api/auditoria/estado-consumidor").hasRole("ADMIN")
                        .anyRequest().denyAll())
                .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(converter))).build();
    }

    @Bean JwtAuthenticationConverter auditoriaJwtConverter() {
        var converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(jwt -> {
            var roles = new LinkedHashSet<GrantedAuthority>();
            var groups = jwt.getClaimAsStringList("cognito:groups");
            if (groups != null) groups.forEach(group -> addAdmin(roles, group));
            return roles;
        });
        return converter;
    }

    private static void addAdmin(LinkedHashSet<GrantedAuthority> roles, String role) {
        if (role != null && "ADMIN".equals(role.trim().toUpperCase(Locale.ROOT))) roles.add(new SimpleGrantedAuthority("ROLE_ADMIN"));
    }

    @Bean JwtDecoder auditoriaJwtDecoder(@Value("${COGNITO_ISSUER_URI:}") String issuer,
                                         @Value("${COGNITO_JWK_SET_URI:}") String jwks,
                                         @Value("${COGNITO_AUDIENCE:}") String audience) {
        if (issuer.isBlank() || jwks.isBlank() || audience.isBlank())
            throw new IllegalStateException("Se requieren COGNITO_ISSUER_URI, COGNITO_JWK_SET_URI y COGNITO_AUDIENCE");
        var decoder = NimbusJwtDecoder.withJwkSetUri(jwks).jwsAlgorithm(SignatureAlgorithm.RS256).build();
        var validators = new ArrayList<OAuth2TokenValidator<Jwt>>();
        validators.add(JwtValidators.createDefaultWithIssuer(issuer));
        validators.add(jwt -> "access".equals(jwt.getClaimAsString("token_use"))
                ? OAuth2TokenValidatorResult.success() : invalid());
        validators.add(jwt -> (jwt.getAudience() != null && jwt.getAudience().contains(audience))
                || audience.equals(jwt.getClaimAsString("client_id"))
                ? OAuth2TokenValidatorResult.success() : invalid());
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(validators));
        return decoder;
    }

    private static OAuth2TokenValidatorResult invalid() {
        return OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token"));
    }
}
