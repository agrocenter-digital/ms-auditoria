package com.agrocenter.auditoria.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class SeparacionRolesConfigTest {
    @Test void exigeCredencialesYRechazaMismoUsuario() {
        var env = new MockEnvironment().withProperty("spring.datasource.username", "runtime");
        assertThatThrownBy(() -> SeparacionRolesConfig.validar(env)).isInstanceOf(IllegalStateException.class);
        env.setProperty("spring.flyway.user", "runtime");
        env.setProperty("spring.flyway.password", "secret-test");
        assertThatThrownBy(() -> SeparacionRolesConfig.validar(env)).isInstanceOf(IllegalStateException.class);
        env.setProperty("spring.flyway.user", "migrador");
        assertThatCode(() -> SeparacionRolesConfig.validar(env)).doesNotThrowAnyException();
        env.setProperty("spring.flyway.enabled", "false");
        env.setProperty("spring.flyway.user", "runtime");
        assertThatCode(() -> SeparacionRolesConfig.validar(env)).doesNotThrowAnyException();
    }
}
