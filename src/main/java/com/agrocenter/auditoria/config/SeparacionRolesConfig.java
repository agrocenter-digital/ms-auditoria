package com.agrocenter.auditoria.config;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

@Configuration
@Profile("prod")
public class SeparacionRolesConfig {
    /** Se valida antes de crear el DataSource o ejecutar Flyway. */
    @Bean
    static BeanFactoryPostProcessor validarRolesAuditoria(Environment environment) {
        return beanFactory -> validar(environment);
    }

    static void validar(Environment environment) {
        if (!environment.getProperty("spring.flyway.enabled", Boolean.class, true)) return;
        String migrador = environment.getProperty("spring.flyway.user");
        String password = environment.getProperty("spring.flyway.password");
        String runtime = environment.getProperty("spring.datasource.username");
        if (migrador == null || migrador.isBlank() || password == null || password.isBlank()
                || runtime == null || runtime.isBlank())
            throw new IllegalStateException("Se requieren credenciales de migracion y de ejecucion para auditoria");
        if (migrador.equals(runtime))
            throw new IllegalStateException("Auditoria requiere usuarios de migracion y ejecucion distintos");
    }
}
