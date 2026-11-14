package com.agrocenter.auditoria.operacion;

import com.agrocenter.auditoria.AuditoriaApplication;
import com.agrocenter.auditoria.eventos.ResolucionCuarentenaService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

/** Herramienta local separada. Nunca arranca HTTP, consumidores KCL ni Flyway. */
public final class ResolucionCommand {
    private ResolucionCommand() { }

    public static void main(String[] args) throws Exception {
        ejecutar(args);
        System.out.println("Resolucion registrada. El consumidor permanece detenido; valide el resultado antes de reiniciarlo.");
    }

    static void ejecutar(String[] args) throws Exception {
        Entrada entrada = Entrada.parse(args);
        if (!Files.isRegularFile(entrada.archivo()) || Files.size(entrada.archivo()) > 1024 * 1024)
            throw new IllegalArgumentException("El archivo corregido debe existir y no superar 1 MiB");
        byte[] payload = Files.readAllBytes(entrada.archivo());
        if (payload.length == 0 || payload.length > 1024 * 1024)
            throw new IllegalArgumentException("El archivo corregido no debe estar vacio ni superar 1 MiB");
        String url = requerirVariable("DB_URL");
        String usuario = requerirVariable("DB_USERNAME");
        String password = requerirVariable("DB_PASSWORD");
        String usuarioRuntimeConfigurado = requerirVariable("AUDIT_CONSUMER_DB_USERNAME");

        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(AuditoriaApplication.class)
                .web(WebApplicationType.NONE).profiles("auditoria-resolucion")
                .initializers(application -> application.getEnvironment().getPropertySources().addFirst(
                        new MapPropertySource("resolucionAislada", Map.of(
                                "spring.main.web-application-type", "none",
                                "spring.flyway.enabled", "false",
                                "spring.datasource.url", url,
                                "spring.datasource.username", usuario,
                                "spring.datasource.password", password,
                                "agrocenter.auditoria.kinesis.enabled", "false",
                                "agrocenter.auditoria.resolucion.enabled", "true"))))
                .run()) {
            String usuarioRuntime = usuarioRuntimeConfigurado;
            JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
            String operador = jdbc.queryForObject("SELECT current_user", String.class);
            Boolean privilegiado = jdbc.queryForObject("SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls FROM pg_roles WHERE rolname = current_user", Boolean.class);
            if (usuarioRuntime == null || usuarioRuntime.isBlank() || operador == null || operador.equals(usuarioRuntime)
                    || Boolean.TRUE.equals(privilegiado))
                throw new IllegalStateException("Se requiere un rol operador no superusuario distinto del rol consumidor");
            Boolean propietario = jdbc.queryForObject("""
                    SELECT EXISTS (SELECT 1 FROM pg_class
                      WHERE oid IN (to_regclass('public.eventos_auditoria'), to_regclass('public.eventos_cuarentena'))
                        AND relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user))
                    """, Boolean.class);
            if (Boolean.TRUE.equals(propietario)) throw new IllegalStateException("El rol operador no puede ser propietario de las tablas");
            for (String tabla : new String[] {"eventos_auditoria", "eventos_cuarentena"}) {
                for (String permiso : new String[] {"DELETE", "TRUNCATE"}) {
                    if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT has_table_privilege(current_user, ?, ?)", Boolean.class, tabla, permiso)))
                        throw new IllegalStateException("El rol operador tiene permisos excesivos");
                }
            }
            if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT has_any_column_privilege(current_user, 'eventos_auditoria', 'UPDATE')", Boolean.class)))
                throw new IllegalStateException("El rol operador no puede modificar trazas de auditoria");
            for (String columna : new String[] {"id", "stream", "shard", "sequence_number", "sub_sequence_number", "payload_base64", "tipo_error", "created_at"}) {
                if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT has_column_privilege(current_user, 'eventos_cuarentena', ?, 'UPDATE')", Boolean.class, columna)))
                    throw new IllegalStateException("El rol operador no puede modificar el registro original");
            }
            context.getBean(ResolucionCuarentenaService.class).resolver(entrada.id(), "db:" + operador, entrada.motivo(), payload);
        }
    }

    private static String requerirVariable(String nombre) {
        String valor = System.getProperty(nombre, System.getenv(nombre));
        if (valor == null || valor.isBlank()) throw new IllegalStateException("Se requiere " + nombre + " explicita");
        return valor;
    }

    record Entrada(String id, Path archivo, String motivo) {
        static Entrada parse(String[] args) {
            if (args.length != 4 || !"--confirmo-consumidores-detenidos".equals(args[0]))
                throw new IllegalArgumentException("Uso: --confirmo-consumidores-detenidos <id> <archivo-json> <motivo>");
            if (!args[1].matches("[0-9a-f]{64}") || args[3].isBlank() || args[3].length() > 500)
                throw new IllegalArgumentException("Identificador o motivo invalidos");
            return new Entrada(args[1], Path.of(args[2]), args[3]);
        }
    }
}
