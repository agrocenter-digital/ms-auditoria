-- Ejecutar solo después de Flyway, en la BD correcta y con un administrador de BD.
-- Uso: psql ... -v runtime_role=nombre_del_rol -f permisos_runtime.sql
-- El rol debe existir; este script no crea usuarios ni contraseñas.
\set ON_ERROR_STOP on
\if :{?runtime_role}
\else
    \echo 'Falta -v runtime_role=nombre_del_rol'
    \quit 3
\endif

GRANT USAGE ON SCHEMA public TO :"runtime_role";
REVOKE ALL PRIVILEGES ON TABLE public.eventos_auditoria, public.eventos_cuarentena FROM :"runtime_role";
GRANT SELECT, INSERT ON TABLE public.eventos_auditoria, public.eventos_cuarentena TO :"runtime_role";
