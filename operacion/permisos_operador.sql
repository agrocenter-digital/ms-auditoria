-- Ejecutar solo después de Flyway, en la BD correcta y con un administrador de BD.
-- Uso: psql ... -v operator_role=nombre_del_rol -f permisos_operador.sql
-- La cuenta operadora debe ser diferente del rol runtime y no ser propietaria ni superusuaria.
-- No concede al runtime privilegios de corrección.
\set ON_ERROR_STOP on
\if :{?operator_role}
\else
    \echo 'Falta -v operator_role=nombre_del_rol'
    \quit 3
\endif

GRANT USAGE ON SCHEMA public TO :"operator_role";
REVOKE ALL PRIVILEGES ON TABLE public.eventos_auditoria, public.eventos_cuarentena FROM :"operator_role";
GRANT SELECT, INSERT ON TABLE public.eventos_auditoria TO :"operator_role";
GRANT SELECT ON TABLE public.eventos_cuarentena TO :"operator_role";
GRANT UPDATE (resuelto_en, resuelto_por, motivo_resolucion, event_id_corregido)
    ON TABLE public.eventos_cuarentena TO :"operator_role";
