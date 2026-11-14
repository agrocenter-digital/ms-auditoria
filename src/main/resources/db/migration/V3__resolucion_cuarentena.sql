ALTER TABLE eventos_cuarentena ADD COLUMN resuelto_en TIMESTAMP WITH TIME ZONE;
ALTER TABLE eventos_cuarentena ADD COLUMN resuelto_por VARCHAR(200);
ALTER TABLE eventos_cuarentena ADD COLUMN motivo_resolucion VARCHAR(500);
ALTER TABLE eventos_cuarentena ADD COLUMN event_id_corregido VARCHAR(100) REFERENCES eventos_auditoria(event_id);
ALTER TABLE eventos_cuarentena ADD CONSTRAINT ck_resolucion_cuarentena CHECK (
    (resuelto_en IS NULL AND resuelto_por IS NULL AND motivo_resolucion IS NULL AND event_id_corregido IS NULL)
    OR (resuelto_en IS NOT NULL AND resuelto_por IS NOT NULL AND motivo_resolucion IS NOT NULL AND event_id_corregido IS NOT NULL)
);
