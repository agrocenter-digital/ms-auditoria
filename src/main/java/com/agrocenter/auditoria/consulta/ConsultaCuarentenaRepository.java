package com.agrocenter.auditoria.consulta;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ConsultaCuarentenaRepository {
    private static final String COLUMNAS = "id, stream, shard, sequence_number, sub_sequence_number, tipo_error, created_at, resuelto_en, event_id_corregido";
    private final JdbcTemplate jdbc;
    public ConsultaCuarentenaRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public Optional<CuarentenaMetadata> buscar(String id) {
        return jdbc.query("SELECT " + COLUMNAS + " FROM eventos_cuarentena WHERE id = ?", this::mapear, id).stream().findFirst();
    }

    public List<CuarentenaMetadata> listar(String tipo, int pagina, int tamano) {
        String sql = "SELECT " + COLUMNAS + " FROM eventos_cuarentena"
                + (tipo == null ? "" : " WHERE tipo_error = ?")
                + " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?";
        Object[] args = tipo == null ? new Object[] {tamano + 1, (long) pagina * tamano}
                : new Object[] {tipo, tamano + 1, (long) pagina * tamano};
        return jdbc.query(sql, this::mapear, args);
    }

    private CuarentenaMetadata mapear(ResultSet rs, int n) throws SQLException {
        var resuelto = rs.getTimestamp("resuelto_en");
        return new CuarentenaMetadata(rs.getString("id"), rs.getString("stream"), rs.getString("shard"),
                rs.getString("sequence_number"), rs.getLong("sub_sequence_number"), rs.getString("tipo_error"),
                rs.getTimestamp("created_at").toInstant(), resuelto == null ? null : resuelto.toInstant(), rs.getString("event_id_corregido"));
    }
}
