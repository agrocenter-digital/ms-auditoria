package com.agrocenter.auditoria.consulta;

import java.time.Instant;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/auditoria/eventos")
public class AuditoriaController {
    private final ConsultaAuditoriaRepository repository;
    public AuditoriaController(ConsultaAuditoriaRepository repository) { this.repository = repository; }

    @GetMapping
    public Pagina listar(@RequestParam(required = false) Instant desde, @RequestParam(required = false) Instant hasta,
                         @RequestParam(required = false) String tipo, @RequestParam(required = false) String productor,
                         @RequestParam(required = false) String correlacion,
                         @RequestParam(defaultValue = "0") int pagina, @RequestParam(defaultValue = "25") int tamano) {
        if (pagina < 0 || pagina > 10000 || tamano < 1 || tamano > 100 || (desde != null && hasta != null && !desde.isBefore(hasta)))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parametros de consulta invalidos");
        validarFiltro(tipo); validarFiltro(productor); validarFiltro(correlacion);
        var resultados = repository.listar(desde, hasta, tipo, productor, correlacion, pagina, tamano);
        return new Pagina(List.copyOf(resultados.subList(0, Math.min(resultados.size(), tamano))), pagina, tamano, resultados.size() > tamano);
    }

    @GetMapping("/{eventId}")
    public EventoAuditado obtener(@PathVariable String eventId) {
        validarFiltro(eventId);
        return repository.buscar(eventId).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    private static void validarFiltro(String valor) {
        if (valor != null && (valor.isBlank() || valor.length() > 100))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Filtro invalido");
    }

    public record Pagina(List<EventoAuditado> elementos, int pagina, int tamano, boolean hayMas) { }
}
