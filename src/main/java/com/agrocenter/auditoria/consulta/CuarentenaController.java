package com.agrocenter.auditoria.consulta;

import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/auditoria/cuarentena")
public class CuarentenaController {
    private final ConsultaCuarentenaRepository repository;
    public CuarentenaController(ConsultaCuarentenaRepository repository) { this.repository = repository; }

    @GetMapping
    public Pagina listar(@RequestParam(required = false) String tipo,
                         @RequestParam(defaultValue = "0") int pagina, @RequestParam(defaultValue = "25") int tamano) {
        if (pagina < 0 || pagina > 10000 || tamano < 1 || tamano > 100 || (tipo != null
                && !tipo.equals("EVENTO_INVALIDO") && !tipo.equals("COLISION_ID")))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parametros de consulta invalidos");
        var eventos = repository.listar(tipo, pagina, tamano);
        return new Pagina(List.copyOf(eventos.subList(0, Math.min(tamano, eventos.size()))), pagina, tamano, eventos.size() > tamano);
    }

    @GetMapping("/{id}")
    public CuarentenaMetadata obtener(@PathVariable String id) {
        if (!id.matches("[0-9a-f]{64}")) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Identificador invalido");
        return repository.buscar(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    public record Pagina(List<CuarentenaMetadata> elementos, int pagina, int tamano, boolean hayMas) { }
}
