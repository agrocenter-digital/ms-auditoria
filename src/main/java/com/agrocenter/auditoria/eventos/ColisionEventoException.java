package com.agrocenter.auditoria.eventos;

/** La misma identidad llegó con contenido distinto: requiere investigación, no se debe omitir. */
public class ColisionEventoException extends IllegalStateException {
    public ColisionEventoException() { super("El eventId ya existe con contenido diferente"); }
}
