package com.agrocenter.auditoria.eventos;

public class EventoInvalidoException extends IllegalArgumentException {
    public EventoInvalidoException(String message) { super(message); }
    public EventoInvalidoException(String message, Throwable cause) { super(message, cause); }
}
