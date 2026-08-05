"""Vínculo Layout ↔ tipo de escopo (PESSOA, PRODUTOS, …) + flag obrigatório."""

from __future__ import annotations

from logger import logger
from utils.depara_escopos import CATEGORIAS_NOMES

# Mesmos códigos usados em Gerenciar Escopos (+ Fiscal)
TIPOS_ESCOPO_LAYOUT = {
    **CATEGORIAS_NOMES,
    "FISCAL": "Fiscal",
}


def label_tipo_escopo(codigo: str | None) -> str:
    if not codigo:
        return ""
    return TIPOS_ESCOPO_LAYOUT.get(str(codigo).strip().upper(), str(codigo))


def garantir_colunas_layout_escopo(cursor, conn=None):
    """Garante TipoEscopo e ObrigatorioNoEscopo em Layouts."""
    try:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'Layouts'
              AND COLUMN_NAME IN ('TipoEscopo', 'ObrigatorioNoEscopo')
            """
        )
        existentes = {row[0] for row in cursor.fetchall()}

        if "TipoEscopo" not in existentes:
            cursor.execute(
                "ALTER TABLE Layouts ADD TipoEscopo NVARCHAR(50) NULL"
            )
            logger.info("Coluna Layouts.TipoEscopo criada")
            if conn:
                conn.commit()

        if "ObrigatorioNoEscopo" not in existentes:
            cursor.execute(
                "ALTER TABLE Layouts ADD ObrigatorioNoEscopo BIT NOT NULL DEFAULT 0"
            )
            logger.info("Coluna Layouts.ObrigatorioNoEscopo criada")
            if conn:
                conn.commit()
    except Exception as exc:
        logger.warning("Não foi possível ajustar colunas de escopo do layout: %s", exc)


def ler_escopo_do_form(form) -> tuple[str | None, int]:
    """Lê tipo_escopo e obrigatorio_escopo do formulário."""
    tipo = (form.get("tipo_escopo") or "").strip().upper()
    if tipo and tipo not in TIPOS_ESCOPO_LAYOUT:
        tipo = ""
    obrigatorio = 1 if form.get("obrigatorio_escopo") in ("1", "on", "true", "True") else 0
    if not tipo:
        obrigatorio = 0
    return (tipo or None), obrigatorio
