"""Gera pacote instalavel das procedures SQL para bancos DadosGX."""

from __future__ import annotations

import re
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "procedure"
OUT = ROOT / "pacote_dadosgx"
STAMP = datetime.now().strftime("%Y%m%d")
ZIP_PATH = ROOT / f"pacote_procedures_DadosGX_{STAMP}.zip"

PROC_NAME_RE = re.compile(
    r"CREATE\s+PROCEDURE\s+(?:\[?dbo\]?\.)?\[?(up_[A-Za-z0-9_]+)\]?",
    re.IGNORECASE,
)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig", errors="replace")


def extract_proc_name(sql_text: str, fallback: str) -> str:
    m = PROC_NAME_RE.search(sql_text)
    if m:
        return m.group(1)
    # fallback pelo nome do arquivo (sem extensão)
    stem = Path(fallback).stem
    return stem


def list_extracao() -> list[Path]:
    files: list[Path] = []
    for p in sorted(SRC.rglob("*.sql")):
        if "depara" in p.parts:
            continue
        if p.parent.name == "_util" and p.name.startswith("alter_"):
            continue
        if p.name in ("00_drop_procedures.sql",):
            continue
        files.append(p)
    # ordem utilitaria: replace primeiro, depois criticas, depois extracoes
    def key(path: Path):
        name = path.name.lower()
        if name.startswith("up_replace"):
            return (0, name)
        if name.startswith("up_09"):
            return (2, name)
        return (1, path.as_posix().lower())

    return sorted(files, key=key)


def list_depara() -> list[Path]:
    files: list[Path] = []
    for p in sorted((SRC / "depara").rglob("*.sql")):
        if p.name == "00_drop_procedures.sql":
            continue
        files.append(p)

    # ordenar por familia e numero do up_
    def key(path: Path):
        rel = path.relative_to(SRC / "depara").as_posix()
        m = re.search(r"up_(\d+)_", path.name, re.I)
        num = int(m.group(1)) if m else 999
        familia = path.parent.name.lower()
        ordem_familia = {
            "forn_cli": 10,
            "endereco": 20,
            "banco": 30,
            "produto": 40,
            "produtoestoque": 50,
            "veiculo": 60,
            "financeiro": 70,
            "movimentoestoque": 80,
            "fseg_cab": 90,
        }.get(familia, 500)
        return (ordem_familia, num, rel.lower())

    return sorted(files, key=key)


def normalize_use(sql: str) -> str:
    """Padroniza USE para placeholder claro do pacote."""
    return re.sub(
        r"USE\s+\[[^\]]+\];\s*--\s*ALTERE",
        "USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )


def write_drop(path: Path, proc_names: list[str], titulo: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "-- =============================================================================",
        f"-- {titulo}",
        "-- Pacote DadosGX — gerar drop antes da reinstalacao",
        f"-- Gerado em: {datetime.now():%d/%m/%Y %H:%M}",
        "-- =============================================================================",
        "",
        "USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX",
        "GO",
        "",
    ]
    for name in proc_names:
        lines.extend(
            [
                f"IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = '{name}' AND TYPE = 'P')",
                f"    DROP PROCEDURE dbo.[{name}];",
                "GO",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def strip_sqlcmd_directives(sql: str) -> str:
    """Remove linhas :r / :setvar (nao sao T-SQL)."""
    out = []
    for line in sql.splitlines():
        if re.match(r"^\s*:(r|setvar|out|error|!!|connect|on\s+error)\b", line, re.I):
            continue
        out.append(line)
    return "\n".join(out)


def ensure_ends_with_go(sql: str) -> str:
    text = sql.strip()
    if not text:
        return ""
    # Se ja termina com GO, mantem
    if re.search(r"(?im)^\s*GO\s*$", text[-20:]):
        return text + "\n"
    return text + "\nGO\n"


def write_concat_installer(
    path: Path,
    file_paths: list[Path],
    titulo: str,
    *,
    footer: str = "",
) -> None:
    """Gera um .sql unico em T-SQL puro (sem SQLCMD Mode)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    parts = [
        "-- =============================================================================",
        f"-- {titulo}",
        "-- Instalador em T-SQL puro (NAO precisa SQLCMD Mode).",
        "-- Antes de executar: substitua [DadosGX_SeuProjeto] pelo nome real do banco.",
        f"-- Gerado em: {datetime.now():%d/%m/%Y %H:%M}",
        "-- =============================================================================",
        "",
        "USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX",
        "GO",
        "",
    ]

    for fp in file_paths:
        body = strip_sqlcmd_directives(read_text(fp))
        # Remove USE duplicado nos scripts filhos (o USE do cabecalho ja selecionou o banco)
        body = re.sub(
            r"(?im)^\s*USE\s+\[[^\]]+\]\s*;?\s*(--.*)?\r?\n\s*GO\s*\r?\n?",
            "",
            body,
            count=1,
        )
        parts.append("")
        parts.append(f"-- >>> INICIO: {fp.name}")
        parts.append(ensure_ends_with_go(body).rstrip())
        parts.append(f"-- <<< FIM: {fp.name}")
        parts.append("GO")
        parts.append("")

    if footer:
        parts.append(footer.rstrip())
        parts.append("")

    path.write_text("\n".join(parts), encoding="utf-8")


def copy_sql(src: Path, dst: Path) -> str:
    dst.parent.mkdir(parents=True, exist_ok=True)
    text = normalize_use(read_text(src))
    dst.write_text(text, encoding="utf-8")
    return extract_proc_name(text, src.name)


def main() -> None:
    if OUT.exists():
        # Evita PermissionError se a pasta estiver aberta no Explorer/SSMS
        for child in OUT.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
            except Exception:
                pass
        try:
            OUT.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
    else:
        OUT.mkdir(parents=True)

    extracao_src = list_extracao()
    depara_src = list_depara()

    extracao_names: list[str] = []
    extracao_includes: list[str] = ["01_extracao\\_util\\00_drop_procedures.sql"]

    # replace util primeiro
    for src in extracao_src:
        if src.name.lower().startswith("up_replace"):
            rel = Path("01_extracao") / "_util" / src.name
            name = copy_sql(src, OUT / rel)
            extracao_names.append(name)
            extracao_includes.append(rel.as_posix().replace("/", "\\"))

    # demais extracoes (pastas) + criticas
    for src in extracao_src:
        if src.name.lower().startswith("up_replace"):
            continue
        if src.parent.name == "_util":
            rel = Path("01_extracao") / "_util" / src.name
        else:
            rel = Path("01_extracao") / src.parent.name / src.name
        name = copy_sql(src, OUT / rel)
        extracao_names.append(name)
        extracao_includes.append(rel.as_posix().replace("/", "\\"))

    # drop extracao: replace + todas
    # ordem do drop: todas as procs de negocio, replace por ultimo
    drop_extracao = [n for n in extracao_names if not n.lower().startswith("up_replace")]
    drop_extracao += [n for n in extracao_names if n.lower().startswith("up_replace")]
    # dedupe preserving order
    seen = set()
    drop_extracao_unique = []
    for n in drop_extracao:
        k = n.lower()
        if k in seen:
            continue
        seen.add(k)
        drop_extracao_unique.append(n)

    write_drop(
        OUT / "01_extracao" / "_util" / "00_drop_procedures.sql",
        drop_extracao_unique,
        "DROP — Procedures de Extração (DadosGX)",
    )

    # Arquivos fisicos na ordem de instalacao (extracao)
    extracao_files: list[Path] = [OUT / "01_extracao" / "_util" / "00_drop_procedures.sql"]
    for rel in extracao_includes[1:]:
        extracao_files.append(OUT / Path(rel.replace("\\", "/")))

    write_concat_installer(
        OUT / "01_extracao" / "instalar_todas.sql",
        extracao_files,
        "INSTALAR — Procedures de Extração (DadosGX)",
    )

    depara_names: list[str] = []
    depara_root_includes: list[str] = ["02_depara\\_util\\00_drop_procedures.sql"]
    for src in depara_src:
        rel = Path("02_depara") / src.parent.name / src.name
        name = copy_sql(src, OUT / rel)
        depara_names.append(name)
        depara_root_includes.append(rel.as_posix().replace("/", "\\"))

    seen = set()
    depara_unique = []
    for n in depara_names:
        k = n.lower()
        if k in seen:
            continue
        seen.add(k)
        depara_unique.append(n)

    write_drop(
        OUT / "02_depara" / "_util" / "00_drop_procedures.sql",
        depara_unique,
        "DROP — Procedures De/Para (DadosGX)",
    )

    depara_files: list[Path] = [OUT / "02_depara" / "_util" / "00_drop_procedures.sql"]
    for rel in depara_root_includes[1:]:
        depara_files.append(OUT / Path(rel.replace("\\", "/")))

    write_concat_installer(
        OUT / "02_depara" / "instalar_todas.sql",
        depara_files,
        "INSTALAR — Procedures De/Para (DadosGX)",
    )

    master_files = list(extracao_files) + list(depara_files)
    footer = """
PRINT '=============================================================================';
PRINT 'Instalacao concluida.';
PRINT 'Execute agora (troque pelo nome real do banco):';
PRINT 'EXEC dbo.up_Replace_Name_DadosGx_Procedures @BancoDadosGX = ''NomeExatoDoSeuBanco'';';
PRINT '=============================================================================';
GO
"""
    write_concat_installer(
        OUT / "00_instalar_completo.sql",
        master_files,
        "INSTALAR COMPLETO — Extração + De/Para (DadosGX)",
        footer=footer,
    )

    readme = f"""PACOTE PROCEDURES SQL — DADOS GX
================================
Gerado em: {datetime.now():%d/%m/%Y %H:%M}

Conteudo
--------
  00_instalar_completo.sql     Instalador mestre (extracao + De/Para) — T-SQL puro
  01_extracao/                 Procedures de extracao + instalar_todas.sql
  02_depara/                   Procedures De/Para + instalar_todas.sql

O que NAO entra neste pacote
----------------------------
  Scripts alter_* da pasta procedure/_util (sao do banco da aplicacao De/Para,
  nao do DadosGX): LayoutVersao, Email, Projeto, etc.

Como instalar (SSMS)
--------------------
1. Abra o SQL Server Management Studio.
2. Conecte no servidor onde esta o banco DadosGX do projeto.
3. Abra 00_instalar_completo.sql
   (NAO precisa ativar SQLCMD Mode).
4. Substitua em todo o arquivo:
     [DadosGX_SeuProjeto]
   pelo nome real do banco (ex.: DadosGX_ClienteX).
5. Execute (F5).
6. Apos criar as procedures, execute UMA vez:

   EXEC dbo.up_Replace_Name_DadosGx_Procedures
        @BancoDadosGX = 'NomeExatoDoSeuBanco';

   Isso troca o nome template interno pelo banco real.

Parametros das procedures
-------------------------
  @BancoDadosGX  Nome do banco DadosGX
  @BancoWF       Nome do banco Workflow (quando exigido)

Contagem
--------
  Extração: {len(drop_extracao_unique)} procedure(s)
  De/Para : {len(depara_unique)} procedure(s)
"""
    (OUT / "LEIA-ME.txt").write_text(readme, encoding="utf-8")

    # inventario
    inv_lines = ["# Inventario do pacote", ""]
    inv_lines.append("## Extracao")
    for rel in extracao_includes[1:]:
        inv_lines.append(f"- {rel}")
    inv_lines.append("")
    inv_lines.append("## De/Para")
    for rel in depara_root_includes[1:]:
        inv_lines.append(f"- {rel}")
    (OUT / "INVENTARIO.txt").write_text("\n".join(inv_lines), encoding="utf-8")

    if ZIP_PATH.exists():
        ZIP_PATH.unlink()
    with zipfile.ZipFile(ZIP_PATH, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in OUT.rglob("*"):
            if f.is_file():
                zf.write(f, arcname=str(Path("pacote_dadosgx") / f.relative_to(OUT)))

    print(f"Pacote: {OUT}")
    print(f"ZIP:    {ZIP_PATH}")
    print(f"Extracao: {len(drop_extracao_unique)}")
    print(f"De/Para:  {len(depara_unique)}")


if __name__ == "__main__":
    main()
