"""Validação de arquivo grande fora do worker FastCGI (evita HTTP 500 por timeout/OOM)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JOB_ROOT = ROOT / "temp" / "validacao_jobs"
# FastCGI/IIS corta o request (~90s / memória do worker). Toda validação e
# importação de arquivo agora roda em processo separado (iniciar_job).
TAMANHO_SEGUNDO_PLANO = 0


def _job_dir(job_id: str) -> Path:
    return JOB_ROOT / str(job_id)


def _escrever_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def escrever_status(job_id: str, **kwargs) -> None:
    pasta = _job_dir(job_id)
    status_path = pasta / "status.json"
    status = {}
    if status_path.exists():
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            status = {}
    status.update(kwargs)
    if not status.get("iniciado"):
        status["iniciado"] = datetime.now().isoformat()
    status["atualizado"] = datetime.now().isoformat()
    _escrever_json(status_path, status)


def tamanho_stream(arquivo) -> int:
    stream = arquivo.stream if hasattr(arquivo, "stream") else arquivo
    try:
        pos = stream.tell()
        stream.seek(0, 2)
        size = int(stream.tell() or 0)
        stream.seek(pos)
        return size
    except Exception:
        return 0


def iniciar_job(
    arquivo,
    usuario_id,
    layout_id,
    somente_validacao,
    projeto,
    filename,
    usuario_nome=None,
) -> str:
    job_id = str(uuid.uuid4())
    pasta = _job_dir(job_id)
    pasta.mkdir(parents=True, exist_ok=True)
    dest = pasta / "upload.txt"
    stream = arquivo.stream if hasattr(arquivo, "stream") else arquivo
    if hasattr(stream, "seek"):
        try:
            stream.seek(0)
        except Exception:
            pass
    with dest.open("wb") as out:
        shutil.copyfileobj(stream, out, length=1024 * 1024)

    return _registrar_job(
        job_id,
        usuario_id=usuario_id,
        layout_id=layout_id,
        somente_validacao=somente_validacao,
        projeto=projeto,
        filename=filename,
        usuario_nome=usuario_nome,
    )


def iniciar_job_de_caminho(
    caminho,
    usuario_id,
    layout_id,
    somente_validacao,
    projeto,
    filename,
    usuario_nome=None,
    importacao_id=None,
    arquivo_persistente=None,
) -> str:
    """Copia o arquivo persistido para a pasta do job e dispara o worker."""
    job_id = str(uuid.uuid4())
    pasta = _job_dir(job_id)
    pasta.mkdir(parents=True, exist_ok=True)
    dest = pasta / "upload.txt"
    origem = Path(caminho)
    if origem.resolve() != dest.resolve():
        shutil.copy2(str(origem), str(dest))
    return _registrar_job(
        job_id,
        usuario_id=usuario_id,
        layout_id=layout_id,
        somente_validacao=somente_validacao,
        projeto=projeto,
        filename=filename,
        usuario_nome=usuario_nome,
        importacao_id=importacao_id,
        arquivo_persistente=arquivo_persistente or str(origem),
    )


def _registrar_job(
    job_id,
    usuario_id,
    layout_id,
    somente_validacao,
    projeto,
    filename,
    usuario_nome=None,
    importacao_id=None,
    arquivo_persistente=None,
) -> str:
    pasta = _job_dir(job_id)
    meta = {
        "job_id": job_id,
        "usuario_id": usuario_id,
        "usuario_nome": usuario_nome,
        "layout_id": layout_id,
        "somente_validacao": bool(somente_validacao),
        "filename": filename,
        "banco_gx": (projeto or {}).get("DadosGX"),
        "banco_wf": (projeto or {}).get("BancoHomo"),
        "projeto_id": (projeto or {}).get("ProjetoID"),
        "importacao_id": importacao_id,
        "arquivo_persistente": arquivo_persistente,
        "tipo": "validacao",
        "created": datetime.now().isoformat(),
    }
    _escrever_json(pasta / "meta.json", meta)
    escrever_status(job_id, state="queued", mensagem="Arquivo recebido. Iniciando validação...")
    _spawn_worker(job_id)
    return job_id


def _spawn_worker(job_id: str) -> None:
    pasta = _job_dir(job_id)
    log_file = (pasta / "worker.log").open("ab")
    flags = 0
    if os.name == "nt":
        # CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP | CREATE_BREAKAWAY_FROM_JOB
        flags = 0x08000000 | 0x00000200 | 0x01000000
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    env["PYTHONUNBUFFERED"] = "1"
    try:
        subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), job_id],
            cwd=str(ROOT),
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            close_fds=True,
            creationflags=flags,
            env=env,
        )
    except Exception as exc:
        try:
            log_file.write(f"Falha ao iniciar worker: {exc}\n".encode("utf-8", errors="replace"))
            log_file.close()
        except Exception:
            pass
        escrever_status(job_id, state="error", mensagem=f"Não foi possível iniciar o processamento: {exc}")
        raise


def iniciar_importacao_job(process_id, usuario_id, projeto, usuario_nome=None) -> str:
    """Importa um processamento já validado fora do worker IIS."""
    job_id = str(uuid.uuid4())
    pasta = _job_dir(job_id)
    pasta.mkdir(parents=True, exist_ok=True)
    meta = {
        "job_id": job_id,
        "tipo": "importacao",
        "process_id": process_id,
        "usuario_id": usuario_id,
        "usuario_nome": usuario_nome,
        "banco_gx": (projeto or {}).get("DadosGX"),
        "banco_wf": (projeto or {}).get("BancoHomo"),
        "projeto_id": (projeto or {}).get("ProjetoID"),
        "created": datetime.now().isoformat(),
    }
    _escrever_json(pasta / "meta.json", meta)
    escrever_status(job_id, state="queued", mensagem="Importação em segundo plano iniciada...")
    _spawn_worker(job_id)
    return job_id


def carregar_status(job_id, usuario_id):
    pasta = _job_dir(job_id)
    meta_path = pasta / "meta.json"
    status_path = pasta / "status.json"
    if not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if str(meta.get("usuario_id")) != str(usuario_id):
        return None
    status = {}
    if status_path.exists():
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            status = {}
    status.setdefault("state", "queued")
    status["job_id"] = job_id
    from utils.importacao_controle import (
        duracao_segundos,
        epoch_local,
        formatar_duracao,
    )
    iniciado = status.get("iniciado") or meta.get("created")
    fim = None
    if (status.get("state") or "").lower() in ("done", "error"):
        fim = status.get("atualizado")
    segundos = duracao_segundos(iniciado, fim)
    status["inicio_epoch"] = epoch_local(iniciado)
    status["tempo_processamento_segundos"] = segundos
    status["tempo_processamento"] = formatar_duracao(segundos)
    return status


def _row_to_dict(row):
    if row is None:
        return {}
    if hasattr(row, "_asdict"):
        return row._asdict()
    if hasattr(row, "cursor_description"):
        return dict(zip([column[0] for column in row.cursor_description], row))
    if hasattr(row, "keys"):
        return {k: row[k] for k in row.keys()}
    return dict(row)


def _mensagem_status_job(job_id: str) -> str:
    pasta = _job_dir(job_id)
    status_path = pasta / "status.json"
    if not status_path.exists():
        return ""
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return ""
    return str(status.get("mensagem") or "").strip()


def _total_df(df):
    if df is None or getattr(df, "empty", True):
        return 0
    return int(getattr(df, "attrs", {}).get("total") or len(df))


def _rodar_importacao_salva(job_id, process_id, usuario_id, projeto_id):
    from db.connection import definir_projeto_conexao, buscar_credenciais_projeto
    from utils.importacao_store import carregar_processamento, salvar_importacao_resultado
    from utils.importacao_executar import importar_por_tipo, montar_resultado_importacao
    from routes.importacao import _detectar_tipo_importacao, _config_importacao_por_tipo

    if projeto_id:
        definir_projeto_conexao(projeto_id)
    creds = buscar_credenciais_projeto(projeto_id) or {}
    banco_gx = creds.get("DadosGX")
    banco_wf = creds.get("BancoHomo")
    if not banco_gx or not banco_wf:
        escrever_status(
            job_id, state="error",
            mensagem="Configure DadosGX e BancoHomo no cadastro do projeto.",
            process_id=process_id,
        )
        return False

    data = carregar_processamento(process_id, usuario_id, carregar_detalhes=False)
    if not data:
        escrever_status(job_id, state="error", mensagem="Resultado da validação não encontrado.", process_id=process_id)
        return False
    if data.get("importacao_resultado"):
        escrever_status(
            job_id, state="done",
            mensagem="Este arquivo já foi importado.",
            process_id=process_id,
        )
        return True

    if data.get("tem_erro_estrutura"):
        escrever_status(
            job_id, state="error",
            mensagem=(
                "Erro de estrutura: quantidade de posições diferente do layout. "
                "A importação foi bloqueada."
            ),
            process_id=process_id,
        )
        return False

    layout_nome = data.get("layout_nome") or ""
    tipo = _detectar_tipo_importacao(
        layout_nome, data.get("layout_descricao"), data.get("colunas_layout"),
    )
    if not tipo:
        escrever_status(
            job_id, state="error",
            mensagem=f'Layout "{layout_nome}" não possui importação automática configurada.',
            process_id=process_id,
        )
        return False

    cfg = _config_importacao_por_tipo(tipo)
    escrever_status(job_id, state="running", mensagem="Importando para o banco DadosGX...", process_id=process_id)
    try:
        tupla = importar_por_tipo(tipo, data.get("df_processado"), banco_gx, banco_wf)
    except MemoryError:
        escrever_status(
            job_id, state="error",
            mensagem=(
                "O arquivo é grande demais para importar na memória deste servidor. "
                "Divida-o em partes menores."
            ),
            process_id=process_id,
        )
        return False
    except Exception as exc:
        escrever_status(
            job_id, state="error",
            mensagem=f"Erro na importação: {exc}",
            process_id=process_id,
        )
        return False
    if not tupla:
        escrever_status(job_id, state="error", mensagem="Falha na importação automática.", process_id=process_id)
        return False
    sucesso, mensagem, resumo = tupla
    resultado = montar_resultado_importacao(tipo, cfg, banco_gx, sucesso, mensagem, resumo)
    salvar_importacao_resultado(process_id, usuario_id, resultado)
    if sucesso:
        try:
            from utils.layout_importacao_projeto import registrar_layout_importado
            registrar_layout_importado(
                projeto_id,
                data.get("layout_id"),
                nome_layout=layout_nome,
                usuario_id=usuario_id,
            )
        except Exception:
            pass
        try:
            from utils.historico_envio_arquivo import registrar_envio_arquivo
            registrar_envio_arquivo(
                projeto_id,
                layout_id=data.get("layout_id"),
                nome_layout=layout_nome,
                nome_arquivo=None,
                modo="importacao",
                total_linhas=int(data.get("total_linhas") or 0),
                total_erros=int(data.get("total_erros") or 0),
                total_avisos=int(data.get("total_avisos") or 0),
                importacao_realizada=True,
                process_id=process_id,
                usuario_id=usuario_id,
            )
        except Exception:
            pass
        escrever_status(
            job_id, state="done",
            mensagem=mensagem or "Importação concluída.",
            process_id=process_id,
        )
        try:
            from utils.importacao_controle import marcar_conclusao_ativa
            res = resumo or {}
            marcar_conclusao_ativa(
                com_erros=int(res.get("flag_0") or 0) > 0,
                Mensagem=mensagem or "Publicação concluída.",
                TotalRegistros=int(data.get("total_linhas") or res.get("total") or 0),
                RegistrosProcessados=int(res.get("total") or data.get("total_linhas") or 0),
                RegistrosImportados=int(res.get("flag_1") or 0),
                RegistrosComErro=int(res.get("flag_0") or data.get("total_erros") or 0),
                ProcessId=process_id,
            )
        except Exception:
            pass
        return True
    escrever_status(job_id, state="error", mensagem=mensagem or "Erro na importação.", process_id=process_id)
    return False


def executar_job(job_id: str) -> None:
    os.chdir(str(ROOT))
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    pasta = _job_dir(job_id)
    meta = json.loads((pasta / "meta.json").read_text(encoding="utf-8"))

    from db.connection import conectar_banco, definir_projeto_conexao
    from utils.importacao_store import salvar_processamento
    from utils.layout_validation import validar_arquivo_com_layout
    from utils.importacao_controle import (
        definir_importacao_ativa,
        marcar_conclusao,
        marcar_erro,
        marcar_inicio_processamento,
        reportar_progresso,
    )
    import pandas as pd

    importacao_id = meta.get("importacao_id")
    definir_importacao_ativa(importacao_id)

    if meta.get("projeto_id"):
        definir_projeto_conexao(meta["projeto_id"])

    if meta.get("tipo") == "importacao":
        process_id = meta.get("process_id")
        if not process_id:
            escrever_status(job_id, state="error", mensagem="Processo de importação sem process_id.")
            marcar_erro(importacao_id, "Processo de importação sem process_id.")
            return
        if importacao_id:
            marcar_inicio_processamento(importacao_id, "Publicando no banco DadosGX...")
        ok = _rodar_importacao_salva(job_id, process_id, meta.get("usuario_id"), meta.get("projeto_id"))
        if not ok:
            marcar_erro(
                importacao_id,
                _mensagem_status_job(job_id) or "Falha na importação para o banco.",
            )
        return

    escrever_status(job_id, state="running", mensagem="Validando arquivo...")
    if importacao_id:
        marcar_inicio_processamento(importacao_id, "Validando arquivo...")

    layout_id = meta["layout_id"]
    usuario_id = meta.get("usuario_id")
    somente_validacao = bool(meta.get("somente_validacao"))
    upload_path = pasta / "upload.txt"

    conn = conectar_banco()
    if not conn:
        escrever_status(job_id, state="error", mensagem="Erro ao conectar com o banco de dados.")
        marcar_erro(importacao_id, "Erro ao conectar com o banco de dados.")
        return
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Layouts WHERE LayoutID = ?", (layout_id,))
        layout = cursor.fetchone()
        if not layout:
            escrever_status(job_id, state="error", mensagem="Layout selecionado não encontrado.")
            return
        layout_dict = _row_to_dict(layout)
        cursor.execute(
            "SELECT * FROM LayoutColunas WHERE LayoutID = ? ORDER BY Posicao",
            (layout_id,),
        )
        colunas_dict = [_row_to_dict(c) for c in cursor.fetchall()]
    finally:
        conn.close()

    if not colunas_dict:
        escrever_status(
            job_id, state="error",
            mensagem="O layout selecionado não possui colunas configuradas.",
        )
        return

    class _ArquivoDisco:
        def __init__(self, path, filename):
            self.filename = filename
            self.stream = open(path, "rb")

        def seek(self, *args, **kwargs):
            return self.stream.seek(*args, **kwargs)

        def read(self, *args, **kwargs):
            return self.stream.read(*args, **kwargs)

        def close(self):
            self.stream.close()

    tamanho_arquivo = 0
    try:
        tamanho_arquivo = int(upload_path.stat().st_size or 0)
    except OSError:
        tamanho_arquivo = 0

    def _fmt_int(n):
        return f"{int(n):,}".replace(",", ".")

    def _progresso_validacao(linhas):
        linhas = int(linhas or 0)
        if tamanho_arquivo:
            estimado = max(linhas, int(tamanho_arquivo / 180))
            frac = min(0.99, linhas / max(estimado, 1))
        else:
            frac = min(0.99, linhas / 1_000_000.0)
        pct = round(10 + 28.0 * frac, 1)
        msg = f"Validando arquivo... {_fmt_int(linhas)} linhas lidas"
        reportar_progresso(
            percentual=pct,
            mensagem=msg,
            RegistrosProcessados=linhas,
        )
        escrever_status(job_id, state="running", mensagem=msg)

    arquivo = _ArquivoDisco(upload_path, meta.get("filename") or "arquivo.txt")
    try:
        banco_gx = meta.get("banco_gx") if not somente_validacao else None
        df_processado, df_erros, df_avisos, mensagem = validar_arquivo_com_layout(
            arquivo,
            colunas_dict,
            layout_nome=layout_dict.get("NomeLayout"),
            layout_descricao=layout_dict.get("Descricao"),
            banco_gx=banco_gx,
            validar_dependencias_banco=not somente_validacao,
            on_progress=_progresso_validacao,
        )
    except MemoryError:
        escrever_status(
            job_id, state="error",
            mensagem=(
                "O arquivo é grande demais para validar na memória deste servidor. "
                "Divida-o em partes menores."
            ),
        )
        raise
    except Exception as exc:
        escrever_status(job_id, state="error", mensagem=f"Erro na validação: {exc}")
        raise
    finally:
        arquivo.close()

    if df_processado is None:
        escrever_status(job_id, state="error", mensagem=mensagem or "Falha na validação.")
        marcar_erro(importacao_id, mensagem or "Falha na validação.")
        return

    process_id = str(uuid.uuid4())
    reportar_progresso(
        percentual=40,
        mensagem="Validação do layout concluída. Gravando resultado...",
        TotalRegistros=len(df_processado) if df_processado is not None else 0,
        RegistrosProcessados=len(df_processado) if df_processado is not None else 0,
        RegistrosComErro=_total_df(df_erros),
    )
    escrever_status(job_id, state="running", mensagem="Gravando resultado da validação...")
    try:
        salvar_processamento(
            process_id,
            usuario_id,
            layout_dict["NomeLayout"],
            df_processado,
            df_erros if df_erros is not None else pd.DataFrame(),
            df_avisos if df_avisos is not None else pd.DataFrame(),
            layout_id=layout_id,
            layout_descricao=layout_dict.get("Descricao"),
            colunas_layout=colunas_dict,
            mensagem=mensagem,
        )
    except MemoryError:
        escrever_status(
            job_id, state="error",
            mensagem=(
                "A validação terminou, mas o resultado não coube na memória para gravar. "
                "Divida o arquivo em partes menores."
            ),
        )
        raise
    except Exception as exc:
        escrever_status(job_id, state="error", mensagem=f"Erro ao gravar o resultado da validação: {exc}")
        raise
    try:
        from utils.historico_envio_arquivo import registrar_envio_arquivo
        registrar_envio_arquivo(
            meta.get("projeto_id"),
            layout_id=layout_id,
            nome_layout=layout_dict.get("NomeLayout"),
            nome_arquivo=meta.get("filename"),
            modo="validacao" if somente_validacao else "importacao",
            total_linhas=len(df_processado) if df_processado is not None else 0,
            total_erros=_total_df(df_erros),
            total_avisos=_total_df(df_avisos),
            importacao_realizada=False,
            process_id=process_id,
            usuario_id=usuario_id,
            usuario_nome=meta.get("usuario_nome"),
            df_erros=df_erros,
            df_avisos=df_avisos,
        )
    except Exception:
        pass

    total_erros = _total_df(df_erros)
    total_linhas = len(df_processado) if df_processado is not None else 0
    reportar_progresso(
        percentual=40,
        mensagem=f"Layout validado. Total de registros identificado: {total_linhas}",
        TotalRegistros=total_linhas,
        RegistrosProcessados=total_linhas,
        RegistrosComErro=total_erros,
        ProcessId=process_id,
    )

    if not somente_validacao and total_erros == 0:
        escrever_status(
            job_id, state="running",
            mensagem="Validação ok. Importando para o banco...",
            process_id=process_id,
        )
        reportar_progresso(percentual=50, mensagem="Importando registros em lotes para a staging...")
        ok = _rodar_importacao_salva(job_id, process_id, usuario_id, meta.get("projeto_id"))
        if not ok:
            marcar_erro(
                importacao_id,
                _mensagem_status_job(job_id) or "Falha na importação para o banco.",
            )
        return

    com_erros = total_erros > 0
    marcar_conclusao(
        importacao_id,
        com_erros=com_erros,
        Mensagem=mensagem or "Arquivo validado com sucesso",
        TotalRegistros=total_linhas,
        RegistrosProcessados=total_linhas,
        RegistrosComErro=total_erros,
        ProcessId=process_id,
        Percentual=100,
    )
    escrever_status(
        job_id,
        state="done",
        mensagem=mensagem or "Arquivo validado com sucesso",
        process_id=process_id,
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("uso: validacao_job.py <job_id>")
    try:
        executar_job(sys.argv[1])
    except MemoryError:
        escrever_status(
            sys.argv[1],
            state="error",
            mensagem=(
                "O arquivo é grande demais para validar na memória deste servidor. "
                "Divida-o em partes menores."
            ),
        )
        try:
            from utils.importacao_controle import marcar_erro
            meta = json.loads((_job_dir(sys.argv[1]) / "meta.json").read_text(encoding="utf-8"))
            marcar_erro(meta.get("importacao_id"), "Memória esgotada no servidor.")
        except Exception:
            pass
        raise
    except Exception as exc:
        escrever_status(sys.argv[1], state="error", mensagem=f"Erro no processamento: {exc}")
        try:
            from utils.importacao_controle import marcar_erro
            meta = json.loads((_job_dir(sys.argv[1]) / "meta.json").read_text(encoding="utf-8"))
            marcar_erro(meta.get("importacao_id"), str(exc))
        except Exception:
            pass
        raise
