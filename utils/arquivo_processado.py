"""Arquivo validado em CSV no disco — evita DataFrame de 100+ MB na RAM do IIS."""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

CSV_SEP = "\x1f"
CSV_ENCODING = "utf-8"


def eh_arquivo_processado(obj):
    return isinstance(obj, ArquivoProcessado)


class ArquivoProcessado:
    """Resultado da validação apontando para CSV no disco (não carrega o arquivo todo)."""

    def __init__(self, caminho, colunas, total_linhas):
        self.caminho = Path(caminho)
        self._colunas = [str(c).strip() for c in (colunas or [])]
        self.total_linhas = int(total_linhas or 0)

    @property
    def empty(self):
        return self.total_linhas <= 0

    @property
    def columns(self):
        return pd.Index(self._colunas)

    @columns.setter
    def columns(self, value):
        self._colunas = [str(c).strip() for c in (value or [])]

    def _opcoes_cabecalho_csv(self):
        if not self._colunas:
            return {}
        return {"names": self._colunas, "header": 0}

    def rename(self, mapper=None, columns=None, inplace=False, **kwargs):
        """Compatível com DataFrame.rename(columns=...) — só altera o cabeçalho em memória."""
        mapa = columns if columns is not None else mapper
        if not mapa:
            return self
        novos = []
        for c in self._colunas:
            if c in mapa:
                novos.append(str(mapa[c]).strip())
                continue
            chave = str(c).strip().upper()
            achou = None
            for origem, destino in mapa.items():
                if str(origem).strip().upper() == chave:
                    achou = destino
                    break
            novos.append(str(achou).strip() if achou is not None else c)
        self._colunas = novos
        return self

    def __len__(self):
        return self.total_linhas

    def head(self, n=100):
        if self.empty or not self.caminho.exists():
            return pd.DataFrame(columns=self._colunas)
        return pd.read_csv(
            self.caminho,
            sep=CSV_SEP,
            encoding=CSV_ENCODING,
            dtype=str,
            keep_default_na=False,
            nrows=int(n),
            **self._opcoes_cabecalho_csv(),
        )

    def __getitem__(self, key):
        """Itera uma coluna sem carregar o arquivo inteiro (compatível com `for v in df[col]`)."""
        nome = str(key).strip().upper()
        try:
            idx = [c.strip().upper() for c in self._colunas].index(nome)
        except ValueError:
            raise KeyError(key)

        def _iter_coluna():
            for chunk in self.iter_chunks(4000):
                for v in chunk.iloc[:, idx]:
                    yield v

        return _iter_coluna()

    def itertuples(self, index=False, name=None):
        for chunk in self.iter_chunks(2000):
            yield from chunk.itertuples(index=index, name=name)

    def iter_chunks(self, tamanho=2000):
        if self.empty or not self.caminho.exists():
            return
        reader = pd.read_csv(
            self.caminho,
            sep=CSV_SEP,
            encoding=CSV_ENCODING,
            dtype=str,
            keep_default_na=False,
            chunksize=int(tamanho),
            **self._opcoes_cabecalho_csv(),
        )
        for chunk in reader:
            yield chunk

    def exportar_csv_excel(self, destino):
        """CSV com `;` e BOM UTF-8 para o Excel, gravado em chunks."""
        import csv as csv_mod
        destino = Path(destino)
        destino.parent.mkdir(parents=True, exist_ok=True)
        with destino.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv_mod.writer(f, delimiter=";", lineterminator="\n")
            writer.writerow(self._colunas)
            for chunk in self.iter_chunks(2000):
                writer.writerows(chunk.itertuples(index=False, name=None))
        return destino


def escrever_csv_processado(caminho, colunas, linhas_iter):
    """Grava linhas (listas) em CSV. Retorna quantidade de registros."""
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with caminho.open("w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.writer(f, delimiter=CSV_SEP, lineterminator="\n")
        writer.writerow(list(colunas))
        for linha in linhas_iter:
            writer.writerow("" if v is None else str(v) for v in linha)
            total += 1
    return total
