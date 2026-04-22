"""
source_reader.py
================
Leitura genérica de planilhas de qualquer marketplace como ORIGEM.

Complementa o reader.py (que lê planilhas Amazon) — este módulo lê
planilhas dos outros marketplaces como FONTE, retornando um DataFrame
padronizado com os nomes de coluna originais do marketplace.

O pipeline detecta automaticamente se a origem é Amazon (usa reader.py)
ou outro marketplace (usa este módulo), baseado no parâmetro source_marketplace.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ─── Configuração por marketplace ─────────────────────────────────────────────
#
# Cada entrada define como ler a aba de dados do marketplace.
# Campos:
#   sheet          — nome exato da aba (case-sensitive)
#   sheet_prefix   — prefixo da aba (ex: Vendor usa "Modelo-")
#   sheet_index    — posição da aba quando o nome varia (ex: Mercado Livre)
#   sheet_candidates — lista de nomes candidatos em ordem de prioridade
#   header_row     — linha do cabeçalho (1-indexed)
#   data_start     — primeira linha de dados (1-indexed)
#   skip_row_if    — lista de strings: pula linha se qualquer célula contiver

SOURCE_CONFIG: dict[str, dict] = {
    "Mercado Livre": {
        # Terceira aba do workbook (índice 2) — nome varia por categoria
        "sheet_index": 2,
        "header_row": 3,
        "data_start": 9,
    },
    "Shopee": {
        "sheet": "Modelo",
        "header_row": 3,
        "data_start": 7,
    },
    "Walmart": {
        "sheet": "Product Content And Site Exp",
        "header_row": 4,
        "data_start": 7,
    },
    "Temu": {
        "sheet": "Template",
        "header_row": 2,
        "data_start": 5,
        "skip_row_if": ["#", "[", "ABC123"],
    },
    "Vendor": {
        "sheet_prefix": "Modelo-",
        "header_row": 3,
        "data_start": 7,   # começa na linha 7 (IDs internos e exemplo são filtrados abaixo)
        "skip_row_if": ["#", ".value", "ABC123", "AMZN4", "OBRIGATÓRIO", "CONDICIONALMENTE"],
    },
    "Magalu": {
        "sheet": "PRODUTO",
        "header_row": 3,
        "data_start": 5,
        "skip_row_if": ["#", "[", "ABC123"],
    },
    # Amazon como origem usa reader.py (AmazonSheetReader),
    # mas deixamos uma entrada aqui como fallback genérico caso necessário.
    "Amazon": {
        "sheet_candidates": ["Template", "Modelo"],
        "header_row": 3,
        "data_start": 5,
        "skip_row_if": ["settings=", "reserved line", "templatetype=", "#", "["],
    },
}


# ─── Dataclass de resultado ───────────────────────────────────────────────────

@dataclass
class SourceReadResult:
    df: pd.DataFrame
    marketplace: str
    sheet_name: str
    total_rows: int
    valid_rows: int
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)


# ─── Reader ───────────────────────────────────────────────────────────────────

class MarketplaceSourceReader:
    """
    Lê planilhas de marketplaces como ORIGEM (não como destino/template).
    Retorna DataFrame com colunas originais do marketplace — o mapper.py
    se encarrega de traduzir para chaves semânticas.
    """

    def read(self, file_like, marketplace: str) -> SourceReadResult:
        config = SOURCE_CONFIG.get(marketplace)
        if config is None:
            return SourceReadResult(
                df=pd.DataFrame(), marketplace=marketplace,
                sheet_name="", total_rows=0, valid_rows=0,
                errors=[
                    f"Marketplace '{marketplace}' não configurado em SOURCE_CONFIG. "
                    f"Disponíveis: {list(SOURCE_CONFIG.keys())}"
                ],
            )

        warnings: list[str] = []

        # ── 1. Abrir workbook ─────────────────────────────────────────────
        try:
            if hasattr(file_like, "seek"):
                file_like.seek(0)
            xl = pd.ExcelFile(file_like)
        except Exception as exc:
            return SourceReadResult(
                df=pd.DataFrame(), marketplace=marketplace,
                sheet_name="", total_rows=0, valid_rows=0,
                errors=[f"Não foi possível abrir o arquivo: {exc}"],
            )

        # ── 2. Resolver aba ───────────────────────────────────────────────
        sheet_name = self._resolve_sheet(xl.sheet_names, config, marketplace, warnings)
        if sheet_name is None:
            return SourceReadResult(
                df=pd.DataFrame(), marketplace=marketplace,
                sheet_name="", total_rows=0, valid_rows=0,
                errors=[f"Aba de dados não encontrada no arquivo {marketplace}."],
            )

        # ── 3. Ler raw sem header ─────────────────────────────────────────
        try:
            if hasattr(file_like, "seek"):
                file_like.seek(0)
            df_raw = pd.read_excel(
                file_like, sheet_name=sheet_name,
                header=None, dtype=str,
            )
        except Exception as exc:
            return SourceReadResult(
                df=pd.DataFrame(), marketplace=marketplace,
                sheet_name=sheet_name, total_rows=0, valid_rows=0,
                errors=[f"Erro ao ler aba '{sheet_name}': {exc}"],
            )

        # ── 4. Extrair cabeçalhos e dados ─────────────────────────────────
        header_idx = config["header_row"] - 1   # converter para 0-based
        data_idx   = config["data_start"] - 1

        if header_idx >= len(df_raw):
            return SourceReadResult(
                df=pd.DataFrame(), marketplace=marketplace,
                sheet_name=sheet_name, total_rows=0, valid_rows=0,
                errors=["Arquivo muito curto — linha de cabeçalho não encontrada."],
            )

        raw_headers = df_raw.iloc[header_idx].tolist()
        headers = self._sanitize_headers(raw_headers)

        df_data = df_raw.iloc[data_idx:].copy()
        df_data.columns = headers
        df_data = df_data.dropna(how="all").reset_index(drop=True)

        # ── 5. Filtrar linhas de lixo (IDs internos, exemplos) ────────────
        skip_patterns = config.get("skip_row_if", [])
        if skip_patterns:
            def _is_valid_row(row) -> bool:
                vals = [str(v).strip() for v in row.dropna().values]
                for v in vals:
                    if any(p in v for p in skip_patterns):
                        return False
                    if v.upper() in ("ABC123", "EXAMPLE", "EXEMPLO"):
                        return False
                return True
            mask = df_data.apply(_is_valid_row, axis=1)
            removed = (~mask).sum()
            if removed:
                warnings.append(f"{removed} linha(s) de metadados ignoradas.")
            df_data = df_data[mask].reset_index(drop=True)

        # ── 6. Remover colunas sem nome útil ──────────────────────────────
        useful_cols = [c for c in df_data.columns if not c.startswith("_col_")]
        df_data = df_data[useful_cols]

        # ── 7. Filtrar linhas sem nenhum dado ─────────────────────────────
        valid_mask = df_data.apply(
            lambda r: r.dropna().astype(str).str.strip().ne("").any(), axis=1
        )
        df_data = df_data[valid_mask].reset_index(drop=True)

        total_rows = len(df_data)
        if total_rows == 0:
            warnings.append(f"Nenhuma linha de dados válida encontrada na planilha {marketplace}.")

        logger.info(
            "Source '%s' lida: sheet=%s total=%d",
            marketplace, sheet_name, total_rows,
        )

        return SourceReadResult(
            df=df_data, marketplace=marketplace, sheet_name=sheet_name,
            total_rows=total_rows, valid_rows=total_rows,
            warnings=warnings,
        )

    # ── Privadas ──────────────────────────────────────────────────────────────

    @staticmethod
    def _resolve_sheet(
        sheets: list[str], config: dict, marketplace: str, warnings: list[str]
    ) -> Optional[str]:
        # Posição fixa (ex: Mercado Livre — terceira aba)
        if "sheet_index" in config:
            idx = config["sheet_index"]
            if idx < len(sheets):
                return sheets[idx]
            warnings.append(f"Índice de aba {idx} não encontrado; usando última aba.")
            return sheets[-1] if sheets else None

        # Prefixo (ex: Vendor — "Modelo-Eletronicos")
        if "sheet_prefix" in config:
            prefix = config["sheet_prefix"]
            for s in sheets:
                if s.startswith(prefix):
                    return s
            # Fallback case-insensitive
            for s in sheets:
                if s.lower().startswith(prefix.lower()):
                    warnings.append(f"Aba com prefixo '{prefix}' encontrada como '{s}'.")
                    return s
            return None

        # Lista de candidatos (ex: Amazon com ["Template", "Modelo"])
        if "sheet_candidates" in config:
            candidates = config["sheet_candidates"]
            # Exato
            for cand in candidates:
                if cand in sheets:
                    return cand
            # Case-insensitive
            for cand in candidates:
                for s in sheets:
                    if cand.lower() in s.lower():
                        warnings.append(f"Aba candidata '{cand}' encontrada como '{s}'.")
                        return s
            # Fallback: primeira aba que não seja instrução
            _skip = {"instruções", "instrucoes", "instructions", "ajuda", "help",
                     "dropdown", "conditions", "cover"}
            for s in sheets:
                if not any(sk in s.lower() for sk in _skip):
                    warnings.append(f"Candidatos não encontrados; usando '{s}'.")
                    return s
            return sheets[0] if sheets else None

        # Nome exato
        if "sheet" in config:
            target = config["sheet"]
            if target in sheets:
                return target
            # Fallback case-insensitive
            for s in sheets:
                if s.lower() == target.lower():
                    warnings.append(f"Aba '{target}' encontrada como '{s}'.")
                    return s
            return None

        return None

    @staticmethod
    def _sanitize_headers(raw: list) -> list[str]:
        seen: dict[str, int] = {}
        result = []
        for i, h in enumerate(raw):
            if pd.isna(h) or str(h).strip() == "":
                name = f"_col_{i}"
            else:
                name = str(h).strip()
            count = seen.get(name, 0)
            seen[name] = count + 1
            result.append(f"{name}_{count}" if count > 0 else name)
        return result
