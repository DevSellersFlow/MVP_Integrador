"""
reader.py
=========
Leitura e parsing de planilhas Amazon.
Suporta múltiplas versões de template (US / BR) e
retorna um DataFrame normalizado + metadados.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ─── Configuração por versão de template ─────────────────────────────────────

AMAZON_TEMPLATE_CONFIG = {
    "default": {
        # Nomes das abas a tentar (em ordem de prioridade)
        "sheet_candidates": ["Template", "Modelo"],
    }
}

# Valores que identificam com certeza uma linha de cabeçalho Amazon (US ou BR)
HEADER_SIGNALS: list[str] = [
    # EN
    "seller sku", "item name", "brand name", "product type",
    "record action", "product description", "bullet point",
    "package weight", "country of origin",
    # PT-BR
    "sku do vendedor", "nome do produto", "nome da marca",
    "descrição do produto", "tópico", "peso do pacote",
    "país de origem", "preço", "estoque",
]

# Padrões que indicam que uma linha NÃO é cabeçalho (metadados, IDs internos, etc.)
SKIP_PATTERNS: tuple[str, ...] = (
    "settings=", "reserved line", "templatetype=",
)

LANGUAGE_SIGNALS = {
    "BR": ["nome", "descrição", "preço", "quantidade", "estoque", "código"],
    "US": ["item name", "your price", "quantity", "product description"],
}


# ─── Dataclasses de resultado ─────────────────────────────────────────────────

@dataclass
class AmazonReadResult:
    df: pd.DataFrame
    language: str                        # "BR" | "US"
    sheet_name: str
    total_rows: int
    valid_rows: int
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)


# ─── Reader ───────────────────────────────────────────────────────────────────

class AmazonSheetReader:
    """
    Lê e processa planilhas de catálogo Amazon.

    Responsabilidades:
    - Detectar e abrir a aba correta
    - Identificar linha de cabeçalho
    - Detectar idioma (PT-BR / EN-US)
    - Retornar DataFrame limpo + metadados
    """

    def __init__(self, config: Optional[dict] = None):
        self._config = config or AMAZON_TEMPLATE_CONFIG["default"]

    # ── Auto-detecção ──────────────────────────────────────────────────────────

    @staticmethod
    def _find_header_row(df_raw: pd.DataFrame, max_scan: int = 20) -> Optional[int]:
        """
        Encontra a linha de cabeçalho: primeira linha com ≥2 valores que
        correspondem a nomes conhecidos de colunas Amazon (US ou BR).
        Retorna índice 0-based ou None.
        """
        for i in range(min(max_scan, len(df_raw))):
            row_vals = df_raw.iloc[i].dropna().astype(str).str.strip()
            if len(row_vals) == 0:
                continue
            # Pula linhas de metadados/instruções
            first = row_vals.iloc[0].lower()
            if any(p in first for p in SKIP_PATTERNS):
                continue
            if len(first) > 300:
                continue
            # Conta matches com sinais de cabeçalho
            row_lower = row_vals.str.lower()
            matches = sum(
                1 for sig in HEADER_SIGNALS
                if row_lower.str.contains(sig, regex=False).any()
            )
            if matches >= 2:
                return i
        return None

    @staticmethod
    def _find_data_start(df_raw: pd.DataFrame, header_idx: int, max_scan: int = 8) -> int:
        """
        Encontra a primeira linha de dados reais após o cabeçalho.
        Pula linhas de IDs internos (contêm # ou [) e linha de exemplo padrão.
        """
        for i in range(header_idx + 1, header_idx + 1 + max_scan):
            if i >= len(df_raw):
                break
            row = df_raw.iloc[i].dropna().astype(str)
            if len(row) == 0:
                continue
            first = row.iloc[0].strip()
            # Pula IDs internos: contribution_sku#1.value, item_name[...], etc.
            if '#' in first or '[' in first:
                continue
            # Pula metadados longos
            if len(first) > 200 or any(p in first.lower() for p in SKIP_PATTERNS):
                continue
            # Pula linha de exemplo padrão da Amazon
            if first.upper() in ("ABC123", "EXAMPLE", "EXEMPLO"):
                continue
            return i
        return header_idx + 1  # fallback

    # ── Pública ───────────────────────────────────────────────────────────────

    def read(self, file_like: io.BytesIO | str) -> AmazonReadResult:
        """
        Lê o arquivo e retorna AmazonReadResult.

        Args:
            file_like: BytesIO ou caminho para o arquivo Excel.

        Returns:
            AmazonReadResult com DataFrame e metadados.
        """
        warnings: list[str] = []
        errors: list[str] = []

        # ── 1. Abrir workbook ─────────────────────────────────────────────
        try:
            if hasattr(file_like, "seek"):
                file_like.seek(0)
            xl = pd.ExcelFile(file_like)
        except Exception as exc:
            return AmazonReadResult(
                df=pd.DataFrame(),
                language="UNKNOWN",
                sheet_name="",
                total_rows=0,
                valid_rows=0,
                errors=[f"Não foi possível abrir o arquivo: {exc}"],
            )

        # ── 2. Resolver aba ───────────────────────────────────────────────
        sheet_name = self._resolve_sheet(xl.sheet_names, warnings)
        if sheet_name is None:
            return AmazonReadResult(
                df=pd.DataFrame(),
                language="UNKNOWN",
                sheet_name="",
                total_rows=0,
                valid_rows=0,
                errors=["Nenhuma aba de dados encontrada no arquivo. Verifique se o arquivo é uma planilha Amazon válida."],
            )

        # ── 3. Ler raw ────────────────────────────────────────────────────
        try:
            if hasattr(file_like, "seek"):
                file_like.seek(0)
            df_raw = pd.read_excel(
                file_like,
                sheet_name=sheet_name,
                header=None,
                dtype=str,
            )
        except Exception as exc:
            return AmazonReadResult(
                df=pd.DataFrame(),
                language="UNKNOWN",
                sheet_name=sheet_name,
                total_rows=0,
                valid_rows=0,
                errors=[f"Erro ao ler aba '{sheet_name}': {exc}"],
            )

        # ── 4. Auto-detectar cabeçalho e início dos dados ─────────────────
        header_row_idx = self._find_header_row(df_raw)
        if header_row_idx is None:
            return AmazonReadResult(
                df=pd.DataFrame(),
                language="UNKNOWN",
                sheet_name=sheet_name,
                total_rows=0,
                valid_rows=0,
                errors=["Linha de cabeçalho não encontrada. Verifique se a planilha é um template Amazon válido."],
            )
        data_start_idx = self._find_data_start(df_raw, header_row_idx)

        logger.info(
            "Amazon auto-detect: sheet=%s header_row=%d data_start=%d",
            sheet_name, header_row_idx + 1, data_start_idx + 1,
        )

        if df_raw.shape[0] <= header_row_idx:
            return AmazonReadResult(
                df=pd.DataFrame(),
                language="UNKNOWN",
                sheet_name=sheet_name,
                total_rows=0,
                valid_rows=0,
                errors=["Arquivo muito curto; cabeçalho não encontrado."],
            )

        raw_headers = df_raw.iloc[header_row_idx].tolist()
        headers = self._sanitize_headers(raw_headers)

        # ── 5. Montar DataFrame de dados ──────────────────────────────────
        df_data = df_raw.iloc[data_start_idx:].copy()
        df_data.columns = headers
        df_data = df_data.dropna(how="all").reset_index(drop=True)

        # ── 6. Detectar idioma ────────────────────────────────────────────
        language = self._detect_language(headers)

        total_rows = len(df_data)
        # Filtra linhas que tem pelo menos 1 campo não-nulo diferente de ""
        valid_mask = df_data.apply(
            lambda r: r.dropna().astype(str).str.strip().ne("").any(), axis=1
        )
        df_data = df_data[valid_mask].reset_index(drop=True)
        valid_rows = len(df_data)

        if valid_rows == 0:
            warnings.append("Nenhuma linha de dados válida encontrada na planilha Amazon.")

        logger.info(
            "Amazon lida: sheet=%s lang=%s total=%d valid=%d",
            sheet_name, language, total_rows, valid_rows,
        )

        return AmazonReadResult(
            df=df_data,
            language=language,
            sheet_name=sheet_name,
            total_rows=total_rows,
            valid_rows=valid_rows,
            warnings=warnings,
            errors=errors,
        )

    # ── Privadas ──────────────────────────────────────────────────────────────

    def _resolve_sheet(self, sheets: list[str], warnings: list[str]) -> Optional[str]:
        # 1. Candidatos exatos (Template, Modelo)
        candidates = self._config.get("sheet_candidates", ["Template", "Modelo"])
        for candidate in candidates:
            if candidate in sheets:
                return candidate

        # 2. Aba que contém "template" ou "modelo" (case-insensitive)
        for keyword in ("template", "modelo"):
            for s in sheets:
                if keyword in s.lower():
                    warnings.append(f"Aba exata não encontrada; usando '{s}'.")
                    return s

        # 3. Fallback amplo: ignora abas de instrução/navegação e retorna
        #    a primeira aba visível restante. Cobre planilhas Amazon exportadas
        #    com nomes como "Detalhes do produto", "Inventário", etc.
        _SKIP_NAMES = {
            "instruções", "instrucoes", "instructions", "leiame", "readme",
            "ajuda", "help", "sobre", "about", "dropdown", "lists",
            "conditions", "cover", "inicio", "início", "índice", "indice",
        }
        for s in sheets:
            s_lower = s.strip().lower()
            if not any(skip in s_lower for skip in _SKIP_NAMES):
                warnings.append(
                    f"Abas 'Template'/'Modelo' não encontradas; "
                    f"usando primeira aba de dados: '{s}'."
                )
                return s

        # 4. Último recurso: primeira aba disponível
        if sheets:
            warnings.append(f"Usando primeira aba disponível: '{sheets[0]}'.")
            return sheets[0]

        return None

    @staticmethod
    def _sanitize_headers(raw: list) -> list[str]:
        """
        Garante cabeçalhos únicos e strings válidas.
        Colunas sem nome ficam como _col_N.
        """
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

    @staticmethod
    def _detect_language(headers: list[str]) -> str:
        joined = " ".join(h.lower() for h in headers)
        br_score = sum(1 for sig in LANGUAGE_SIGNALS["BR"] if sig in joined)
        us_score = sum(1 for sig in LANGUAGE_SIGNALS["US"] if sig in joined)
        return "BR" if br_score >= us_score else "US"