"""
normalizer.py
=============
Normalização inteligente de valores de campos.
Converte variações de entrada em formas canônicas padronizadas.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Optional

import pandas as pd


# ─────────────────────────────────────────────
# Tabelas de normalização
# ─────────────────────────────────────────────

COLOR_MAP: dict[str, str] = {
    # pt-br
    "azul": "Azul", "azul claro": "Azul Claro", "azul escuro": "Azul Escuro",
    "azul marinho": "Azul Marinho", "azul royal": "Azul Royal",
    "vermelho": "Vermelho", "verde": "Verde", "verde claro": "Verde Claro",
    "verde escuro": "Verde Escuro", "amarelo": "Amarelo", "laranja": "Laranja",
    "roxo": "Roxo", "violeta": "Violeta", "rosa": "Rosa", "rosa claro": "Rosa Claro",
    "preto": "Preto", "branco": "Branco", "cinza": "Cinza", "cinza claro": "Cinza Claro",
    "cinza escuro": "Cinza Escuro", "marrom": "Marrom", "bege": "Bege",
    "dourado": "Dourado", "prateado": "Prateado", "prata": "Prateado",
    "ouro": "Dourado", "creme": "Creme", "nude": "Nude", "coral": "Coral",
    "turquesa": "Turquesa", "vinho": "Vinho", "bordo": "Bordô", "caramelo": "Caramelo",
    # en
    "blue": "Azul", "light blue": "Azul Claro", "dark blue": "Azul Escuro",
    "navy": "Azul Marinho", "navy blue": "Azul Marinho",
    "red": "Vermelho", "green": "Verde", "light green": "Verde Claro",
    "dark green": "Verde Escuro", "yellow": "Amarelo", "orange": "Laranja",
    "purple": "Roxo", "violet": "Violeta", "pink": "Rosa", "light pink": "Rosa Claro",
    "black": "Preto", "white": "Branco", "gray": "Cinza", "grey": "Cinza",
    "light gray": "Cinza Claro", "dark gray": "Cinza Escuro",
    "brown": "Marrom", "beige": "Bege", "gold": "Dourado", "golden": "Dourado",
    "silver": "Prateado", "cream": "Creme", "turquoise": "Turquesa",
    "wine": "Vinho", "caramel": "Caramelo",
}

SIZE_MAP: dict[str, str] = {
    # Vestuário
    "pp": "PP", "p": "P", "m": "M", "g": "G", "gg": "GG",
    "xg": "XG", "xxg": "XXG", "3g": "3G", "4g": "4G",
    "xs": "XS", "s": "S", "l": "L", "xl": "XL",
    "xxl": "XXL", "2xl": "XXL", "xxxl": "3XL", "3xl": "3XL",
    "extra pequeno": "PP", "extra small": "PP",
    "pequeno": "P", "small": "P",
    "medio": "M", "médio": "M", "medium": "M",
    "grande": "G", "large": "G",
    "extra grande": "XG", "extra large": "XG",
    "extra extra grande": "XXG",
    # Calçados (numérico, preservado)
    # Único / OS
    "u": "Único", "un": "Único", "unico": "Único", "único": "Único",
    "one size": "Único", "os": "Único", "tamanho único": "Único",
}

UNIT_CONVERSIONS: dict[tuple[str, str], float] = {
    # Para marketplaces destino (Amazon → outros)
    ("lb", "kg"): 0.453592,
    ("oz", "kg"): 0.0283495,
    ("oz", "g"): 28.3495,
    ("in", "cm"): 2.54,
    ("in", "m"): 0.0254,
    ("ft", "cm"): 30.48,
    ("ft", "m"): 0.3048,
    ("cm", "m"): 0.01,
    ("fl_oz", "ml"): 29.5735,
    # Para Amazon como destino (outros → Amazon)
    ("kg", "lb"): 2.20462,
    ("g", "lb"): 0.00220462,
    ("cm", "in"): 0.393701,
    ("m", "in"): 39.3701,
    ("m", "cm"): 100.0,
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def _to_key(text: str) -> str:
    """Converte para chave de lookup: lowercase + sem acentos + strip."""
    return _strip_accents(str(text).strip().lower())


# ─────────────────────────────────────────────
# Dataclass de resultado
# ─────────────────────────────────────────────

@dataclass
class NormalizationResult:
    original: str
    normalized: str
    method: str          # "map", "numeric", "passthrough"
    confidence: float    # 0.0–1.0


# ─────────────────────────────────────────────
# Normalizer principal
# ─────────────────────────────────────────────

class FieldNormalizer:
    """Normaliza valores individuais de campos de catálogo."""

    # ── Cor ─────────────────────────────────────────────

    def normalize_color(self, value: str) -> NormalizationResult:
        if pd.isna(value) or str(value).strip() == "":
            return NormalizationResult(value, "", "passthrough", 0.0)
        key = _to_key(value)
        if key in COLOR_MAP:
            return NormalizationResult(value, COLOR_MAP[key], "map", 1.0)
        # Tenta match parcial
        for k, v in COLOR_MAP.items():
            if k in key or key in k:
                return NormalizationResult(value, v, "partial_map", 0.75)
        return NormalizationResult(value, str(value).strip().title(), "passthrough", 0.5)

    # ── Tamanho ──────────────────────────────────────────

    def normalize_size(self, value: str) -> NormalizationResult:
        if pd.isna(value) or str(value).strip() == "":
            return NormalizationResult(value, "", "passthrough", 0.0)
        key = _to_key(value)
        if key in SIZE_MAP:
            return NormalizationResult(value, SIZE_MAP[key], "map", 1.0)
        # Numérico (calçado, etc.)
        if re.match(r"^\d{1,3}(\.\d)?$", str(value).strip()):
            return NormalizationResult(value, str(value).strip(), "numeric", 1.0)
        return NormalizationResult(value, str(value).strip().upper(), "passthrough", 0.5)

    # ── Peso ─────────────────────────────────────────────

    def normalize_weight(
        self,
        value: float | str,
        from_unit: str = "lb",
        to_unit: str = "kg",
    ) -> NormalizationResult:
        try:
            v = float(str(value).strip().replace(",", "."))
        except (ValueError, AttributeError):
            return NormalizationResult(value, value, "passthrough", 0.0)
        factor = UNIT_CONVERSIONS.get((from_unit.lower(), to_unit.lower()), 1.0)
        converted = round(v * factor, 4)
        return NormalizationResult(value, converted, "conversion", 1.0)

    # ── Dimensão ─────────────────────────────────────────

    def normalize_dimension(
        self,
        value: float | str,
        from_unit: str = "in",
        to_unit: str = "cm",
    ) -> NormalizationResult:
        return self.normalize_weight(value, from_unit, to_unit)

    # ── Preço ────────────────────────────────────────────

    def normalize_price(self, value: str | float) -> NormalizationResult:
        """Remove símbolos de moeda e normaliza separador decimal."""
        if pd.isna(value):
            return NormalizationResult(value, "", "passthrough", 0.0)
        cleaned = re.sub(r"[^\d,\.]", "", str(value))
        # Detecta padrão BR (vírgula como decimal): 1.234,56
        if re.match(r"^\d{1,3}(\.\d{3})*(,\d{1,2})?$", cleaned):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        try:
            return NormalizationResult(value, float(cleaned), "numeric", 1.0)
        except ValueError:
            return NormalizationResult(value, value, "passthrough", 0.3)

    # ── Dispatcher ───────────────────────────────────────

    def normalize_field(
        self,
        field_type: str,
        value,
        **kwargs,
    ) -> NormalizationResult:
        """Dispara o normalizador correto com base no tipo de campo."""
        dispatch = {
            "cor": self.normalize_color,
            "color": self.normalize_color,
            "tamanho": self.normalize_size,
            "size": self.normalize_size,
            "peso_pacote": lambda v: self.normalize_weight(v, **kwargs),
            "package_weight": lambda v: self.normalize_weight(v, **kwargs),
            "comprimento_pacote": lambda v: self.normalize_dimension(v, **kwargs),
            "largura_pacote": lambda v: self.normalize_dimension(v, **kwargs),
            "altura_pacote": lambda v: self.normalize_dimension(v, **kwargs),
            "preco": self.normalize_price,
            "price": self.normalize_price,
        }
        fn = dispatch.get(field_type.lower())
        if fn:
            return fn(value)
        return NormalizationResult(value, value, "passthrough", 1.0)