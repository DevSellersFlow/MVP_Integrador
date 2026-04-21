"""
ai_engine.py
============
Camada de IA para o SellersFlow.

Funcionalidades:
  1. suggest_mapping()     → sugestão de coluna Amazon para coluna destino
  2. enrich_row()          → enriquecimento de título, descrição, bullets
  3. normalize_with_ai()   → normalização de campos complexos via LLM

Design:
  - Usa a API Anthropic (claude-sonnet-4-20250514)
  - Cache de resultados em memória para evitar chamadas redundantes
  - Cada método retorna um dict padronizado com "result" e "confidence"
  - Falhas são logadas e retornam None (nunca propagam exceção)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Optional

import anthropic

logger = logging.getLogger(__name__)

# ─── Cliente ──────────────────────────────────────────────────────────────────

def _get_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    return anthropic.Anthropic(api_key=api_key)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _cache_key(*args) -> str:
    raw = json.dumps(args, ensure_ascii=False, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


def _call_llm(prompt: str, max_tokens: int = 512) -> Optional[str]:
    try:
        client = _get_client()
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as exc:
        logger.error("Erro na chamada LLM: %s", exc)
        return None


def _parse_json(text: Optional[str]) -> Optional[dict]:
    if not text:
        return None
    # Remove fences de markdown se presentes
    clean = text.replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        logger.warning("Resposta LLM não é JSON válido: %s", text[:200])
        return None


# ─── AIEngine ────────────────────────────────────────────────────────────────

class AIEngine:
    """
    Motor de IA para enriquecimento e sugestão de mapeamento.
    Thread-safe (cache em dict simples — suficiente para uso single-process).
    """

    def __init__(self):
        self._cache: dict[str, any] = {}

    # ── 1. Sugestão de mapeamento ─────────────────────────────────────────────

    def suggest_mapping(
        self,
        dest_col: str,
        marketplace: str,
        amazon_columns: list[str],
    ) -> Optional[dict]:
        """
        Sugere qual coluna da Amazon corresponde à coluna destino.

        Returns:
            {"source_col": str, "confidence": float, "reasoning": str} | None
        """
        key = _cache_key("suggest_mapping", dest_col, marketplace, sorted(amazon_columns))
        if key in self._cache:
            return self._cache[key]

        amazon_list = "\n".join(f"- {c}" for c in amazon_columns[:80])
        prompt = f"""Você é um especialista em catálogos de marketplaces e-commerce.

Marketplace destino: {marketplace}
Coluna destino que precisa ser preenchida: "{dest_col}"

Colunas disponíveis da planilha Amazon:
{amazon_list}

Tarefa: Identifique qual coluna Amazon contém os dados para preencher "{dest_col}".

Responda APENAS com JSON válido no formato:
{{
  "source_col": "<nome exato da coluna Amazon ou null se não há match>",
  "confidence": <0.0 a 1.0>,
  "reasoning": "<explicação em 1 linha>"
}}"""

        raw = _call_llm(prompt, max_tokens=200)
        result = _parse_json(raw)
        self._cache[key] = result
        return result

    # ── 2. Enriquecimento de linha ─────────────────────────────────────────────

    def enrich_row(
        self,
        row_data: dict,
        marketplace: str,
        language: str = "pt-BR",
    ) -> Optional[dict]:
        """
        Enriquece título, descrição e bullets de um produto.

        Args:
            row_data: Dicionário com os campos da linha (campo → valor).
            marketplace: Nome do marketplace destino.
            language: Idioma do enriquecimento.

        Returns:
            {
                "title": str,
                "description": str,
                "bullets": list[str],
                "confidence": float
            } | None
        """
        key = _cache_key("enrich_row", str(row_data), marketplace, language)
        if key in self._cache:
            return self._cache[key]

        # Campos relevantes para o prompt — busca case-insensitive
        _RELEVANT = {
            "nome_produto", "item name", "product name", "título", "titulo",
            "marca", "brand", "brand name",
            "descricao", "product description", "descrição do produto",
            "sabor", "flavour", "cor", "color", "tamanho", "size",
        }
        relevant = {
            k: v for k, v in row_data.items()
            if k.lower() in _RELEVANT
            and str(v).strip() not in ("", "nan", "None")
        }

        prompt = f"""Você é um copywriter especialista em e-commerce {marketplace}.

Dados atuais do produto:
{json.dumps(relevant, ensure_ascii=False, indent=2)}

Idioma de saída: {language}
Marketplace: {marketplace}

Melhore os dados do produto para aumentar conversão. Siga as boas práticas do {marketplace}.

Responda APENAS com JSON válido:
{{
  "title": "<título otimizado, máx 150 chars>",
  "description": "<descrição melhorada, 2-4 frases, foco em benefícios>",
  "bullets": [
    "<ponto chave 1>",
    "<ponto chave 2>",
    "<ponto chave 3>",
    "<ponto chave 4>",
    "<ponto chave 5>"
  ],
  "confidence": <0.0 a 1.0>
}}"""

        raw = _call_llm(prompt, max_tokens=600)
        result = _parse_json(raw)
        self._cache[key] = result
        return result

    # ── 3. Normalização inteligente ────────────────────────────────────────────

    def normalize_with_ai(
        self,
        field_name: str,
        value: str,
        marketplace: str,
    ) -> Optional[dict]:
        """
        Normaliza um valor de campo usando LLM.
        Útil para casos não cobertos pelas tabelas estáticas.

        Returns:
            {"normalized": str, "confidence": float, "reasoning": str} | None
        """
        key = _cache_key("normalize", field_name, value, marketplace)
        if key in self._cache:
            return self._cache[key]

        prompt = f"""Normalize o seguinte valor de campo de catálogo e-commerce.

Campo: {field_name}
Valor original: "{value}"
Marketplace: {marketplace}

Regras:
- Cor: Use nome canônico em Português BR (ex: "azul claro")
- Tamanho: Use padrão BR (PP, P, M, G, GG, XG, XXG ou número)
- Unidades: Converta para padrão métrico se necessário
- Capitalize corretamente

Responda APENAS com JSON:
{{
  "normalized": "<valor normalizado>",
  "confidence": <0.0 a 1.0>,
  "reasoning": "<explicação>"
}}"""

        raw = _call_llm(prompt, max_tokens=150)
        result = _parse_json(raw)
        self._cache[key] = result
        return result

    # ── Cache management ──────────────────────────────────────────────────────

    def clear_cache(self) -> None:
        self._cache.clear()
        logger.info("Cache do AIEngine limpo.")

    @property
    def cache_size(self) -> int:
        return len(self._cache)