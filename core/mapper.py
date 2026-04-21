"""
mapper.py
=========
Motor de mapeamento multi-estratégia com scoring de confiança.

Estratégias (em ordem de prioridade):
  1. Mapeamento aprendido
  2. Mapeamento fixo + sinônimos
  3. Similaridade de string (SequenceMatcher)
  4. IA (via AIEngine — fallback)

MUDANÇAS:
  - Grupos multi-coluna detectam padrões _N (underscore) além de espaço+N.
    O AmazonSheetReader sanitiza colunas duplicadas como "Bullet Point",
    "Bullet Point_1", "Bullet Point_2" — o mapper agora reconhece esse padrão.
  - Colunas destino com nome IDÊNTICO repetido (ex: 5x "Tópico" no Vendor)
    são tratadas corretamente: cada ocorrência consome a próxima posição
    do grupo multi-coluna, em ordem de col_idx.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from dataclasses import dataclass, field, asdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ─── Configuração ─────────────────────────────────────────────────────────────

SIMILARITY_THRESHOLD = 0.72

AMAZON_SYNONYMS: dict[str, list[str]] = {
    "sku": ["sku", "seller sku", "sellers sku", "sku do vendedor"],
    "nome_produto": ["item name", "nome do produto", "product name", "título", "titulo"],
    "marca": ["brand name", "nome da marca", "marca", "brand"],
    "descricao": [
        "product description", "descrição do produto", "description",
        "descrição", "descricao",
    ],
    "quantidade": ["Quantity (US)", "quantidade (br)", "estoque", "stock", "qtd"],
    "preco": [
        "your price usd (sell on amazon, us)", "preço", "preco", "price",
        "preço de venda", "Preço padrão BRL (Vender na Amazon, BR)", "Preço sugerido com impostos",
    ],
    # bullet_point é tratado como grupo multi-coluna
    "bullet_point": ["Bullet Point", "tópico", "topico", "key feature"],
    "peso_pacote": ["package weight", "peso do pacote", "peso"],
    "unidade_peso": ["Package Weight Unit", "Unidade de peso do pacote"],
    "comprimento_pacote": [
        "item package length", "comprimento do pacote", "comprimento",
    ],
    "unidade_comprimento": ["Package Length Unit", "Unidade de comprimento do pacote"],
    "largura_pacote": ["item package width", "largura do pacote", "width", "largura"],
    "unidade_largura": ["Package Width Unit", "Unidade de largura do pacote"],
    "altura_pacote": ["item package height", "altura do pacote", "height", "altura"],
    "unidade_altura": ["Package Height Unit", "Unidade de altura do pacote"],
    "id_produto": ["external product id", "id do produto", "ean", "gtin", "upc", "asin"],
    "tipo_id_produto": [
        "external product id type", "tipo de id do produto",
        "tipo id", "id type",
    ],
    "ncm": ["código ncm", "ncm", "codigo ncm"],
    "sabor": ["flavour", "flavor", "sabor", "flavors"],
    "cor": ["color", "colour", "cor"],
    "tamanho": ["size", "tamanho"],
    "fabricante": ["manufacturer", "fabricante"],
    "tipo_produto": ["item type keyword", "tipo de produto", "product type"],
    "hierarquia": ["parentage level", "nível de hierarquia", "nivel de hierarquia"],
    "sku_pai": ["parent sku", "sku do produto pai", "sku pai"],
    "pais_origem": ["country of origin", "país de origem", "pais de origem"],
    "origem_mercadoria": ["Origem da mercadoria"],
    "cest": ["Código Especificador da Substituição Tributária (CEST)", "CEST"],
    "material": ["Material"]
}

MARKETPLACE_MAPPINGS: dict[str, dict[str, list[str]]] = {
    "Vendor": {
        "sku do fornecedor": ["sku"],
        "nome do produto": ["nome_produto"],
        "nome da marca": ["marca"],
        "descrição do produto": ["descricao"],
        "Preço sugerido com impostos": ["preco"],
        "código ncm": ["ncm"],
	"Origem da mercadoria": ["origem_mercadoria"],
	"Código Especificador da Substituição Tributária (CEST)": ["cest"],
        "sabor": ["sabor"],
        "cor": ["cor"],
        "tamanho": ["tamanho"],
        # "Tópico" — as 5 colunas idênticas no template são tratadas como grupo
        "Tópico": ["bullet_point"],
        "peso do pacote": ["peso_pacote"],
	"Unidade de peso do pacote": ["unidade_peso"],
        "comprimento do pacote": ["comprimento_pacote"],
	"Unidade de comprimento do pacote": ["unidade_comprimento"],
        "largura do pacote": ["largura_pacote"],
	"Unidade de largura do pacote": ["unidade_largura"],
        "altura do pacote": ["altura_pacote"],
	"Unidade de altura do pacote": ["unidade_altura"],
        "fabricante": ["fabricante"],
        "id externo do produto": ["id_produto"],
        "tipo de id externo do produto": ["tipo_id_produto"],
        "tipo de produto": ["tipo_produto"],
        "nível de hierarquia": ["hierarquia"],
        "sku do produto pai": ["sku_pai"],
        "país de origem": ["pais_origem"],
	"Material": ["material"],
    },
    "Temu": {
        # Nomes exatos da linha 2 do template temu_template_integrador.xlsx
        # cols 1-4 "Processing Results" são preenchidas pela Temu — não mapeadas
        # O SKU do vendedor vai em "Contribution SKU" (col 9, variações)
        # e "Contribution Goods" (col 8, produto pai / produto simples)
        "Contribution Goods": ["sku"],        # SKU pai (col 8)
        "Contribution SKU": ["sku"],          # SKU da variação (col 9)
        "Product Name": ["nome_produto"],     # col 7
        "Brand": ["marca"],                   # col 13
        "Product Description": ["descricao"], # col 15
        "Bullet Point": ["bullet_point"],     # cols 16-21
        "Base Price - USD": ["preco"],        # col 108
        "Flavors": ["sabor"],                 # col 87
        "Color": ["cor"],                     # col 83
        "Size": ["tamanho"],                  # col 84
        "Weight - lb": ["peso_pacote"],       # col 111
        "Length - in": ["comprimento_pacote"],# col 112
        "Width - in": ["largura_pacote"],     # col 113
        "Height - in": ["altura_pacote"],     # col 114
        "External Product ID Type": ["tipo_id_produto"], # col 115
        "External Product ID": ["id_produto"],           # col 116
        "Country/Region of Origin": ["pais_origem"],     # col 121
    },
    "Shopee": {
        "sku principal": ["sku"],
        "nome do produto": ["nome_produto"],
        "descrição do produto": ["descricao"],
        "preço": ["preco"],
        "estoque": ["quantidade"],
        "gtin (ean)": ["id_produto"],
        "ncm": ["ncm"],
	"Origem": ["origem_mercadoria"],
	"CEST": ["cest"],
	"Peso": ["peso_pacote"],
	"Comprimento": ["comprimento_pacote"],
	"Altura": ["altura_pacote"],
	"Largura": ["largura_pacote"],
    },
    "Mercado Livre": {
        # Coluna de título do ML (nome longo) → nome_produto da Amazon
        "título: informe o produto, marca, modelo e destaque as características principais \ncaso crie variações, você deve criar um título geral para todas": ["nome_produto"],
        # Aliases curtos para robustez (similaridade / aprendizado)
        "título": ["nome_produto"],
        "codigo universal de produto": ["id_produto"],
        "sku": ["sku"],
        "estoque": ["quantidade"],
	"Preço [R$]": ["preco"],
	"Descrição": ["descricao"],
	"Largura (cm)": ["largura_pacote"],
	"Altura (cm)": ["altura_pacote"],
	"Profundidade (cm)": ["comprimento_pacote"],
	"Peso físico (kg)  \nEmbalagem com o produto dentro.": ["peso_pacote"],
	"Marca": ["marca"],
    },
    "Amazon": {
        # Colunas do template Amazon → chaves semânticas
        # Usado quando Amazon é o marketplace DESTINO
        # EN
        "item name": ["nome_produto"],
        "seller sku": ["sku"],
        "brand name": ["marca"],
        "product description": ["descricao"],
        "bullet point": ["bullet_point"],
        "package weight": ["peso_pacote"],
        "item package length": ["comprimento_pacote"],
        "item package width": ["largura_pacote"],
        "item package height": ["altura_pacote"],
        "external product id": ["id_produto"],
        "external product id type": ["tipo_id_produto"],
        "country of origin": ["pais_origem"],
        # PT-BR
        "nome do produto": ["nome_produto"],
        "sku do vendedor": ["sku"],
        "nome da marca": ["marca"],
        "descrição do produto": ["descricao"],
        "tópico": ["bullet_point"],
        "peso do pacote": ["peso_pacote"],
        "comprimento do pacote": ["comprimento_pacote"],
        "largura do pacote": ["largura_pacote"],
        "altura do pacote": ["altura_pacote"],
        "código ncm": ["ncm"],
        "país de origem": ["pais_origem"],
        "preço": ["preco"],
        "estoque": ["quantidade"],
    },
    "Magalu": {
        "SKU":             ["sku"],
        "EAN":             ["id_produto"],
        "NCM":             ["ncm"],
        "TITULO_PRODUTO":  ["nome_produto"],
        "DESCRICAO_ITEM":  ["descricao"],
        "MARCA / Editora": ["marca"],
        "PESO":            ["peso_pacote"],
        "ALTURA":          ["altura_pacote"],
        "LARGURA":         ["largura_pacote"],
        "COMPRIMENTO":     ["comprimento_pacote"],
    },
}

REQUIRED_FIELDS: dict[str, list[str]] = {
    # Valores devem bater EXATAMENTE com os cabeçalhos do template
    # (após .strip().lower()) para que _validate_output os encontre.
    "Shopee": ["SKU Principal", "Nome do Produto", "Preço", "Estoque"],
    "Temu": ["Contribution Goods", "Product Name"],
    "Vendor": ["SKU do fornecedor", "Nome do Produto"],
    "Magalu": ["SKU", "TITULO_PRODUTO"],
}


# ─── Mapeamento de ORIGEM: campo do marketplace → chave semântica ─────────────
#
# Usado quando um marketplace é a FONTE dos dados (não o destino).
# Após normalize_source_df(), o DataFrame terá colunas com chaves semânticas
# independente de qual marketplace originou os dados.

SOURCE_MAPPINGS: dict[str, dict[str, str]] = {
    "Amazon": {
        "item name": "nome_produto",
        "seller sku": "sku",
        "brand name": "marca",
        "product description": "descricao",
        "bullet point": "bullet_point",
        "package weight": "peso_pacote",
        "item package length": "comprimento_pacote",
        "item package width": "largura_pacote",
        "item package height": "altura_pacote",
        "external product id": "id_produto",
        "external product id type": "tipo_id_produto",
        "country of origin": "pais_origem",
        "nome do produto": "nome_produto",
        "sku do vendedor": "sku",
        "nome da marca": "marca",
        "descricao do produto": "descricao",
        "topico": "bullet_point",
        "peso do pacote": "peso_pacote",
        "comprimento do pacote": "comprimento_pacote",
        "largura do pacote": "largura_pacote",
        "altura do pacote": "altura_pacote",
        "codigo ncm": "ncm",
        "pais de origem": "pais_origem",
        "preco": "preco",
        "estoque": "quantidade",
    },
    "Mercado Livre": {
        "titulo": "nome_produto",
        "codigo universal de produto": "id_produto",
        "sku": "sku",
        "estoque": "quantidade",
        "preco [r$]": "preco",
        "descricao": "descricao",
        "largura (cm)": "largura_pacote",
        "altura (cm)": "altura_pacote",
        "profundidade (cm)": "comprimento_pacote",
        "peso fisico (kg)  \nembalagem com o produto dentro.": "peso_pacote",
        "marca": "marca",
    },
    "Shopee": {
        "sku principal": "sku",
        "nome do produto": "nome_produto",
        "descricao do produto": "descricao",
        "preco": "preco",
        "estoque": "quantidade",
        "gtin (ean)": "id_produto",
        "ncm": "ncm",
        "peso": "peso_pacote",
        "comprimento": "comprimento_pacote",
        "altura": "altura_pacote",
        "largura": "largura_pacote",
    },
    "Temu": {
        "contribution goods": "sku",
        "contribution sku": "sku",
        "product name": "nome_produto",
        "brand": "marca",
        "product description": "descricao",
        "bullet point": "bullet_point",
        "base price - usd": "preco",
        "flavors": "sabor",
        "color": "cor",
        "size": "tamanho",
        "weight - lb": "peso_pacote",
        "length - in": "comprimento_pacote",
        "width - in": "largura_pacote",
        "height - in": "altura_pacote",
        "external product id type": "tipo_id_produto",
        "external product id": "id_produto",
        "country/region of origin": "pais_origem",
    },
    "Vendor": {
        "sku do fornecedor": "sku",
        "nome do produto": "nome_produto",
        "nome da marca": "marca",
        "descricao do produto": "descricao",
        "preco sugerido com impostos": "preco",
        "codigo ncm": "ncm",
        "topico": "bullet_point",
        "peso do pacote": "peso_pacote",
        "comprimento do pacote": "comprimento_pacote",
        "largura do pacote": "largura_pacote",
        "altura do pacote": "altura_pacote",
        "id externo do produto": "id_produto",
        "pais de origem": "pais_origem",
        "fabricante": "fabricante",
        "cor": "cor",
        "tamanho": "tamanho",
    },
    "Magalu": {
        "sku": "sku",
        "ean": "id_produto",
        "ncm": "ncm",
        "titulo_produto": "nome_produto",
        "descricao_item": "descricao",
        "marca / editora": "marca",
        "peso": "peso_pacote",
        "altura": "altura_pacote",
        "largura": "largura_pacote",
        "comprimento": "comprimento_pacote",
    },
}

# Prefixos normalizados de grupos multi-coluna.
# O AmazonSheetReader sanitiza duplicatas como "Bullet Point", "Bullet Point_1"...
# Este mapper detecta as variantes: com espaço+N, com _N, ou sem sufixo (primeiro).
MULTI_COLUMN_GROUPS: dict[str, list[str]] = {
    "bullet_point": ["bullet point", "topico", "key feature"],
}


# ─── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class FieldMappingDecision:
    dest_col: str
    source_col: Optional[str]
    source_idx: Optional[int]
    strategy: str
    confidence: float
    notes: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MappingResult:
    marketplace: str
    decisions: list[FieldMappingDecision] = field(default_factory=list)
    index_map: dict[int, int] = field(default_factory=dict)
    unmapped_dest: list[str] = field(default_factory=list)
    unmapped_source: list[str] = field(default_factory=list)

    @property
    def coverage(self) -> float:
        mapped = sum(1 for d in self.decisions if d.strategy != "unmapped")
        total = len(self.decisions)
        return mapped / total if total else 0.0

    @property
    def avg_confidence(self) -> float:
        scores = [d.confidence for d in self.decisions if d.strategy != "unmapped"]
        return sum(scores) / len(scores) if scores else 0.0


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def _normalize(text) -> str:
    if pd.isna(text):
        return ""
    return _strip_accents(str(text).strip().lower())


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _col_number(col_name: str) -> int:
    """
    Extrai número ordinal de coluna sanitizada ou numerada.
    Aceita: 'Bullet Point'→1, 'Bullet Point 2'→2,
            'Bullet Point_1'→2 (sanitize _0-based → +1),
            'Bullet Point2'→2
    """
    # Underscore variant do sanitizer: _N é 0-based, então ordinal = N+1
    m = re.search(r"_(\d+)$", col_name.strip())
    if m:
        return int(m.group(1)) + 2  # _0→ord 2, _1→ord 3 ... (sem suffix→ord 1)
    # Espaço+número ou número colado
    m2 = re.search(r"\s*(\d+)\s*$", col_name.strip())
    return int(m2.group(1)) if m2 else 1


def _base_name(col_name: str) -> str:
    """
    Remove sufixo numérico (com espaço, colado ou underscore):
    'Bullet Point 3'→'Bullet Point', 'Bullet Point_2'→'Bullet Point'
    """
    # Remove _N (sanitizer)
    s = re.sub(r"_\d+$", "", col_name.strip()).strip()
    # Remove espaço+número ou número colado
    s = re.sub(r"\s*\d+\s*$", "", s).strip()
    return s


def _matches_group_prefix(col_norm: str, prefix: str) -> bool:
    """
    Verifica se col_norm pertence ao grupo com o dado prefixo.
    Aceita: exato, prefixo+espaço+N, prefixo+N, prefixo+_N
    """
    p = re.escape(prefix)
    return bool(re.match(r"^" + p + r"([_\s]\d+|\d+)?$", col_norm))


# ─── Mapper ───────────────────────────────────────────────────────────────────

class ColumnMapper:
    """
    Mapeia colunas destino (marketplace) → colunas origem (Amazon).
    """

    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = db_path
        self._learned: dict[str, dict[str, str]] = {}
        if db_path and db_path.exists():
            self._load_db()

    # ── Pública ───────────────────────────────────────────────────────────────

    def build_mapping(
        self,
        amazon_df: pd.DataFrame,
        dest_headers: dict[int, str],
        marketplace: str,
        ai_engine=None,
    ) -> MappingResult:
        result = MappingResult(marketplace=marketplace)
        amazon_cols = list(amazon_df.columns)
        amazon_norm = {_normalize(c): i for i, c in enumerate(amazon_cols)}
        fixed_map = MARKETPLACE_MAPPINGS.get(marketplace, {})

        used_source_indices: set[int] = set()

        # Pré-computa grupos multi-coluna da Amazon
        multi_groups = self._collect_multi_groups(amazon_cols)
        multi_group_cursor: dict[str, int] = {k: 0 for k in multi_groups}

        for col_idx, dest_col in dest_headers.items():
            decision = self._map_one_column(
                dest_col=dest_col,
                col_idx=col_idx,
                amazon_cols=amazon_cols,
                amazon_norm=amazon_norm,
                fixed_map=fixed_map,
                marketplace=marketplace,
                used=used_source_indices,
                ai_engine=ai_engine,
                multi_groups=multi_groups,
                multi_group_cursor=multi_group_cursor,
            )
            result.decisions.append(decision)
            if decision.source_idx is not None:
                result.index_map[col_idx] = decision.source_idx
                if not self._is_multi_group_col(amazon_cols[decision.source_idx]):
                    used_source_indices.add(decision.source_idx)
            else:
                result.unmapped_dest.append(dest_col)

        mapped_source = set(result.index_map.values())
        result.unmapped_source = [
            c for i, c in enumerate(amazon_cols) if i not in mapped_source
        ]

        logger.info(
            "Mapping %s: cobertura=%.0f%% confiança_média=%.2f",
            marketplace, result.coverage * 100, result.avg_confidence,
        )
        return result

    def normalize_source_df(
        self, df: pd.DataFrame, source_marketplace: str
    ) -> pd.DataFrame:
        """
        Renomeia colunas do DataFrame de origem para chaves semanticas.
        Recebe DataFrame com colunas nativas do marketplace e retorna
        DataFrame com colunas no vocabulario semantico (nome_produto, sku...).
        Colunas nao reconhecidas sao mantidas com nome original.
        """
        mapping = SOURCE_MAPPINGS.get(source_marketplace, {})
        if not mapping:
            logger.warning(
                "SOURCE_MAPPINGS nao tem entrada para '%s'. Colunas mantidas.",
                source_marketplace,
            )
            return df.copy()

        rename_map: dict[str, str] = {}
        used_semantic: set[str] = set()

        for col in df.columns:
            col_norm = _normalize(col)
            semantic = mapping.get(col_norm)
            if semantic and semantic not in used_semantic:
                rename_map[col] = semantic
                used_semantic.add(semantic)

        df_renamed = df.rename(columns=rename_map)
        logger.info(
            "normalize_source_df '%s': %d/%d colunas mapeadas",
            source_marketplace, len(rename_map), len(df.columns),
        )
        return df_renamed

    def learn(self, marketplace: str, dest_col: str, source_col: str) -> None:
        if marketplace not in self._learned:
            self._learned[marketplace] = {}
        self._learned[marketplace][_normalize(dest_col)] = source_col
        self._persist_db()

    # ── Multi-grupo helpers ────────────────────────────────────────────────────

    def _collect_multi_groups(
        self, amazon_cols: list[str]
    ) -> dict[str, list[tuple[str, int]]]:
        """
        Detecta e agrupa colunas multi-coluna na Amazon.
        Suporta:
          - 'Bullet Point'           → ordinal 1
          - 'Bullet Point 2'         → ordinal 2
          - 'Bullet Point_1'         → ordinal 2  (sanitize do reader, 0-based)
          - 'Bullet Point2'          → ordinal 2
        """
        groups: dict[str, list[tuple[str, int]]] = {}
        for key, prefixes in MULTI_COLUMN_GROUPS.items():
            matches: list[tuple[int, str, int]] = []
            for idx, col in enumerate(amazon_cols):
                col_norm = _normalize(col)
                for prefix in prefixes:
                    if _matches_group_prefix(col_norm, prefix):
                        ordinal = _col_number(col)
                        matches.append((ordinal, col, idx))
                        break
            if matches:
                matches.sort(key=lambda x: x[0])
                groups[key] = [(col, idx) for _, col, idx in matches]
        return groups

    def _is_multi_group_col(self, col_name: str) -> bool:
        col_norm = _normalize(col_name)
        for prefixes in MULTI_COLUMN_GROUPS.values():
            for prefix in prefixes:
                if _matches_group_prefix(col_norm, prefix):
                    return True
        return False

    def _resolve_multi_group_key(self, semantic_key: str) -> Optional[str]:
        return semantic_key if semantic_key in MULTI_COLUMN_GROUPS else None

    # ── Mapeamento individual ──────────────────────────────────────────────────

    def _map_one_column(
        self,
        dest_col: str,
        col_idx: int,
        amazon_cols: list[str],
        amazon_norm: dict[str, int],
        fixed_map: dict,
        marketplace: str,
        used: set[int],
        ai_engine,
        multi_groups: dict,
        multi_group_cursor: dict,
    ) -> FieldMappingDecision:

        dest_norm = _normalize(dest_col)

        # ── Estratégia 0: Aprendizado ─────────────────────────────────────
        learned_source = self._learned.get(marketplace, {}).get(dest_norm)
        if learned_source and _normalize(learned_source) in amazon_norm:
            idx = amazon_norm[_normalize(learned_source)]
            if idx not in used:
                return FieldMappingDecision(
                    dest_col=dest_col, source_col=learned_source,
                    source_idx=idx, strategy="learned", confidence=0.98,
                    notes="Mapeamento previamente confirmado pelo usuário.",
                )

        # ── Estratégia 1: Mapeamento fixo + sinônimos ─────────────────────
        for fixed_dest, semantic_keys in fixed_map.items():
            fixed_norm = _normalize(fixed_dest)
            dest_base_norm = _normalize(_base_name(dest_col))

            # Bate se: exato OU variante numerada/sanitizada do mesmo prefixo
            is_exact = fixed_norm == dest_norm
            is_variant = dest_base_norm == fixed_norm and dest_norm != fixed_norm

            if not (is_exact or is_variant):
                continue

            for key in semantic_keys:
                group_key = self._resolve_multi_group_key(key)
                if group_key and group_key in multi_groups:
                    cursor = multi_group_cursor.get(group_key, 0)
                    group_cols = multi_groups[group_key]
                    if cursor < len(group_cols):
                        src_col, src_idx = group_cols[cursor]
                        multi_group_cursor[group_key] = cursor + 1
                        return FieldMappingDecision(
                            dest_col=dest_col, source_col=src_col,
                            source_idx=src_idx, strategy="fixed+synonym",
                            confidence=1.0,
                            notes=(
                                f"Grupo multi-coluna '{group_key}' "
                                f"— posição {cursor + 1}/{len(group_cols)}."
                            ),
                        )
                    else:
                        return FieldMappingDecision(
                            dest_col=dest_col, source_col=None, source_idx=None,
                            strategy="unmapped", confidence=0.0,
                            notes=f"Grupo '{group_key}' esgotado ({len(group_cols)} disponível(is)).",
                        )

                synonyms = [_normalize(s) for s in AMAZON_SYNONYMS.get(key, [key])]
                # Inclui a própria chave semântica — quando a origem já foi
                # normalizada (ex: Vendor → Amazon), as colunas têm nomes
                # como "nome_produto", "sku" que batem diretamente com a chave.
                if _normalize(key) not in synonyms:
                    synonyms.append(_normalize(key))
                for syn in synonyms:
                    if syn in amazon_norm:
                        idx = amazon_norm[syn]
                        if idx not in used:
                            return FieldMappingDecision(
                                dest_col=dest_col, source_col=amazon_cols[idx],
                                source_idx=idx, strategy="fixed+synonym",
                                confidence=1.0,
                                notes=f"Mapeamento fixo via chave semântica '{key}'.",
                            )

        # ── Estratégia 2: Similaridade ────────────────────────────────────
        best_sim, best_col, best_idx = 0.0, None, None
        for norm_col, idx in amazon_norm.items():
            if idx in used:
                continue
            if self._is_multi_group_col(amazon_cols[idx]):
                continue
            sim = _similarity(dest_norm, norm_col)
            if sim > best_sim:
                best_sim, best_col, best_idx = sim, amazon_cols[idx], idx

        if best_sim >= SIMILARITY_THRESHOLD and best_idx is not None:
            return FieldMappingDecision(
                dest_col=dest_col, source_col=best_col, source_idx=best_idx,
                strategy="similarity", confidence=round(best_sim, 3),
                notes=f"Similaridade={best_sim:.2f} com '{best_col}'.",
            )

        # ── Estratégia 3: IA ──────────────────────────────────────────────
        if ai_engine is not None:
            try:
                suggestion = ai_engine.suggest_mapping(
                    dest_col=dest_col, marketplace=marketplace,
                    amazon_columns=amazon_cols,
                )
                if suggestion and suggestion.get("source_col"):
                    src = suggestion["source_col"]
                    src_norm = _normalize(src)
                    if src_norm in amazon_norm:
                        idx = amazon_norm[src_norm]
                        if idx not in used:
                            return FieldMappingDecision(
                                dest_col=dest_col, source_col=src, source_idx=idx,
                                strategy="ai", confidence=suggestion.get("confidence", 0.7),
                                notes=suggestion.get("reasoning", "Sugestão via IA."),
                            )
            except Exception as exc:
                logger.warning("Falha no AI mapping para '%s': %s", dest_col, exc)

        return FieldMappingDecision(
            dest_col=dest_col, source_col=None, source_idx=None,
            strategy="unmapped", confidence=0.0,
            notes="Nenhuma correspondência encontrada.",
        )

    def _load_db(self) -> None:
        try:
            with open(self._db_path, "r", encoding="utf-8") as f:
                self._learned = json.load(f)
        except Exception as exc:
            logger.warning("Não foi possível carregar DB de mappings: %s", exc)

    def _persist_db(self) -> None:
        """
        Persiste learned.json com escrita atômica (write-then-replace).
        Evita corrupção do arquivo em caso de escrita simultânea ou crash
        no meio do processo — o os.replace() é uma operação atômica no SO.
        """
        if self._db_path is None:
            return
        import os
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._db_path.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._learned, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self._db_path)
        except Exception as exc:
            logger.warning("Não foi possível salvar DB de mappings: %s", exc)
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass