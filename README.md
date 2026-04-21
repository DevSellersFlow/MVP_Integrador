# ⚡ SellersFlow

Motor inteligente de transformação de catálogos multi-marketplace.

## Estrutura de Pastas

```
sellersflow/
├── app.py                  # Interface Streamlit
├── pipeline.py             # Orquestrador principal
├── requirements.txt
│
├── core/
│   ├── reader.py           # Leitura e parsing da planilha Amazon
│   ├── mapper.py           # Motor de mapeamento multi-estratégia
│   ├── filler.py           # Preenchimento de templates Excel
│   └── normalizer.py       # Normalização de valores (cor, tamanho, unidades)
│
├── ai/
│   └── ai_engine.py        # Camada de IA (Anthropic Claude)
│
└── data/
    └── mappings_db/
        └── learned.json    # Banco de mapeamentos aprendidos (auto-gerado)
```

## Módulos

| Módulo | Responsabilidade |
|--------|-----------------|
| `core/reader.py` | Lê planilhas Amazon, detecta idioma (PT-BR/EN-US), retorna DataFrame limpo |
| `core/mapper.py` | Mapeia colunas destino → origem usando 4 estratégias em cascata |
| `core/filler.py` | Preenche templates Excel preservando formatação; valida obrigatórios |
| `core/normalizer.py` | Normaliza cor, tamanho, unidades, preços sem chamada de IA |
| `ai/ai_engine.py` | Sugestão de mapeamento, enriquecimento de título/descrição/bullets |
| `pipeline.py` | Orquestra todos os módulos em um pipeline único |
| `app.py` | Interface Streamlit com preview, mapeamento visual e download |

## Estratégias de Mapeamento

O `ColumnMapper` aplica as seguintes estratégias em cascata, parando no primeiro match:

1. **Aprendido** (confiança 0.98) — decisões confirmadas pelo usuário em execuções anteriores
2. **Fixo + Sinônimo** (confiança 1.0) — tabela `MARKETPLACE_MAPPINGS` + `AMAZON_SYNONYMS`
3. **Similaridade** (confiança = score) — `SequenceMatcher`, threshold 0.72
4. **IA** (confiança variável) — Claude via API Anthropic, apenas como fallback
5. **Não mapeado** — campo fica vazio no output

## Instalação

```bash
pip install -r requirements.txt
```

## Uso

```bash
export ANTHROPIC_API_KEY="sua-chave"
python -m streamlit run app.py
```

## Uso Programático

```python
from pipeline import SellersFlowPipeline

pipeline = SellersFlowPipeline()

with open("amazon.xlsx", "rb") as af, open("template_shopee.xlsx", "rb") as tf:
    result = pipeline.run(
        amazon_file=af,
        template_file=tf,
        marketplace="Shopee",
        use_ai=True,        # IA como fallback de mapeamento
        enrich_ai=False,    # Enriquecimento de conteúdo
    )

if result.success:
    print(f"Arquivo: {result.output_path}")
    print(f"Cobertura: {result.mapping_result.coverage:.0%}")
    print(f"Confiança média: {result.mapping_result.avg_confidence:.0%}")

# Ver decisões de mapeamento
for decision in result.mapping_result.decisions:
    print(f"{decision.dest_col} → {decision.source_col} [{decision.strategy}] {decision.confidence:.0%}")

# Ensinar um mapeamento
pipeline.learn_mapping("Shopee", "sku principal", "Seller SKU")
```

## Adicionando Novo Marketplace

1. Adicione em `core/mapper.py → MARKETPLACE_MAPPINGS`
2. Adicione em `core/filler.py → MARKETPLACE_CONFIG`
3. Adicione em `core/mapper.py → REQUIRED_FIELDS`

## Roadmap SaaS

- [ ] API REST (FastAPI) com autenticação JWT
- [ ] Banco de dados (PostgreSQL) para mappings por conta
- [ ] Histórico de execuções com diff de mapeamentos
- [ ] Versionamento de templates por marketplace
- [ ] Fila de processamento assíncrono (Celery + Redis) para batch
- [ ] Dashboard de qualidade de catálogo por seller
- [ ] Webhook de notificação pós-processamento

## Mercado Livre

O Mercado Livre tem uma particularidade: o nome da aba de dados **muda conforme a categoria do produto**. Por isso, a resolução da aba usa posição em vez de nome fixo:

- A aba correta é **sempre a segunda aba após "Ajuda"** no workbook.
- Se a aba "Ajuda" não for encontrada, o sistema usa a segunda aba disponível como fallback.
- O cabeçalho fica na **linha 8** e os dados começam na **linha 9**.

### Mapeamento de colunas

| Coluna do template ML | Campo Amazon (chave semântica) |
|---|---|
| Título (coluna longa de instrução) | `nome_produto` |
| Código universal de produto | `id_produto` |
| SKU | `sku` |
| Estoque | `quantidade` |

Para adicionar mais colunas do ML ao mapeamento, edite `MARKETPLACE_MAPPINGS["MercadoLivre"]` em `core/mapper.py`.
