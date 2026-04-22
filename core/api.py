"""
api.py
======
API REST do SellersFlow (FastAPI).

Endpoints:
  POST /process          — pipeline completo (upload + processamento)
  GET  /download/{job}   — download do arquivo gerado
  GET  /status/{job}     — status e resultado de um job
  GET  /marketplaces     — lista os marketplaces disponíveis (destino)
  GET  /source-marketplaces — lista os marketplaces disponíveis como origem
  POST /learn            — salva mapeamento aprendido
  GET  /mappings         — lista mapeamentos aprendidos

Design para múltiplos usuários simultâneos:
  - Cada requisição é isolada: arquivos de entrada/saída em pastas por job_id (UUID).
  - SellersFlowPipeline instanciada por requisição (stateless).
  - Jobs ficam em memória (dict protegido por asyncio.Lock) + arquivo no disco.
  - Para produção com múltiplos workers, substituir o dict por Redis.
  - Limpeza automática de jobs antigos a cada hora.

Como rodar:
  pip install fastapi uvicorn python-multipart
  uvicorn api:app --host 0.0.0.0 --port 8000 --workers 4
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import shutil
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

import sys
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import SellersFlowPipeline, DEFAULT_DB_PATH
from core.mapper import MARKETPLACE_MAPPINGS
from core.source_reader import SOURCE_CONFIG

# ─── Configuração ─────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

JOBS_DIR = Path(__file__).parent / "data" / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

JOB_TTL_HOURS = 2

# Marketplaces válidos como ORIGEM (inclui Amazon que usa reader.py dedicado)
SOURCE_MARKETPLACES: list[str] = ["Amazon"] + [
    mp for mp in SOURCE_CONFIG.keys() if mp != "Amazon"
]

# ─── Estado em memória (jobs) ─────────────────────────────────────────────────

_jobs: dict[str, dict] = {}
_jobs_lock = asyncio.Lock()


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SellersFlow API",
    description="Motor inteligente de transformação de catálogos multi-marketplace.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Schemas ──────────────────────────────────────────────────────────────────

class JobStatus(BaseModel):
    job_id: str
    status: str                        # "pending" | "running" | "done" | "error"
    marketplace: str
    source_marketplace: str
    created_at: str
    elapsed_seconds: Optional[float] = None
    rows_written: Optional[int] = None
    coverage: Optional[float] = None
    avg_confidence: Optional[float] = None
    errors: list[str] = []
    warnings: list[str] = []
    unmapped_fields: list[str] = []
    validation_issues: list[dict] = []
    download_url: Optional[str] = None


class LearnRequest(BaseModel):
    marketplace: str
    dest_col: str
    source_col: str


class LearnResponse(BaseModel):
    saved: bool
    message: str


class MarketplacesResponse(BaseModel):
    marketplaces: list[str]


class MappingsResponse(BaseModel):
    mappings: dict[str, dict[str, str]]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _job_output_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id


def _build_job_status(job_id: str, job: dict) -> JobStatus:
    result = job.get("result")
    download_url = None

    if result and result.output_path:
        download_url = f"/download/{job_id}"

    validation_issues = []
    if result and result.fill_result and result.fill_result.validation_issues:
        validation_issues = [
            {
                "column": i.column,
                "severity": i.severity,
                "message": i.message,
            }
            for i in result.fill_result.validation_issues
        ]

    return JobStatus(
        job_id=job_id,
        status=job["status"],
        marketplace=job["marketplace"],
        source_marketplace=job.get("source_marketplace", "Amazon"),
        created_at=job["created_at"],
        elapsed_seconds=result.elapsed_seconds if result else None,
        rows_written=(
            result.fill_result.rows_written
            if result and result.fill_result else None
        ),
        coverage=(
            result.mapping_result.coverage
            if result and result.mapping_result else None
        ),
        avg_confidence=(
            result.mapping_result.avg_confidence
            if result and result.mapping_result else None
        ),
        errors=result.errors if result else job.get("errors", []),
        warnings=result.warnings if result else [],
        unmapped_fields=(
            result.mapping_result.unmapped_dest
            if result and result.mapping_result else []
        ),
        validation_issues=validation_issues,
        download_url=download_url,
    )


async def _run_pipeline(
    job_id: str,
    amazon_bytes: bytes,
    template_bytes: bytes,
    marketplace: str,
    source_marketplace: str,
    use_ai: bool,
    enrich_ai: bool,
) -> None:
    """Executa o pipeline em background e atualiza o estado do job."""
    async with _jobs_lock:
        _jobs[job_id]["status"] = "running"

    try:
        output_dir = str(_job_output_dir(job_id))
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        pipeline = SellersFlowPipeline(output_dir=output_dir)

        result = pipeline.run(
            amazon_file=io.BytesIO(amazon_bytes),
            template_file=io.BytesIO(template_bytes),
            marketplace=marketplace,
            source_marketplace=source_marketplace,
            use_ai=use_ai,
            enrich_ai=enrich_ai,
        )

        async with _jobs_lock:
            _jobs[job_id]["status"] = "done" if result.success else "error"
            _jobs[job_id]["result"] = result

    except Exception as exc:
        logger.exception("Erro no pipeline do job %s", job_id)
        async with _jobs_lock:
            _jobs[job_id]["status"] = "error"
            _jobs[job_id]["errors"] = [str(exc)]


def _cleanup_old_jobs() -> None:
    """Remove jobs e arquivos com mais de JOB_TTL_HOURS horas."""
    cutoff = datetime.utcnow() - timedelta(hours=JOB_TTL_HOURS)
    to_remove = []
    for job_id, job in _jobs.items():
        try:
            created = datetime.fromisoformat(job["created_at"])
            if created < cutoff:
                to_remove.append(job_id)
        except Exception:
            pass

    for job_id in to_remove:
        _jobs.pop(job_id, None)
        job_dir = _job_output_dir(job_id)
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)

    if to_remove:
        logger.info("Limpeza: %d jobs removidos.", len(to_remove))


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/marketplaces", response_model=MarketplacesResponse, tags=["Config"])
async def list_marketplaces():
    """Retorna a lista de marketplaces configurados como DESTINO."""
    return MarketplacesResponse(marketplaces=list(MARKETPLACE_MAPPINGS.keys()))


@app.get("/source-marketplaces", response_model=MarketplacesResponse, tags=["Config"])
async def list_source_marketplaces():
    """Retorna a lista de marketplaces suportados como ORIGEM."""
    return MarketplacesResponse(marketplaces=SOURCE_MARKETPLACES)


@app.get("/mappings", response_model=MappingsResponse, tags=["Aprendizado"])
async def list_mappings():
    """Retorna todos os mapeamentos aprendidos persistidos."""
    try:
        from core.mapper import ColumnMapper
        cm = ColumnMapper(db_path=DEFAULT_DB_PATH)
        return MappingsResponse(mappings=cm._learned)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/learn", response_model=LearnResponse, tags=["Aprendizado"])
async def learn_mapping(req: LearnRequest):
    """Persiste um mapeamento confirmado pelo usuário."""
    try:
        pipeline = SellersFlowPipeline()
        pipeline.learn_mapping(req.marketplace, req.dest_col, req.source_col)
        return LearnResponse(
            saved=True,
            message=f"Mapeamento salvo: '{req.dest_col}' → '{req.source_col}' ({req.marketplace})",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/process", response_model=JobStatus, status_code=202, tags=["Pipeline"])
async def process(
    background_tasks: BackgroundTasks,
    source_file: UploadFile = File(..., description="Planilha de ORIGEM (.xlsx/.xlsm)"),
    template_file: UploadFile = File(..., description="Template do marketplace DESTINO (.xlsx/.xlsm)"),
    marketplace: str = Form(..., description="Marketplace DESTINO (ex: Shopee, Amazon, Mercado Livre)"),
    source_marketplace: str = Form("Amazon", description="Marketplace ORIGEM (ex: Amazon, Mercado Livre, Shopee)"),
    use_ai: bool = Form(False, description="Usar IA como fallback de mapeamento"),
    enrich_ai: bool = Form(False, description="Enriquecer título/descrição via IA"),
):
    """
    Inicia o processamento em background.
    Retorna imediatamente com job_id e status 202 Accepted.
    Consulte GET /status/{job_id} para acompanhar.

    Suporta qualquer combinação de marketplace origem/destino:
      - Amazon → Shopee
      - Mercado Livre → Amazon
      - Shopee → Temu
      - etc.
    """
    # ── Validações básicas ────────────────────────────────────────────────
    allowed_ext = {".xlsx", ".xlsm", ".xls"}
    for upload in (source_file, template_file):
        ext = Path(upload.filename or "").suffix.lower()
        if ext not in allowed_ext:
            raise HTTPException(
                status_code=400,
                detail=f"Arquivo '{upload.filename}' inválido. Use .xlsx, .xlsm ou .xls.",
            )

    if marketplace not in MARKETPLACE_MAPPINGS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Marketplace destino '{marketplace}' não suportado. "
                f"Disponíveis: {list(MARKETPLACE_MAPPINGS.keys())}"
            ),
        )

    if source_marketplace not in SOURCE_MARKETPLACES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Marketplace origem '{source_marketplace}' não suportado. "
                f"Disponíveis: {SOURCE_MARKETPLACES}"
            ),
        )

    if marketplace == source_marketplace:
        raise HTTPException(
            status_code=400,
            detail="Marketplace de origem e destino não podem ser iguais.",
        )

    MAX_SIZE = 20 * 1024 * 1024  # 20 MB
    amazon_bytes = await source_file.read()
    template_bytes = await template_file.read()

    if len(amazon_bytes) > MAX_SIZE:
        raise HTTPException(status_code=413, detail="Planilha de origem excede 20 MB.")
    if len(template_bytes) > MAX_SIZE:
        raise HTTPException(status_code=413, detail="Template excede 20 MB.")

    # ── Criar job ─────────────────────────────────────────────────────────
    _cleanup_old_jobs()

    job_id = str(uuid.uuid4())
    async with _jobs_lock:
        _jobs[job_id] = {
            "status": "pending",
            "marketplace": marketplace,
            "source_marketplace": source_marketplace,
            "created_at": datetime.utcnow().isoformat(),
            "result": None,
            "errors": [],
        }

    background_tasks.add_task(
        _run_pipeline,
        job_id=job_id,
        amazon_bytes=amazon_bytes,
        template_bytes=template_bytes,
        marketplace=marketplace,
        source_marketplace=source_marketplace,
        use_ai=use_ai,
        enrich_ai=enrich_ai,
    )

    logger.info(
        "Job %s criado: %s → %s",
        job_id, source_marketplace, marketplace,
    )
    return _build_job_status(job_id, _jobs[job_id])


@app.get("/status/{job_id}", response_model=JobStatus, tags=["Pipeline"])
async def get_status(job_id: str):
    """Retorna o status atual de um job de processamento."""
    async with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' não encontrado.")
    return _build_job_status(job_id, job)


@app.get("/download/{job_id}", tags=["Pipeline"])
async def download_result(job_id: str):
    """
    Retorna o arquivo Excel gerado para download.
    Disponível apenas quando status == 'done'.
    """
    async with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' não encontrado.")
    if job["status"] != "done":
        raise HTTPException(
            status_code=409,
            detail=f"Job ainda não concluído (status: {job['status']}).",
        )

    result = job.get("result")
    if not result or not result.output_path:
        raise HTTPException(status_code=404, detail="Arquivo de saída não encontrado.")

    output_path = Path(result.output_path)
    if not output_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo expirou ou foi removido.")

    return FileResponse(
        path=str(output_path),
        filename=output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ─── Health check ─────────────────────────────────────────────────────────────

@app.get("/health", tags=["Sistema"])
async def health():
    """Verifica se a API está no ar."""
    return {
        "status": "ok",
        "version": "2.0.0",
        "jobs_active": len(_jobs),
        "source_marketplaces": SOURCE_MARKETPLACES,
        "dest_marketplaces": list(MARKETPLACE_MAPPINGS.keys()),
        "timestamp": datetime.utcnow().isoformat(),
    }