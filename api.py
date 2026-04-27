"""
api.py — FastAPI REST endpoint
Endpoint: POST /prospectar
Deploy: Vercel (vercel.json) ou Railway/Render como serviço separado
"""
import re, os, asyncio, logging
from typing import Optional, List
from datetime import datetime

try:
    from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, Field
except ImportError:
    raise ImportError("pip install fastapi uvicorn")

# Importa módulos do PROSPEC-O
import sys
sys.path.insert(0, os.path.dirname(__file__))

from enricher_async import enrich_batch_async, enrich_lead_async

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
#  FastAPI App
# ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="PROSPEC-O API",
    description="Motor de prospecção comercial Scitec — Laboratório + OCP",
    version="5.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # Restrinja em produção
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────
#  Schemas Pydantic
# ─────────────────────────────────────────────────────────────────────
class LeadContato(BaseModel):
    cnpj:          Optional[str] = None
    nome:          Optional[str] = None
    email:         Optional[str] = None
    telefone:      Optional[str] = None
    whatsapp:      Optional[str] = None
    decisor:       Optional[str] = None
    cargo_decisor: Optional[str] = None
    site:          Optional[str] = None
    cidade:        Optional[str] = None
    estado:        Optional[str] = None
    setor:         Optional[str] = None
    ocp:           Optional[str] = None
    validade:      Optional[str] = None
    urgencia:      Optional[str] = None
    crm_status:    Optional[str] = None
    lookalike:     Optional[int] = None
    icp_acao:      Optional[str] = None
    icp_flags:     Optional[str] = None
    fonte_enrich:  Optional[str] = None
    site_visitado: Optional[str] = None


class ProspectarRequest(BaseModel):
    portaria:         str = Field(..., description="145, 384, ISO10993, MRI, BIOBURDEN, ENDO_EST")
    estado:           Optional[str] = Field(None, description="UF ex: SP")
    cidade:           Optional[str] = None
    max_leads:        int = Field(50, ge=1, le=500)
    enriquecer:       bool = Field(True, description="Buscar emails/tel/WhatsApp via async")
    concorrencia:     int = Field(8, ge=1, le=20, description="Requisições paralelas")
    filtro_icp:       bool = Field(True, description="Aplicar filtro ICP score")
    icp_score_minimo: int = Field(0, ge=0, le=100)


class ProspectarResponse(BaseModel):
    portaria:      str
    total:         int
    com_email:     int
    com_whatsapp:  int
    com_decisor:   int
    gerado_em:     str
    leads:         List[LeadContato]


# ─────────────────────────────────────────────────────────────────────
#  Dependência de autenticação simples (API Key via header)
# ─────────────────────────────────────────────────────────────────────
API_KEY = os.getenv("PROSPEC_API_KEY", "")

def verify_api_key(x_api_key: str = None):
    """Autenticação por header X-API-Key (opcional se API_KEY não definida)."""
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API Key inválida")


# ─────────────────────────────────────────────────────────────────────
#  Endpoints
# ─────────────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"])
async def health():
    return {"status": "ok", "version": "5.0.0", "service": "PROSPEC-O API"}


@app.post("/prospectar", response_model=ProspectarResponse, tags=["Prospecção"])
async def prospectar(
    body: ProspectarRequest,
    background_tasks: BackgroundTasks,
):
    """
    Coleta leads por portaria/escopo, enriquece com multi-fontes async
    e retorna JSON estruturado com contatos completos.

    **Fluxo:**
    1. Busca INMETRO (portarias 145/384) ou Casa dos Dados por CNAE (Lab)
    2. Enriquecimento paralelo: BrasilAPI + Serper + Hunter.io + Apollo + scraping
    3. Score ICP automático
    4. Retorna JSON ordenado por prioridade
    """
    try:
        # Import inline para evitar dependência circular com Streamlit
        from app import (
            InmetroScraper, EndotoxinaBot, CRMAnalyzer,
            CNAE_POR_ESCOPO, _is_empresa
        )
    except ImportError as e:
        raise HTTPException(500, f"Módulo app.py não encontrado: {e}")

    leads_raw = []

    # ── Coleta por módulo ─────────────────────────────────────────────
    if body.portaria in ("145", "384"):
        scraper = InmetroScraper()
        leads_raw = scraper.fetch_all_empresas(
            portaria=body.portaria,
            estado=body.estado,
            cidade=body.cidade,
        )
    else:
        # Módulo Laboratório — busca por CNAE
        bot = EndotoxinaBot()
        cnaes = CNAE_POR_ESCOPO.get(body.portaria, ["3250701"])
        ufs   = [body.estado] if body.estado else ["SP", "MG", "SC", "RS", "PR"]
        seen  = set()
        for cnae in cnaes:
            for uf in ufs[:4]:
                try:
                    batch = bot.buscar_cnae(cnae, uf, paginas=2)
                    for l in batch:
                        k = re.sub(r"\D", "", l.get("cnpj", "")) or l.get("nome", "")
                        if k and k not in seen:
                            seen.add(k)
                            leads_raw.append(l)
                except Exception as e:
                    logger.warning(f"CNAE {cnae}/{uf}: {e}")

    if not leads_raw:
        return ProspectarResponse(
            portaria=body.portaria, total=0,
            com_email=0, com_whatsapp=0, com_decisor=0,
            gerado_em=datetime.now().isoformat(),
            leads=[]
        )

    # ── Limita e enriquece ────────────────────────────────────────────
    leads_limited = leads_raw[:body.max_leads]

    if body.enriquecer:
        leads_limited = await enrich_batch_async(leads_limited, body.concorrencia)

    # ── ICP Score ────────────────────────────────────────────────────
    for lead in leads_limited:
        score = CRMAnalyzer.icp_score_lead(lead, df=None)
        lead["lookalike"]  = score.get("total", 0)
        lead["crm_status"] = score.get("crm_status", "novo")
        lead["icp_acao"]   = score.get("acao", "prospectar")
        lead["icp_flags"]  = " | ".join(score.get("flags", []))

    # ── Filtra por score mínimo ────────────────────────────────────────
    if body.filtro_icp and body.icp_score_minimo > 0:
        leads_limited = [l for l in leads_limited
                         if (l.get("lookalike") or 0) >= body.icp_score_minimo]

    # ── Ordena por prioridade ─────────────────────────────────────────
    _PRIOR = {"upsell": 0, "reativar": 1, "nurturing": 2, "prospectar": 3, "descartar": 4}
    leads_limited.sort(key=lambda x: (
        _PRIOR.get(x.get("icp_acao", "prospectar"), 3),
        -(x.get("lookalike") or 0)
    ))

    # ── Serializa ─────────────────────────────────────────────────────
    leads_out = [LeadContato(**{k: v for k, v in l.items()
                                if k in LeadContato.model_fields})
                 for l in leads_limited]

    return ProspectarResponse(
        portaria=body.portaria,
        total=len(leads_out),
        com_email=sum(1 for l in leads_out if l.email),
        com_whatsapp=sum(1 for l in leads_out if l.whatsapp),
        com_decisor=sum(1 for l in leads_out if l.decisor),
        gerado_em=datetime.now().isoformat(),
        leads=leads_out,
    )


@app.get("/lead/enriquecer", tags=["Enriquecimento"])
async def enriquecer_lead(
    cnpj: str = Query(None, description="CNPJ da empresa (somente números)"),
    nome: str = Query(None, description="Nome/razão social"),
):
    """Enriquece um único lead com todas as fontes disponíveis."""
    if not cnpj and not nome:
        raise HTTPException(400, "Informe cnpj ou nome")
    lead = {"cnpj": cnpj or "", "nome": nome or ""}
    result = await enrich_lead_async(lead)
    return result


@app.get("/saude", tags=["Health"])
async def saude():
    """Verifica disponibilidade das APIs configuradas."""
    return {
        "serper_configurado":  bool(os.getenv("SERPER_API_KEY")),
        "hunter_configurado":  bool(os.getenv("HUNTER_API_KEY")),
        "apollo_configurado":  bool(os.getenv("APOLLO_API_KEY")),
        "auth_ativa":          bool(os.getenv("PROSPEC_API_KEY")),
    }
