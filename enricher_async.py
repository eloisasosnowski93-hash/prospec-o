"""
enricher_async.py — Motor de enriquecimento assíncrono (httpx)
Executa buscas em paralelo: BrasilAPI, Serper, Hunter.io, scraping de site
"""
import re, os, asyncio, logging
from typing import Optional
try:
    import httpx
except ImportError:
    httpx = None

logger = logging.getLogger(__name__)

# ── Variáveis de ambiente (nunca hardcode keys) ───────────────────────
SERPER_KEY  = os.getenv("SERPER_API_KEY", "")
HUNTER_KEY  = os.getenv("HUNTER_API_KEY", "")
APOLLO_KEY  = os.getenv("APOLLO_API_KEY", "")

RE_EMAIL     = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
RE_PHONE_BR  = re.compile(r"\(?(?:0?[1-9]{2})\)?\s?(?:9\s?)?[1-9]\d{3}[\s\-]?\d{4}")
RE_WHATSAPP  = re.compile(
    r"(?:https?://)?(?:api\.whatsapp\.com/send\?phone=|wa\.me/|whatsapp://send\?phone=)"
    r"(\d{10,15})", re.IGNORECASE
)
RE_EMAIL_CORP = re.compile(
    r"[a-zA-Z0-9][a-zA-Z0-9._%+\-]{1,40}@"
    r"(?!gmail|yahoo|hotmail|outlook|bol|uol|terra|ig\.)"
    r"[a-zA-Z0-9.\-]{2,40}\.[a-zA-Z]{2,6}"
)

TIMEOUT  = httpx.Timeout(12.0, connect=5.0) if httpx else None
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ─────────────────────────────────────────────────────────────────────
#  1. BrasilAPI — dados societários e CNPJ
# ─────────────────────────────────────────────────────────────────────
async def fetch_brasilapi(client: "httpx.AsyncClient", cnpj: str) -> dict:
    cnpj_clean = re.sub(r"\D", "", cnpj)
    if len(cnpj_clean) != 14:
        return {}
    try:
        r = await client.get(
            f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_clean}",
            timeout=TIMEOUT
        )
        if r.status_code == 200:
            d = r.json()
            tel = re.sub(r"\D", "", d.get("ddd_telefone_1") or "")
            return {
                "nome":     d.get("razao_social", ""),
                "cidade":   d.get("municipio", ""),
                "estado":   d.get("uf", ""),
                "situacao": d.get("descricao_situacao_cadastral", ""),
                "setor":    d.get("cnae_fiscal_descricao", "")[:60],
                "email":    (d.get("email") or "").lower() or None,
                "telefone": (f"({tel[:2]}) {tel[2:7]}-{tel[7:]}" if len(tel) == 11
                             else f"({tel[:2]}) {tel[2:]}" if len(tel) == 10 else None),
                "decisor":  _extract_decisor_qsa(d.get("qsa", [])),
                "fonte_enrich": "brasilapi",
            }
    except Exception as e:
        logger.warning(f"BrasilAPI {cnpj}: {e}")
    return {}


def _extract_decisor_qsa(qsa: list) -> Optional[str]:
    priority_codes = {"5", "10", "49", "22", "16", "21"}
    for s in qsa:
        if str(s.get("codigo_qualificacao_socio", "")) in priority_codes:
            return s.get("nome_socio") or s.get("nome")
    if qsa:
        return qsa[0].get("nome_socio") or qsa[0].get("nome")
    return None


# ─────────────────────────────────────────────────────────────────────
#  2. Serper (Google Search) — encontrar site oficial da empresa
# ─────────────────────────────────────────────────────────────────────
async def fetch_serper_site(client: "httpx.AsyncClient", company_name: str) -> Optional[str]:
    """Usa Serper.dev (Google Search API) para encontrar o site oficial."""
    if not SERPER_KEY:
        return None
    try:
        r = await client.post(
            "https://google.serper.dev/search",
            json={"q": f"{company_name} site oficial", "gl": "br", "hl": "pt", "num": 3},
            headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
            timeout=TIMEOUT,
        )
        if r.status_code == 200:
            results = r.json().get("organic", [])
            for res in results:
                url = res.get("link", "")
                # Exclui portais genéricos
                _SKIP = ("cnpj.biz", "jusbrasil", "econodata", "linkedin.com",
                         "facebook", "instagram", "receita.fazenda")
                if not any(s in url for s in _SKIP):
                    return url
    except Exception as e:
        logger.warning(f"Serper '{company_name}': {e}")
    return None


# ─────────────────────────────────────────────────────────────────────
#  3. Hunter.io — validar e encontrar emails corporativos
# ─────────────────────────────────────────────────────────────────────
async def fetch_hunter_email(client: "httpx.AsyncClient", domain: str, company: str) -> dict:
    """
    Hunter.io Domain Search — retorna emails corporativos encontrados.
    Plano gratuito: 25 buscas/mês. Profissional: 500/mês.
    Cadastro: https://hunter.io/users/sign_up
    """
    if not HUNTER_KEY or not domain:
        return {}
    try:
        r = await client.get(
            "https://api.hunter.io/v2/domain-search",
            params={"domain": domain, "company": company, "api_key": HUNTER_KEY, "limit": 5},
            timeout=TIMEOUT,
        )
        if r.status_code == 200:
            data = r.json().get("data", {})
            emails = data.get("emails", [])
            if emails:
                # Prioriza email de decisor (cargo relevante)
                priority_depts = {"executive", "management", "quality", "engineering", "regulatory"}
                for e in emails:
                    dept = (e.get("department") or "").lower()
                    if any(d in dept for d in priority_depts):
                        return {"email": e["value"], "decisor": f"{e.get('first_name','')} {e.get('last_name','')}".strip(), "fonte_email": "hunter"}
                # Fallback: primeiro email encontrado
                first = emails[0]
                return {"email": first["value"], "fonte_email": "hunter"}
    except Exception as e:
        logger.warning(f"Hunter.io '{domain}': {e}")
    return {}


# ─────────────────────────────────────────────────────────────────────
#  4. Apollo.io — busca de contatos B2B
# ─────────────────────────────────────────────────────────────────────
async def fetch_apollo_contact(client: "httpx.AsyncClient", company_name: str) -> dict:
    """
    Apollo.io People Search — encontra decisores por cargo e empresa.
    Plano gratuito: 10k créditos/mês.
    Cadastro: https://app.apollo.io/#/sign-up
    Endpoints: POST https://api.apollo.io/v1/mixed_people/search
    """
    if not APOLLO_KEY:
        return {}
    try:
        r = await client.post(
            "https://api.apollo.io/v1/mixed_people/search",
            json={
                "api_key": APOLLO_KEY,
                "q_organization_name": company_name,
                "person_titles": ["Gerente de Qualidade", "Regulatory Affairs", "P&D",
                                  "Quality Manager", "Engenharia", "Compras"],
                "page": 1, "per_page": 3,
            },
            timeout=TIMEOUT,
        )
        if r.status_code == 200:
            people = r.json().get("people", [])
            if people:
                p = people[0]
                return {
                    "decisor": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                    "cargo_decisor": p.get("title", ""),
                    "email": p.get("email") or None,
                    "linkedin": p.get("linkedin_url") or None,
                    "fonte_decisor": "apollo",
                }
    except Exception as e:
        logger.warning(f"Apollo '{company_name}': {e}")
    return {}


# ─────────────────────────────────────────────────────────────────────
#  5. Scraping do site da empresa — emails, fones, WhatsApp
# ─────────────────────────────────────────────────────────────────────
async def scrape_company_site(client: "httpx.AsyncClient", url: str) -> dict:
    """
    Visita o site da empresa e extrai:
    - E-mails corporativos (evita webmail)
    - Telefones brasileiros
    - Link de WhatsApp (wa.me ou api.whatsapp.com)
    - Página de contato se disponível
    """
    if not url:
        return {}
    result = {}
    try:
        # Normaliza URL
        if not url.startswith("http"):
            url = f"https://{url}"

        # Tenta página principal
        r = await client.get(url, timeout=TIMEOUT, follow_redirects=True)
        if r.status_code != 200:
            return {}

        text = r.text
        result.update(_parse_contacts(text, url))

        # Se não encontrou email/WhatsApp, tenta /contato ou /contact
        if not result.get("email") and not result.get("whatsapp"):
            for slug in ("/contato", "/contatos", "/contact", "/fale-conosco", "/sobre"):
                try:
                    base = str(r.url).rstrip("/")
                    r2 = await client.get(base + slug, timeout=TIMEOUT, follow_redirects=True)
                    if r2.status_code == 200:
                        extra = _parse_contacts(r2.text, base + slug)
                        for k, v in extra.items():
                            result.setdefault(k, v)
                        if result.get("email") and result.get("whatsapp"):
                            break
                except Exception:
                    pass

    except Exception as e:
        logger.warning(f"Scrape site '{url}': {e}")

    return result


def _parse_contacts(html: str, source_url: str) -> dict:
    """Extrai contatos de um bloco HTML."""
    result = {}

    # WhatsApp — prioridade máxima
    wa_matches = RE_WHATSAPP.findall(html)
    if wa_matches:
        num = re.sub(r"\D", "", wa_matches[0])
        result["whatsapp"] = f"+{num}" if not num.startswith("+") else num

    # Email corporativo primeiro
    corp = [e.lower() for e in RE_EMAIL_CORP.findall(html)
            if not e.lower().endswith((".png", ".jpg", ".gif", "@sentry.io"))
            and len(e) < 80]
    all_emails = [e.lower() for e in RE_EMAIL.findall(html)
                  if not e.lower().endswith((".png", ".jpg", ".gif", "@sentry.io"))
                  and len(e) < 80]
    best_email = corp[0] if corp else (all_emails[0] if all_emails else None)
    if best_email:
        result["email"] = best_email

    # Telefone/fone
    phones = RE_PHONE_BR.findall(html)
    if phones:
        result["telefone"] = phones[0].strip()

    result["site_visitado"] = source_url
    return result


# ─────────────────────────────────────────────────────────────────────
#  6. Pipeline assíncrono principal
# ─────────────────────────────────────────────────────────────────────
async def enrich_lead_async(lead: dict) -> dict:
    """
    Pipeline completo de enriquecimento assíncrono para 1 lead.
    Executa em paralelo: BrasilAPI + Serper + Apollo.
    """
    if not httpx:
        return lead  # fallback silencioso se httpx não instalado

    cnpj = re.sub(r"\D", "", lead.get("cnpj", ""))
    nome = lead.get("nome", "")

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        # Executa fontes independentes em paralelo
        tasks = [
            fetch_brasilapi(client, cnpj) if cnpj else asyncio.sleep(0, result={}),
            fetch_serper_site(client, nome),
            fetch_apollo_contact(client, nome),
        ]
        brasil_data, site_url, apollo_data = await asyncio.gather(*tasks, return_exceptions=True)

        # Trata exceções (cada fonte falha independentemente)
        brasil_data  = brasil_data  if isinstance(brasil_data,  dict) else {}
        site_url     = site_url     if isinstance(site_url,     str)  else None
        apollo_data  = apollo_data  if isinstance(apollo_data,  dict) else {}

        # Dados BrasilAPI
        for k, v in brasil_data.items():
            if v and not lead.get(k):
                lead[k] = v

        # Apollo
        for k, v in apollo_data.items():
            if v and not lead.get(k):
                lead[k] = v

        # Site encontrado via Serper
        if site_url and not lead.get("site"):
            lead["site"] = site_url

        # Scraping do site + Hunter.io em paralelo
        domain = _extract_domain(site_url or "")
        tasks2 = [
            scrape_company_site(client, site_url) if site_url else asyncio.sleep(0, result={}),
            fetch_hunter_email(client, domain, nome) if domain else asyncio.sleep(0, result={}),
        ]
        site_data, hunter_data = await asyncio.gather(*tasks2, return_exceptions=True)
        site_data   = site_data   if isinstance(site_data,   dict) else {}
        hunter_data = hunter_data if isinstance(hunter_data, dict) else {}

        # Mescla dados do site
        for k, v in {**site_data, **hunter_data}.items():
            if v and not lead.get(k):
                lead[k] = v

    lead["_enrich_async"] = True
    return lead


async def enrich_batch_async(leads: list, max_concurrent: int = 8) -> list:
    """
    Enriquece N leads com concorrência controlada (semáforo).
    Evita rate-limit e sobrecarga de rede.
    """
    if not httpx:
        return leads

    semaphore = asyncio.Semaphore(max_concurrent)

    async def _guarded(lead):
        async with semaphore:
            return await enrich_lead_async(lead)

    return await asyncio.gather(*[_guarded(l) for l in leads], return_exceptions=False)


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"https?://(?:www\.)?([a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,})", url)
    return m.group(1) if m else ""


# ─────────────────────────────────────────────────────────────────────
#  Adapter síncrono (compatível com Streamlit)
# ─────────────────────────────────────────────────────────────────────
def enrich_lead_sync(lead: dict) -> dict:
    """Wrapper síncrono para uso dentro do Streamlit (thread seguro)."""
    try:
        return asyncio.run(enrich_lead_async(lead))
    except RuntimeError:
        # Já há um event loop rodando (Jupyter / alguns ambientes)
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(asyncio.run, enrich_lead_async(lead))
            return future.result(timeout=30)


def enrich_batch_sync(leads: list, max_concurrent: int = 8) -> list:
    """Wrapper síncrono para batch — usa asyncio.run() ou thread pool."""
    try:
        return asyncio.run(enrich_batch_async(leads, max_concurrent))
    except RuntimeError:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(asyncio.run, enrich_batch_async(leads, max_concurrent))
            return future.result(timeout=120)
