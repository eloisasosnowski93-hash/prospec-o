# PROSPEC-O v5 — Guia de Deploy e APIs

## Estrutura de arquivos

```
prospec-o/
├── app.py              ← Streamlit UI completa (3.150 linhas)
├── api.py              ← FastAPI REST endpoint
├── enricher_async.py   ← Motor assíncrono (httpx + BrasilAPI + Serper + Hunter + Apollo)
├── requirements.txt    ← Dependências completas
├── vercel.json         ← Configuração Vercel (API REST)
├── .env.example        ← Variáveis de ambiente (copie para .env)
└── README_deploy.md    ← Este arquivo
```

---

## 1. Streamlit Cloud (UI — GRATUITO)

```bash
# 1. Push para GitHub
git init && git add . && git commit -m "PROSPEC-O v5"
git remote add origin https://github.com/SEU_USUARIO/prospec-o.git
git push -u origin main

# 2. Acesse share.streamlit.io
# New app → selecionar repo → Main file: app.py → Deploy
```

**Variáveis no Streamlit Cloud:**
Settings → Secrets → adicionar:
```toml
SERPER_API_KEY = "sua_chave"
HUNTER_API_KEY = "sua_chave"
APOLLO_API_KEY = "sua_chave"
```

---

## 2. API REST — Vercel (FastAPI)

```bash
# Instalar Vercel CLI
npm i -g vercel

# Configurar secrets
vercel secrets add serper_api_key "sua_chave"
vercel secrets add hunter_api_key "sua_chave"
vercel secrets add apollo_api_key "sua_chave"
vercel secrets add prospec_api_key "$(python3 -c 'import secrets; print(secrets.token_hex(32))')"

# Deploy
vercel --prod
```

URL resultante: `https://prospec-o.vercel.app`

---

## 3. API REST — Local (desenvolvimento)

```bash
pip install -r requirements.txt

# Copiar e preencher variáveis
cp .env.example .env
# edite .env com suas chaves

# Rodar API
uvicorn api:app --reload --port 8000

# Rodar Streamlit
streamlit run app.py
```

Documentação interativa: http://localhost:8000/docs

---

## 4. Uso da API REST

### POST /prospectar

```bash
curl -X POST http://localhost:8000/prospectar \
  -H "Content-Type: application/json" \
  -d '{
    "portaria": "ISO10993",
    "estado": "SP",
    "max_leads": 50,
    "enriquecer": true,
    "concorrencia": 8,
    "filtro_icp": true,
    "icp_score_minimo": 30
  }'
```

**Resposta:**
```json
{
  "portaria": "ISO10993",
  "total": 42,
  "com_email": 28,
  "com_whatsapp": 15,
  "com_decisor": 19,
  "gerado_em": "2026-04-15T10:30:00",
  "leads": [
    {
      "cnpj": "01.234.567/0001-01",
      "nome": "ORTOSINTESE IND COM LTDA",
      "email": "qualidade@ortosintese.com.br",
      "telefone": "(11) 3456-7890",
      "whatsapp": "+5511987654321",
      "decisor": "Ana Paula Rodrigues",
      "cargo_decisor": "Gerente de Qualidade",
      "site": "https://www.ortosintese.com.br",
      "lookalike": 85,
      "icp_acao": "prospectar",
      "icp_flags": "🏭 Segmento ICP: implant | 🎯 CNAE alvo Scitec"
    }
  ]
}
```

### GET /lead/enriquecer

```bash
curl "http://localhost:8000/lead/enriquecer?cnpj=01234567000101&nome=ORTOSINTESE"
```

---

## 5. APIs de Terceiros — Configuração

| API | Gratuito | Pago | Usar para |
|-----|----------|------|-----------|
| **Serper.dev** | 2.500 buscas/mês | $50/mês (100k) | Encontrar site oficial da empresa |
| **Hunter.io** | 25 buscas/mês | $49/mês (500) | Validar e descobrir emails corporativos |
| **Apollo.io** | 10k créditos/mês | $49/mês (75k) | Buscar decisores por cargo e empresa |
| **BrasilAPI** | Ilimitado ✅ | — | CNPJ, QSA, endereço, CNAE |
| **ReceitaWS** | 3 req/min ✅ | — | Fallback CNPJ |
| **Casa dos Dados** | Limitado ✅ | — | Busca por CNAE |

> **Prioridade de uso sem chaves:** BrasilAPI → ReceitaWS → scraping direto.
> Com chaves: Serper → scraping do site → Hunter.io → Apollo.

---

## 6. Performance — Async vs Sync

| Modo | 10 leads | 50 leads | 200 leads |
|------|----------|----------|-----------|
| Sync (requests) | ~30s | ~150s | ~600s |
| **Async (httpx) c/ 8 workers** | **~5s** | **~20s** | **~80s** |

O `enricher_async.py` usa `asyncio.Semaphore(max_concurrent)` para controlar
o número de requisições paralelas sem sobrecarregar as APIs.

---

## 7. Segurança

- Todas as chaves em variáveis de ambiente (`os.getenv`)
- Nunca commite `.env` (está no `.gitignore`)
- API REST protegida por `X-API-Key` header quando `PROSPEC_API_KEY` definida
- Rate limiting nativo via `asyncio.Semaphore`
