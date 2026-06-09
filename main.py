"""
lexia-scraper — scraping de processos judiciais (eSAJ + PJe).
Roda em servidor brasileiro (Railway SA East) para contornar o bloqueio de IPs
estrangeiros pelos portais dos tribunais.

Salva os processos encontrados diretamente no Supabase via service_role.
"""

import os, re, asyncio, httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from supabase import create_client, Client
from typing import Optional

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
CRON_SECRET  = os.environ.get("CRON_SECRET", "cron-secret")

app = FastAPI(title="LexIA Scraper", version="1.0.0")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
CNJ_RE = re.compile(r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}")

def numero_valido(n: str) -> bool:
    """Rejeita números placeholder (ano fora de 1990-2030, dígitos repetidos)."""
    try:
        ano = int(n[8:12])
        return 1990 <= ano <= 2030 and n != "9999999-99.9999.9.99.9999"
    except Exception:
        return False

# ─── Helpers ──────────────────────────────────────────────────────────────────

def check_auth(authorization: str):
    if authorization != f"Bearer {CRON_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")

def get_owner(organization_id: str) -> Optional[str]:
    r = sb.from_("organization_members")\
          .select("user_id")\
          .eq("organization_id", organization_id)\
          .eq("role", "admin")\
          .single()\
          .execute()
    return r.data["user_id"] if r.data else None

def salvar_processo(organization_id: str, owner_id: str, numero: str,
                    nome: str, tribunal: str, fonte: str) -> bool:
    try:
        res = sb.from_("processos")\
               .select("id")\
               .eq("organization_id", organization_id)\
               .eq("numero", numero)\
               .limit(1)\
               .execute()
        if res and res.data:
            return False  # já existe
    except Exception:
        pass
    sb.from_("processos").insert({
        "organization_id": organization_id,
        "owner_id": owner_id,
        "numero": numero,
        "titulo": f"Processo {numero}",
        "parte": nome,
        "tribunal": tribunal,
        "status": "ativo",
        "fonte": fonte,
    }).execute()
    return True

# ─── eSAJ TJCE ────────────────────────────────────────────────────────────────

async def esaj_obter_sessao(client: httpx.AsyncClient) -> str:
    try:
        r = await client.get(
            "https://esaj.tjce.jus.br/cpopg/open.do",
            headers={"User-Agent": UA},
            follow_redirects=True,
            timeout=15,
        )
        sc = r.headers.get("set-cookie", "")
        # httpx junta cookies automaticamente, mas tenta extrair manualmente
        match = re.search(r"JSESSIONID=([^;]+)", sc)
        if match:
            return match.group(1)
        # Fallback: cookies do httpx
        jsid = client.cookies.get("JSESSIONID")
        return jsid or ""
    except Exception as e:
        print(f"[eSAJ] sessão: {e}")
        return ""

async def esaj_buscar(oab: str, uf: str, jsession: str) -> list[str]:
    numeros: list[str] = []
    cookies = {"JSESSIONID": jsession} if jsession else {}
    async with httpx.AsyncClient(cookies=cookies, follow_redirects=True, timeout=25) as c:
        for pagina in range(1, 11):
            try:
                r = await c.post(
                    "https://esaj.tjce.jus.br/cpopg/search.do",
                    headers={
                        "User-Agent": UA,
                        "Referer": "https://esaj.tjce.jus.br/cpopg/open.do",
                        "Accept": "text/html,application/xhtml+xml",
                    },
                    data={
                        "conversationId": "",
                        "cbPesquisa": "NUMOAB",
                        "dePesquisaNuOAB": oab,
                        "dePesquisaUfOAB": uf,
                        "gateway": "true",
                        "paginaConsulta": str(pagina),
                        "localPesquisa": "INTERF_INICIAL",
                    },
                )
                html = r.text
            except Exception as e:
                print(f"[eSAJ] p{pagina}: {e}")
                break

            if any(x in html for x in ["Não existem", "nenhum processo", "Nenhum processo"]) \
               or len(html) < 1000:
                break

            found = list(set(CNJ_RE.findall(html)))
            if not found:
                break
            for n in found:
                if n not in numeros and numero_valido(n):
                    numeros.append(n)

            if not any(x in html for x in ["Próxima", "próxima", "paginaConsulta"]):
                break

    return numeros

# ─── PJe (TJCE, TRT7, TRF5) ──────────────────────────────────────────────────

PJE_INSTANCIAS_CE = [
    {"nome": "TJCE-1G", "base": "https://pje-consulta.tjce.jus.br/pje1grau", "tribunal": "TJCE"},
    {"nome": "TJCE-2G", "base": "https://pje-consulta.tjce.jus.br/pje2grau", "tribunal": "TJCE"},
    {"nome": "TRT7",    "base": "https://pje.trt7.jus.br/pje",               "tribunal": "TRT7"},
    {"nome": "TRF5",    "base": "https://pje.trf5.jus.br/pje",               "tribunal": "TRF5"},
]

async def pje_buscar(inst: dict, oab: str, uf: str) -> list[str]:
    numeros: list[str] = []
    base = inst["base"]
    consulta_url = f"{base}/ConsultaPublica/listView.seam"

    async with httpx.AsyncClient(follow_redirects=True, timeout=25) as c:
        # Tentativa 1: REST JSON
        for url in [
            f"{base}/rest/processo/consultapublica?oabNumero={oab}&oabUF={uf}&pagina=0&tamanhoPagina=50",
            f"{base}/api/v1/processos?oabNumero={oab}&oabUf={uf}&size=50",
        ]:
            try:
                r = await c.get(url, headers={"User-Agent": UA, "Accept": "application/json"})
                if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
                    j = r.json()
                    items = j.get("content") or j.get("processos") or (j if isinstance(j, list) else [])
                    for item in items:
                        n = item.get("numeroProcesso") or item.get("numero")
                        if n and CNJ_RE.match(n) and numero_valido(n) and n not in numeros:
                            numeros.append(n)
                    if numeros:
                        print(f"[PJe] {inst['nome']} REST: {len(numeros)}")
                        return numeros
            except Exception as e:
                print(f"[PJe] {inst['nome']} REST: {e}")

        # Tentativa 2: GET página → extrai todos campos hidden → POST com OAB
        try:
            rg = await c.get(consulta_url, headers={"User-Agent": UA})
            html_get = rg.text

            # Extrai todos inputs com seus valores atuais
            form_data: dict[str, str] = {}
            for m in re.finditer(r'<input([^>]+)>', html_get, re.IGNORECASE):
                attrs = m.group(1)
                name  = re.search(r'name="([^"]*)"',  attrs)
                value = re.search(r'value="([^"]*)"', attrs)
                if name:
                    form_data[name.group(1)] = value.group(1) if value else ""

            # Campos do RichFaces AJAX
            nome = inst["nome"]
            if "TJCE" in nome:
                form_data["fPP:Decoration:numeroOAB"] = oab
                form_data["fPP:searchProcessos"]      = "Pesquisar"
            else:
                for key in list(form_data.keys()):
                    if "oab" in key.lower() and "numero" in key.lower():
                        form_data[key] = oab

            # RichFaces 3.x exige AJAXREQUEST para disparar busca AJAX
            form_data["AJAXREQUEST"] = "_viewRoot"
            form_data["javax.faces.ViewState"] = form_data.get("javax.faces.ViewState", "j_id1")

            rp = await c.post(
                consulta_url,
                headers={
                    "User-Agent": UA,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": consulta_url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "text/html,application/xhtml+xml,*/*",
                },
                data=form_data,
            )
            if rp.status_code == 200:
                found = [n for n in set(CNJ_RE.findall(rp.text)) if numero_valido(n)]
                for n in found:
                    if n not in numeros:
                        numeros.append(n)
                print(f"[PJe] {inst['nome']} HTML: {len(found)} válidos")
        except Exception as e:
            print(f"[PJe] {inst['nome']} HTML: {e}")

    return numeros

# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"ok": True, "service": "lexia-scraper"}

@app.post("/pje-debug")
async def pje_debug(authorization: str = Header(...)):
    check_auth(authorization)
    base = "https://pje-consulta.tjce.jus.br/pje1grau"
    result = {}
    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as c:
        try:
            rg = await c.get(f"{base}/ConsultaPublica/listView.seam", headers={"User-Agent": UA})
            result["get_status"] = rg.status_code
            result["final_url"] = str(rg.url)
            html = rg.text
            result["html_len"] = len(html)
            # Extrai ViewState
            vs = re.search(r'id="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html)
            result["viewstate"] = vs.group(1)[:80] if vs else "NÃO ENCONTRADO"
            # Extrai todos os inputs do form
            inputs = re.findall(r'<input[^>]+name="([^"]+)"', html)
            result["form_fields"] = list(set(inputs))
            # Tenta POST com OAB
            if vs:
                rp = await c.post(
                    f"{base}/ConsultaPublica/listView.seam",
                    headers={"User-Agent": UA, "Content-Type": "application/x-www-form-urlencoded",
                             "Referer": f"{base}/ConsultaPublica/listView.seam"},
                    data={"javax.faces.ViewState": vs.group(1), **{f: "10727" if "oab" in f.lower() else ("CE" if "uf" in f.lower() or "estado" in f.lower() else "") for f in inputs if "oab" in f.lower() or "uf" in f.lower() or "estado" in f.lower()}},
                )
                result["post_status"] = rp.status_code
                result["post_url"] = str(rp.url)
                result["numeros"] = list(set(CNJ_RE.findall(rp.text)))
                result["post_snippet"] = rp.text[:800]
        except Exception as e:
            result["error"] = str(e)
    return result

@app.post("/esaj-debug")
async def esaj_debug(authorization: str = Header(...)):
    check_auth(authorization)
    result = {}
    async with httpx.AsyncClient(follow_redirects=True, timeout=15) as c:
        # Sessão
        try:
            r = await c.get("https://esaj.tjce.jus.br/cpopg/open.do", headers={"User-Agent": UA})
            jsession = c.cookies.get("JSESSIONID") or re.search(r"JSESSIONID=([^;]+)", r.headers.get("set-cookie","") or "")
            if hasattr(jsession, "group"): jsession = jsession.group(1)
            result["jsession"] = jsession or "não obtido"
            result["open_status"] = r.status_code
        except Exception as e:
            result["open_error"] = str(e)
            return result

    # Busca OAB 10727 CE
    async with httpx.AsyncClient(follow_redirects=True, timeout=25,
                                  cookies={"JSESSIONID": result.get("jsession","")}) as c:
        try:
            r2 = await c.post(
                "https://esaj.tjce.jus.br/cpopg/search.do",
                headers={"User-Agent": UA, "Referer": "https://esaj.tjce.jus.br/cpopg/open.do",
                         "Accept": "text/html,application/xhtml+xml"},
                data={"conversationId":"","cbPesquisa":"NUMOAB","dePesquisaNuOAB":"10727",
                      "dePesquisaUfOAB":"CE","gateway":"true","paginaConsulta":"1",
                      "localPesquisa":"INTERF_INICIAL"},
            )
            html = r2.text
            result["search_status"] = r2.status_code
            result["html_len"] = len(html)
            result["html_snippet"] = html[:1500]
            result["processos_encontrados"] = list(set(CNJ_RE.findall(html)))
            result["tem_nao_existem"] = "Não existem" in html or "nenhum processo" in html
            result["tem_captcha"] = "captcha" in html.lower()
            result["final_url"] = str(r2.url)
        except Exception as e:
            result["search_error"] = str(e)
    return result

@app.post("/esaj-sync")
async def esaj_sync(authorization: str = Header(...)):
    check_auth(authorization)

    oabs_res = sb.from_("oabs_monitoradas")\
                 .select("organization_id,numero_oab,estado_oab,nome_advogado")\
                 .eq("ativo", True)\
                 .execute()
    oabs = oabs_res.data or []
    if not oabs:
        return {"ok": True, "msg": "Nenhuma OAB ativa."}

    total_novos = 0
    total_existiam = 0

    async with httpx.AsyncClient(follow_redirects=True, timeout=15) as sess_client:
        jsession = await esaj_obter_sessao(sess_client)
    print(f"[eSAJ] JSESSIONID: {'✅' if jsession else '❌'}")

    for row in oabs:
        if row["estado_oab"] != "CE":
            continue
        org_id = row["organization_id"]
        oab    = row["numero_oab"]
        nome   = row["nome_advogado"]

        owner_id = get_owner(org_id)
        if not owner_id:
            continue

        numeros = await esaj_buscar(oab, row["estado_oab"], jsession)
        print(f"[eSAJ] CE {oab} ({nome}): {len(numeros)} processos")

        for numero in numeros:
            if salvar_processo(org_id, owner_id, numero, nome, "TJCE", "eSAJ"):
                total_novos += 1
            else:
                total_existiam += 1

    return {
        "ok": True,
        "fonte": "eSAJ",
        "jsession_ok": bool(jsession),
        "processos_novos": total_novos,
        "ja_existiam": total_existiam,
    }

@app.post("/pje-sync")
async def pje_sync(authorization: str = Header(...)):
    check_auth(authorization)
    try:
        oabs_res = sb.from_("oabs_monitoradas")\
                     .select("organization_id,numero_oab,estado_oab,nome_advogado")\
                     .eq("ativo", True)\
                     .execute()
        oabs = oabs_res.data or []
        if not oabs:
            return {"ok": True, "msg": "Nenhuma OAB ativa."}

        instancias_por_uf = {"CE": PJE_INSTANCIAS_CE}
        total_novos = 0
        total_existiam = 0
        log = []

        for row in oabs:
            instancias = instancias_por_uf.get(row["estado_oab"])
            if not instancias:
                continue
            org_id = row["organization_id"]
            oab    = row["numero_oab"]
            nome   = row["nome_advogado"]

            owner_id = get_owner(org_id)
            if not owner_id:
                log.append(f"sem owner para org {org_id}")
                continue

            for inst in instancias:
                try:
                    numeros = await pje_buscar(inst, oab, row["estado_oab"])
                    log.append(f"{inst['nome']} OAB {oab}: {len(numeros)} encontrados")
                    for numero in numeros:
                        if salvar_processo(org_id, owner_id, numero, nome, inst["tribunal"], "PJe"):
                            total_novos += 1
                        else:
                            total_existiam += 1
                except Exception as e:
                    log.append(f"{inst['nome']} OAB {oab} erro: {e}")

        return {
            "ok": True,
            "fonte": "PJe",
            "processos_novos": total_novos,
            "ja_existiam": total_existiam,
            "log": log,
        }
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()}
