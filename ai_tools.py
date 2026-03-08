import json
import os
import glob
from difflib import SequenceMatcher
import openpyxl
import requests
import unicodedata
from datetime import datetime, timedelta

# Import credentials from bot if possible, otherwise hardcode
# Import credentials from environment variables (Cloud) or hardcoded (Local Backup)
NET_USER = os.getenv("NET_USER", "diogoooalbuquerque@gmail.com")
NET_PASS = os.getenv("NET_PASS", "Diogo1984#")
LOGIN_URL = "https://aws.netcontroll.com.br/netadm/api/v1/account/login/"
PARTNER_LOGIN_URL = "https://aws.netcontroll.com.br/netadm/api/v1/parceiro/login"
EXPENSES_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/financeiro/conta-pagar/plano/periodo"
EXPENSES_SUPPLIER_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/financeiro/conta-pagar/fornecedor/periodo"
EXPENSES_DETAILED_URL = "https://aws.netcontroll.com.br/netweb/api/v1/financeiro/despesas/intervalo"
SALES_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/venda/produto/periodo"
INBOUND_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/entrada-mercadoria/periodo"  # Portal: #/estoque/entrada-mercadoria
INVENTORY_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/estoque"              # Portal: #/estoque/inventario
CMV_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/listagem-cmv-produto"
PRODUCT_URL = "https://aws.netcontroll.com.br/netweb/api/v1/estoque/produto"
COMPOSITION_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/composicao"
CANCELLATION_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/item-cancelado/periodo"
COMMISSION_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/comissao/venda/periodo"
REVENUE_PAYMENT_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/faturamento/forma-pagamento/periodo"
CASHIER_CLOSURE_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/fechamento-caixa/periodo"
FISCAL_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/venda/emissor-fiscal/nfce/periodo"
RECEIVABLES_URL = "https://aws.netcontroll.com.br/netweb/api/v1/financeiro/receitas/intervalo"
CASH_BOOK_URL = "https://aws.netcontroll.com.br/netweb/api/v1/financeiro/livro-caixa/conta/extrato"     # Portal: #/financeiro/conciliacao
DASHBOARD_RESUMO_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/venda/produto/periodo"  # Portal: #/dashboard/resumo (base: vendas + despesas combinadas)
DASHBOARD_COMPRAS_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/entrada-mercadoria/periodo"  # Portal: #/dashboard/compra
BALANCETE_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/balancete"                # Portal: #/relatorio/balancete
BRASILAPI_NCM_URL = "https://brasilapi.com.br/api/ncm/v1/{}"


# Cache em memória para evitar chamadas repetidas à BrasilAPI
_NCM_CACHE = {}

# Mapeamento de palavras-chave do nomeSubgrupo do NetControll → capítulos NCM esperados (2 dígitos)
# Capítulo = 2 primeiros dígitos do NCM
# Baseado nos subgrupos reais identificados no Nauan Beach Club
NCM_GRUPO_MAP = {
    # ── Bebidas (cap. 22 = bebidas alcoólicas/não-alcoólicas, 21 = sucos/xaropes) ──
    "BEBIDA": ["22", "21", "20"],
    "ALCOOL": ["22"],
    "DRINK": ["22", "21"],
    "CERVEJA": ["22"],
    "VINHO": ["22"],
    "DESTILADO": ["22"],
    "ESPIRITUOSA": ["22"],
    "REFRIG": ["22"],
    "AGUA": ["22"],
    "SUCO": ["22", "21", "20"],
    "ENERGETIC": ["22"],
    "CHOPP": ["22"],
    # ── Carnes (cap. 02 = carnes frescas, 16 = conservas/embutidos) ──
    "CARNE": ["02", "16"],
    "BOVINO": ["02", "16"],
    "SUINO": ["02", "16"],
    "FRANGO": ["02", "16"],
    "AVE": ["02", "16"],
    "PROTEINA": ["02", "16", "03"],
    "EMBUTIDO": ["02", "16"],
    "DEFUMADO": ["02", "16"],
    # ── Frutos do Mar / Pescados (cap. 03 = frescos, 16 = conservas) ──
    "PESCADO": ["03", "16"],
    "PEIXE": ["03", "16"],
    "FRUTO DO MAR": ["03", "16"],
    "FRUTOS DO MAR": ["03", "16"],
    "CAMARAO": ["03", "16"],
    "SALMAO": ["03", "16"],
    "ATUM": ["03", "16"],
    "LAGOSTA": ["03", "16"],
    "POLVO": ["03", "16"],
    "OSTRA": ["03", "16"],
    "LULA": ["03", "16"],
    # ── Laticínios / Frios (cap. 04 = leite/queijo/manteiga) ──
    "LATICI": ["04", "19"],
    "LATIC": ["04", "19"],
    "QUEIJO": ["04"],
    "LEITE": ["04"],
    "FRIOS": ["04", "02", "16"],
    "REQUEIJAO": ["04"],
    "MANTEIGA": ["04"],
    "CREME": ["04", "21"],
    # ── Hortifrutti / FLV (cap. 07 = legumes, 08 = frutas, 09 = temperos) ──
    "HORTIF": ["07", "08", "09"],
    "LEGUME": ["07"],
    "VERDURA": ["07"],
    "FOLHA": ["07"],
    "FRUTA": ["08"],
    "FLV": ["07", "08", "09"],
    # ── Grãos / Cereais / Massas (cap. 10/11 = grãos/farinha, 19 = massas/pão) ──
    "GRAO": ["10", "11", "19"],
    "CEREAL": ["10", "11", "19"],
    "MASSA": ["19", "11"],
    "PADARIA": ["19"],
    "PAO": ["19"],
    "FARINHA": ["11"],
    "ARROZ": ["10"],
    "FEIJAO": ["07", "11"],
    # ── Temperos / Condimentos (cap. 09 = especiarias, 21 = molhos/extratos) ──
    "TEMPERO": ["09", "21"],
    "CONDIMENT": ["09", "21"],
    "MOLHO": ["21", "09"],
    "ESPECIAR": ["09"],
    "AZEITE": ["15"],
    "OLEO": ["15"],
    "VINAGRE": ["22", "21"],
    # ── Limpeza / Higiene / Descartáveis (cap. 34 = sabão, 38 = químicos, 39 = plásticos) ──
    "LIMPEZA": ["34", "38"],
    "HIGIENE": ["34", "33"],
    "QUIMICO": ["38"],
    "DESCART": ["39", "48"],
    "EMBALAGEM": ["39", "48"],
    "UTENSIL": ["39", "73", "76"],
    "EPI": ["39", "62", "63"],
}

DESKTOP = os.getenv("LOCAL_DATA_PATH", "")

RESTAURANTS = [
    {"name": "Nauan Beach Club", "id": 18784, 
     "sales_file": os.path.join(DESKTOP, "Vendas_Nauan_LIVE.json") if DESKTOP else None, 
     "stock_file": os.path.join(DESKTOP, "Estoque_Nauan_LIVE.json") if DESKTOP else None,
     "expense_file": os.path.join(DESKTOP, "Despesas_Nauan_LIVE.json") if DESKTOP else None,
     "inbound_file": os.path.join(DESKTOP, "entrada de mercadorias nauan.xlsx") if DESKTOP else None,
     "recipe_file": os.path.join(DESKTOP, "composições nauan.xlsx") if DESKTOP else None
    },
    {"name": "Milagres do Toque", "id": 19165, 
     "sales_file": os.path.join(DESKTOP, "Vendas_Milagres_LIVE.json") if DESKTOP else None, 
     "stock_file": os.path.join(DESKTOP, "Estoque_Milagres_LIVE.json") if DESKTOP else None,
     "expense_file": os.path.join(DESKTOP, "Despesas_Milagres_LIVE.json") if DESKTOP else None,
     "inbound_file": None, "recipe_file": None},
    {"name": "Ahau Arte e Cozinha", "id": 20814, 
     "sales_file": os.path.join(DESKTOP, "Vendas_Ahau_LIVE.json") if DESKTOP else None, 
     "stock_file": os.path.join(DESKTOP, "Estoque_Ahau_LIVE.json") if DESKTOP else None,
     "expense_file": os.path.join(DESKTOP, "Despesas_Ahau_LIVE.json") if DESKTOP else None,
     "inbound_file": None, "recipe_file": None},
]

def load_json(path):
    if not path or not os.path.exists(path): return []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except: return []

def find_restaurant_files(rest_name_or_id):
    rest_name_or_id = str(rest_name_or_id).lower()
    for r in RESTAURANTS:
        if rest_name_or_id in r['name'].lower() or rest_name_or_id == str(r['id']):
            return r
    return RESTAURANTS[0]

def safe_float(v):
    if v is None: return 0.0
    if isinstance(v, (int, float)): return float(v)
    s = str(v).replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.').strip()
    try: return float(s)
    except: return 0.0

def normalize_text(text):
    if not text: return ""
    text = str(text)
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn').lower()

def fmt_brl(v, decimals=2):
    """
    Formata valor monetário brasileiro com abreviação automática para facilitar leitura:
      >= 1.000.000  → R$ 1,3 mi
      >= 1.000      → R$ 45,2 mil
      < 1.000       → R$ 945,50   (formato completo)

    Passar decimals=0 para inteiros (ex: quantidades).
    """
    try:
        v = float(v)

        if decimals == 0:
            # Modo inteiro — sem abreviação, só formata
            formatted = f"{v:,.0f}"
            return formatted.replace(",", "X").replace(".", ",").replace("X", ".")

        abs_v = abs(v)
        sinal = "-" if v < 0 else ""

        if abs_v >= 1_000_000:
            # Milhoes: ex 1.308.835,73 -> "1,3 mi"
            abrev = abs_v / 1_000_000
            return f"{sinal}R$ {f'{abrev:.1f}'.replace('.', ',')} mi"
        elif abs_v >= 1_000:
            # Milhares: ex 181.280,11 -> "181,3 mil"
            abrev = abs_v / 1_000
            return f"{sinal}R$ {f'{abrev:.1f}'.replace('.', ',')} mil"
        else:
            # Valor pequeno: formato completo
            formatted = f"{abs_v:,.{decimals}f}"
            formatted = formatted.replace(",", "X").replace(".", ",").replace("X", ".")
            return f"{sinal}R$ {formatted}"
    except:
        return str(v)


def fmt_pct(v, decimals=1):
        formatted = f"{float(v):.{decimals}f}"
        return formatted.replace(".", ",")
    except:
        return str(v)


def match_query(query, target):
    """
    Checks if ALL words in the normalized query exist in the normalized target.
    This allows 'agua gas' to safely match 'AGUA MINERAL COM GAS'.
    """
    q_norm = normalize_text(query).split()
    t_norm = normalize_text(target)
    if not q_norm: return True # Empty query matches anything
    return all(word in t_norm for word in q_norm)


# Stop-words que não ajudam no match de pratos (ignoradas na pontuação)
_STOP = {'de', 'da', 'do', 'para', 'com', 'sem', 'e', 'a', 'o', 'as', 'os',
         'ao', 'na', 'no', 'por', 'g', 'gr', 'kg', 'ml', 'lt', 'unid', 'und'}

def find_best_cost_match(sales_key: str, cost_map: dict, min_score: float = 0.55) -> float:
    """
    Busca o melhor custo no cost_map para um nome de item vendido (sales_key).
    Usa sobreposição de palavras (Jaccard) para tolerar variações de nome:
      'parmegiana de file mignon 2 pessoas' → 'parmegiana de file 2 pessoas'  ✅
      'isca de carne de sol c macaxeira frita' → 'isca carne de sol'          ✅

    Retorna o custo do melhor match ou 0.0 se nenhum atingir min_score.
    """
    import re
    # Remove pontuação (parênteses, barras, etc.) antes de tokenizar
    def _tokens(s: str):
        s = re.sub(r"[^\w\s]", " ", s)   # remove pontuação
        return [w for w in s.split() if w not in _STOP and len(w) > 2]

    if sales_key in cost_map:
        return cost_map[sales_key]

    words_a = _tokens(sales_key)
    if not words_a:
        return 0.0

    best_val   = 0.0
    best_score = 0.0

    for fk, fv in cost_map.items():
        if fv <= 0:
            continue
        words_b = _tokens(fk)
        if not words_b:
            continue
        common = len(set(words_a) & set(words_b))
        score  = common / len(set(words_a) | set(words_b))
        if score > best_score:
            best_score = score
            best_val   = fv

    return best_val if best_score >= min_score else 0.0



def get_session_for_rest(rest_id):
    session = requests.Session()
    session.headers.update({'Content-Type': 'application/json', 'App-Origin': 'Portal'})
    payload = {'Email': NET_USER, 'Senha': NET_PASS}
    try:
        r = session.post(LOGIN_URL, json=payload, timeout=10)
        if r.status_code == 200:
            token = r.json().get('data', {}).get('access_token')
            if token:
                session.headers.update({'Authorization': f'Bearer {token}'})
                p_resp = session.post(PARTNER_LOGIN_URL, data=str(rest_id), timeout=10)
                if p_resp.status_code == 200:
                    ptoken = p_resp.json().get('data', {}).get('access_token')
                    if ptoken:
                        session.headers.update({'Authorization': f'Bearer {ptoken}'})
                        return session
    except: pass
    return None

def fetch_sales_data(rest_id, start_date=None, end_date=None):
    if not start_date: start_date = datetime.now().strftime('%Y-%m-%d')
    if not end_date: end_date = start_date
    session = get_session_for_rest(rest_id)
    if not session: return []
    s_date = f"{start_date}T03:00:00.000Z"
    e_date = f"{end_date}T03:00:00.000Z"
    s_params = {'DataInicial': s_date, 'DataFinal': e_date, 'IncluirCusto': 'true'}
    try:
        r = session.get(SALES_URL, params=s_params)
        if r.status_code == 200: return r.json()
    except: pass
    return []

def fetch_expenses_data(rest_id, start_date=None, end_date=None):
    """Despesas agrupadas por Plano de Contas (categorias: FOLHA, CMV, FIXO, etc.).
    Portal: /relatorio/financeiro-conta-pagar-plano"""
    if not start_date: start_date = datetime.now().strftime('%Y-%m-%d')
    if not end_date: end_date = start_date
    session = get_session_for_rest(rest_id)
    if not session: return []
    s_date = f"{start_date}T03:00:00.000Z"
    e_date = f"{end_date}T03:00:00.000Z"
    exp_params = {'DataInicial': s_date, 'DataFinal': e_date, 'TipoDataDespesa': 0}
    try:
        r = session.get(EXPENSES_URL, params=exp_params)
        if r.status_code == 200: return r.json()
    except: pass
    return []

def fetch_expenses_supplier_data(rest_id, start_date=None, end_date=None):
    """Despesas agrupadas por Fornecedor (ranking: quem recebeu mais).
    Portal: /relatorio/financeiro-conta-pagar-fornecedor"""
    if not start_date: start_date = datetime.now().strftime('%Y-%m-%d')
    if not end_date: end_date = start_date
    session = get_session_for_rest(rest_id)
    if not session: return []
    s_date = f"{start_date}T03:00:00.000Z"
    e_date = f"{end_date}T03:00:00.000Z"
    params = {'DataInicial': s_date, 'DataFinal': e_date}
    try:
        r = session.get(EXPENSES_SUPPLIER_URL, params=params)
        if r.status_code == 200: return r.json()
    except: pass
    return []

def fetch_inbound_data(rest_id, start_date=None, end_date=None):
    if not start_date: start_date = datetime.now().strftime('%Y-%m-%d')
    if not end_date: end_date = start_date
    session = get_session_for_rest(rest_id)
    if not session: return []
    s_date = f"{start_date}T03:00:00.000Z"
    e_date = f"{end_date}T03:00:00.000Z"
    params = {'DataInicial': s_date, 'DataFinal': e_date}
    try:
        r = session.get(INBOUND_URL, params=params)
        if r.status_code == 200: return r.json()
    except: pass
    return []

def fetch_cmv_data(rest_id, start_date=None, end_date=None):
    if not start_date: start_date = datetime.now().strftime('%Y-%m-%d')
    if not end_date: end_date = start_date
    session = get_session_for_rest(rest_id)
    if not session: return []
    s_date = f"{start_date}T03:00:00.000Z"
    e_date = f"{end_date}T03:00:00.000Z"
    params = {'DataInicial': s_date, 'DataFinal': e_date}
    try:
        r = session.get(CMV_URL, params=params)
        if r.status_code == 200: return r.json()
    except: pass
    return []

def fetch_composition_data(rest_id):
    session = get_session_for_rest(rest_id)
    if not session: return []
    try:
        r = session.get(COMPOSITION_URL)
        if r.status_code == 200: return r.json()
    except: pass
    return []

# ─────────────────────────────────────────────────────────────────────────────
# Helpers específicos por restaurante para CMV
# ─────────────────────────────────────────────────────────────────────────────

def get_compras_periodo(rest: dict, start_date: str, end_date: str) -> tuple:
    """
    Retorna (total_compras_R$, fonte_str) para o restaurante e período.
    Prioridade: 1) API de Entradas de Mercadoria  2) Excel local (inbound_file)
    """
    # Tentativa 1 — API NetControll (filtrada por rest_id via partner login)
    try:
        rows = fetch_inbound_data(rest['id'], start_date, end_date)
        if rows:
            total = sum(safe_float(r.get('valorTotal', 0)) for r in rows)
            if total > 0:
                return total, f"API Entradas ({len(rows)} NFs)"
    except: pass

    # Tentativa 2 — Excel local (quando disponível, ex: Nauan)
    inbound_path = rest.get('inbound_file')
    if inbound_path and os.path.exists(inbound_path):
        try:
            wb  = openpyxl.load_workbook(inbound_path, data_only=True)
            ws  = wb.active
            headers = [str(c.value).strip().lower() if c.value else '' for c in ws[1]]

            # Tenta localizar coluna de data e valor total
            col_data  = next((i for i, h in enumerate(headers) if 'data' in h), None)
            col_valor = next((i for i, h in enumerate(headers)
                              if 'total' in h or 'valor' in h), None)

            s_dt = datetime.strptime(start_date, '%Y-%m-%d')
            e_dt = datetime.strptime(end_date,   '%Y-%m-%d')
            total = 0.0
            count = 0
            for row in ws.iter_rows(min_row=2, values_only=True):
                if col_data is not None and row[col_data]:
                    try:
                        cell_dt = row[col_data]
                        if isinstance(cell_dt, str):
                            cell_dt = datetime.strptime(cell_dt[:10], '%Y-%m-%d')
                        elif not isinstance(cell_dt, datetime):
                            cell_dt = None
                        if cell_dt and not (s_dt <= cell_dt <= e_dt):
                            continue
                    except: pass
                if col_valor is not None and row[col_valor]:
                    total += safe_float(row[col_valor])
                    count += 1
            if total > 0:
                return total, f"Excel local ({count} lançamentos)"
        except Exception as e:
            pass

    return 0.0, "sem dados de compras"


def get_ficha_cost_map(rest: dict) -> tuple:
    """
    Retorna (dict {prato_normalizado: custo_por_porcao}, fonte_str)
    Prioridade: 1) API de Composições (COMPOSITION_URL) 2) Excel local (recipe_file)
    """
    # Tentativa 1 — API NetControll
    try:
        rows = fetch_composition_data(rest['id'])
        if rows:
            ficha = {}
            for row in rows:
                dish        = normalize_text(str(row.get('compostoNome', '')))
                custo_ingr  = safe_float(row.get('custo', 0))
                ficha[dish] = ficha.get(dish, 0.0) + custo_ingr
            ficha = {k: v for k, v in ficha.items() if v > 0}
            if ficha:
                return ficha, f"API Composições ({len(ficha)} pratos)"
    except: pass

    # Tentativa 2 — Excel local de receitas
    recipe_path = rest.get('recipe_file')
    if recipe_path and os.path.exists(recipe_path):
        try:
            wb  = openpyxl.load_workbook(recipe_path, data_only=True)
            ws  = wb.active
            ficha        = {}
            current_dish = None
            headers      = [str(c.value).strip().lower() if c.value else ''
                            for c in ws[1]] if ws.max_row > 0 else []

            # Detectar colunas de custo (geralmente "custo total" ou "custo")
            col_custo = next((i for i, h in enumerate(headers)
                              if 'custo' in h and ('total' in h or 'unit' in h)), None)
            if col_custo is None:
                col_custo = next((i for i, h in enumerate(headers) if 'custo' in h), None)

            for row in ws.iter_rows(min_row=2, values_only=True):
                r0 = str(row[0]).strip() if row[0] else ''
                r1 = str(row[1]).strip() if len(row) > 1 and row[1] else ''

                # Linha de cabeçalho de prato (col1 preenchida, col2 vazia ou "Produto:")
                if r0 and (not r1 or r0.startswith('Produto:')):
                    dish_name = r0.replace('Produto:', '').strip()
                    if dish_name:
                        current_dish = normalize_text(dish_name)
                        if current_dish not in ficha:
                            ficha[current_dish] = 0.0
                elif current_dish and col_custo is not None:
                    val = safe_float(row[col_custo]) if len(row) > col_custo else 0
                    ficha[current_dish] += val

            ficha = {k: v for k, v in ficha.items() if v > 0}
            if ficha:
                return ficha, f"Excel local ({len(ficha)} pratos)"
        except Exception as e:
            pass

    return {}, "fichas técnicas indisponíveis"



def get_revenue(restaurant_name, start_date=None, end_date=None):
    rest = find_restaurant_files(restaurant_name)
    data = fetch_sales_data(rest['id'], start_date, end_date)
    total = sum([safe_float(item.get('valor', 0)) for item in data])
    dt_str = start_date if start_date else "ontem"
    return f"Faturamento total de {rest['name']} ({dt_str}): {fmt_brl(total)}"

def get_top_selling_items(restaurant_name, top_n=5, start_date=None, end_date=None):
    rest = find_restaurant_files(restaurant_name)
    data = fetch_sales_data(rest['id'], start_date, end_date)
    sales_map = {}
    for row in data:
        n = row.get("nome", "")
        v = safe_float(row.get("valor", 0))
        q = safe_float(row.get("qtde", 0))
        if n:
            if n not in sales_map:
                sales_map[n] = {'qty': 0, 'rev': 0}
            sales_map[n]['qty'] += q
            sales_map[n]['rev'] += v
    items = list(sales_map.items())
    items.sort(key=lambda x: x[1]['rev'], reverse=True)
    dt_str = start_date if start_date else "ontem"
    res = f"Top {top_n} mais vendidos de {rest['name']} ({dt_str}):\n"
    for k, v in items[:int(top_n)]:
        res += f"- {k}: {v['qty']} unid -> {fmt_brl(v['rev'])}\n"
    return res

def search_sales(restaurant_name, query, start_date=None, end_date=None):
    rest = find_restaurant_files(restaurant_name)
    data = fetch_sales_data(rest['id'], start_date, end_date)
    
    grouped = {}
    for row in data:
        n = row.get("nome", "")
        if match_query(query, n):
            if n not in grouped: grouped[n] = {'qty': 0, 'val': 0.0}
            grouped[n]['qty'] += safe_float(row.get('qtde', 0))
            grouped[n]['val'] += safe_float(row.get('valor', 0))

            
    dt_str = start_date if start_date else "ontem"
    if not grouped: 
        return f"Nenhuma venda encontrada para '{query}' em {rest['name']} ({dt_str})."
        
    res = f"Vendas para '{query}' em {rest['name']} ({dt_str}):\n"
    for k, v in grouped.items():
        res += f"- {k}: {v['qty']} unid -> {fmt_brl(v['val'])}\n"
    return res

def get_stock(restaurant_name, query):
    """Consulta inventário de estoque.
    Tenta API ao vivo primeiro (INVENTORY_URL = portal #/estoque/inventario),
    depois cai no JSON local como fallback."""
    rest = find_restaurant_files(restaurant_name)
    data = []

    # 1. Tenta buscar ao vivo da API de inventário
    try:
        session = get_session_for_rest(rest['id'])
        if session:
            r = session.get(INVENTORY_URL)
            if r.status_code == 200:
                raw = r.json()
                if isinstance(raw, list) and len(raw) > 0:
                    data = raw
                    print(f"[get_stock] Inventário ao vivo OK — {len(data)} itens")
    except Exception as e:
        print(f"[get_stock] API ao vivo falhou: {e}")

    # 2. Fallback: JSON local (sincronizado pelo /sync)
    if not data:
        path = rest.get('stock_file')
        if path and os.path.exists(path):
            data = load_json(path)
            print(f"[get_stock] Usando JSON local — {len(data)} itens")
        else:
            return f"Estoque indisponível para {rest['name']}. Use /sync para sincronizar ou aguarde o auto-sync."

    q_low = normalize_text(query)
    is_generic = q_low in ['estoque', 'inventario', 'tudo', 'completo', 'geral', '']

    matches = []
    for row in data:
        n = row.get('produto', '')
        if is_generic or match_query(query, n):
            est  = safe_float(row.get('estoqueAtual', 0))
            custo = safe_float(row.get('custoAtual', 0))
            matches.append({
                'nome': n,
                'estoque': est,
                'custo': custo,
                'total': est * custo
            })

    if not matches:
        return f"Produto '{query}' não encontrado no estoque de {rest['name']}."

    matches.sort(key=lambda x: SequenceMatcher(None, q_low, normalize_text(x['nome'])).ratio(), reverse=True)

    # Para consultas genéricas, mostra top 30 por valor; para produto específico, mostra os 5 mais relevantes
    if is_generic:
        matches_show = sorted(matches, key=lambda x: -x['total'])[:30]
        total_val = sum(m['total'] for m in matches)
        res = f"📦 **INVENTÁRIO — {rest['name']}** _(portal: #/estoque/inventario)_\n"
        res += f"Total de itens: {len(matches)} | Capital em estoque: R${fmt_brl(total_val)}\n\n"
        for m in matches_show:
            alerta = " ⚠️ RUPTURA" if m['estoque'] <= 0 else (" 🔻 BAIXO" if m['estoque'] < 3 else "")
            res += f"• *{m['nome']}*: {fmt_brl(m['estoque'], 2)} unid × R${fmt_brl(m['custo'])} = R${fmt_brl(m['total'])}{alerta}\n"
        if len(matches) > 30:
            res += f"\n_...e mais {len(matches)-30} itens. Peça 'estoque completo' para ver todos._"
    else:
        matches_show = matches[:5]
        res = f"📦 **Estoque — '{query}' em {rest['name']}**\n"
        for m in matches_show:
            alerta = " ⚠️ RUPTURA" if m['estoque'] <= 0 else (" 🔻 ESTOQUE BAIXO" if m['estoque'] < 3 else "")
            res += f"• *{m['nome']}*: {fmt_brl(m['estoque'], 2)} unid | Custo unit.: R${fmt_brl(m['custo'])} | Total em estoque: R${fmt_brl(m['total'])}{alerta}\n"

    return res

def get_recipe(restaurant_name, dish_name):
    rest = find_restaurant_files(restaurant_name)
    path = rest.get('recipe_file')
    if not path or not os.path.exists(path):
        return f"Planilha de Ficha Técnica indisponível para {rest['name']}."
    
    try:
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb.active
        current_dish = None
        ingredients = []
        dish_found = False
        
        for row in ws.iter_rows(min_row=2, values_only=True):
            r0 = str(row[0]).strip() if row[0] else ""
            r1 = str(row[1]).strip() if len(row) > 1 and row[1] else ""
            
            if r0 and not r1 and not r0.replace('.','').isdigit():
                if dish_found: break # Finished reading the target dish
                if dish_name.lower() in r0.lower():
                    dish_found = True
                    current_dish = r0
            elif dish_found and current_dish:
                r7 = str(row[7]).strip() if len(row) > 7 and row[7] else ""
                r8 = str(row[8]).strip() if len(row) > 8 and row[8] else ""
                
                # Try to extract ingredient name and qty heuristically based on columns
                # In Nauan's composition excel, columns usually look like: 
                # (empty), code, NAME, unit, yield... qty is scattered. Let's just dump row text.
                text_parts = [str(x) for x in row if x and str(x).strip()]
                if len(text_parts) > 1:
                    ingredients.append(" | ".join(text_parts))
                    
        if not dish_found:
             return f"Ficha técnica para '{dish_name}' não encontrada."
        
        res = f"🍲 Ficha Técnica de {current_dish}:\n"
        for i in ingredients:
            res += f"- {i}\n"
        return res[:1000] # Limit size for LLM context
        
    except Exception as e:
        return f"Erro ao ler ficha técnica: {e}"

def get_product_specs(restaurant_name, product_name):
    rest = find_restaurant_files(restaurant_name)
    session = get_session_for_rest(rest['id'])
    if not session: return "Não foi possível autenticar."
    
    try:
        # 1. Search for Product ID
        r = session.get(PRODUCT_URL)
        if r.status_code != 200: return "Erro ao buscar lista de produtos."
        
        products = r.json()
        match = None
        for p in products:
            if product_name.lower() in p.get('nome', '').lower():
                match = p
                break
        
        if not match: return f"Produto '{product_name}' não encontrado no portal."
        
        # 2. Fetch full details
        pid = match['id']
        detail_url = f"{PRODUCT_URL}/{pid}/completo"
        rd = session.get(detail_url)
        if rd.status_code != 200: return f"Erro ao buscar detalhes do produto {pid}."
        
        data = rd.json()
        
        res = f"📄 **DOSSIÊ TÉCNICO: {data.get('nome')}**\n"
        res += f"🏠 Unidade: {rest['name']}\n"
        res += f"🔢 ID: {data.get('id')} | Unidade: {data.get('nomeUnidadeMedida', 'N/A')}\n"
        res += f"🏷️ Subgrupo: {data.get('nomeSubgrupo')}\n"
        res += f"⚖️ NCM: {data.get('ncm') or 'N/A'} | CEST: {data.get('cest') or 'N/A'}\n"
        res += f"💰 Preço de Venda: {fmt_brl(safe_float(data.get('preco')))}\n\n"

        comp = data.get('composicoes', [])
        if comp:
            res += "🧪 **COMPOSIÇÃO / FICHA TÉCNICA:**\n"
            for c in comp:
                qty = safe_float(c.get('quantidade'))
                u = c.get('nomeUnidadeMedidaIngrediente', '')
                res += f"  • {c.get('nomeIngrediente')}: {qty} {u}\n"
        else:
            res += "ℹ️ Produto sem composição cadastrada no portal."
            
        return res[:4000]
    except Exception as e:
        return f"Erro ao extrair dossiê: {e}"

def analyze_recipes_profitability(restaurant_name, query=None):
    rest = find_restaurant_files(restaurant_name)
    
    # Try Live Data first
    try:
        live_data = fetch_composition_data(rest['id'])
        if live_data:
            dishes_map = {}
            for item in live_data:
                d_name = item.get('compostoNome')
                if not d_name: continue
                
                if d_name not in dishes_map:
                    dishes_map[d_name] = {
                        "name": d_name,
                        "group": "Geral", # Composition report doesn't explicitly have group in this endpoint
                        "cost": 0.0,
                        "ingredients": []
                    }
                
                ing_qty = safe_float(item.get('quantidade'))
                ing_cost = safe_float(item.get('custo'))
                dishes_map[d_name]["cost"] += ing_cost
                dishes_map[d_name]["ingredients"].append({
                    "name": item.get('composicaoNome'),
                    "qty": ing_qty,
                    "t_cost": ing_cost,
                    "u_cost": (ing_cost / ing_qty) if ing_qty > 0 else 0
                })
            
            dishes = list(dishes_map.values())
            if query:
                dishes = [d for d in dishes if match_query(query, d["name"])]
                
            if dishes:
                return build_profitability_report(rest, dishes, query)
    except: pass

    # Fallback to Excel if live fails or returns nothing
    path = rest.get('recipe_file')
    if not path or not os.path.exists(path):
        return f"Não foi possível obter dados live nem Planilha de Ficha Técnica para {rest['name']}."
        
    try:
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb.active
        dishes = []
        current_dish = None
        current_group = ""
        current_cost = 0.0
        ingredients = []
        
        for row in ws.iter_rows(min_row=1, values_only=True):
            r0 = str(row[0]).strip() if row[0] else ""
            r1 = str(row[1]).strip() if len(row) > 1 and row[1] else ""
            if r0.startswith("Subgrupo:"):
                current_group = r0.replace('Subgrupo:', '').strip()
                continue
            if r0.startswith("Produto:") or (r0 and not r1 and not r0.replace('.','').isdigit() and not r0.startswith("Subgrupo:")):
                if current_dish:
                    dishes.append({"name": current_dish, "group": current_group, "cost": current_cost, "ingredients": ingredients})
                current_dish = r0.split(":", 1)[1].strip() if ":" in r0 else r0
                cost_str = str(row[5]) if len(row) > 5 and row[5] else "0"
                current_cost = safe_float(cost_str)
                ingredients = []
            elif current_dish and len(row) >= 5:
                if isinstance(row[2], (int, float)):
                    ing_name = str(row[1]).strip() if row[1] else ""
                    ing_qty = safe_float(row[2])
                    ing_u_cost = safe_float(row[4])
                    ing_t_cost = safe_float(row[5])
                    if ing_name:
                        ingredients.append({"name": ing_name, "qty": ing_qty, "t_cost": ing_t_cost, "u_cost": ing_u_cost})
        if current_dish:
            dishes.append({"name": current_dish, "group": current_group, "cost": current_cost, "ingredients": ingredients})
        
        if query:
            dishes = [d for d in dishes if match_query(query, d["name"]) or match_query(query, d["group"])]
            
        return build_profitability_report(rest, dishes, query)
    except Exception as e:
        return f"Erro ao auditar as fichas técnicas: {e}"

def build_profitability_report(rest, dishes, query=None):
    zero_cost_ingredients = set()
    for d in dishes:
        for ing in d['ingredients']:
            if ing.get('u_cost', 0) == 0 or ing.get('t_cost', 0) == 0:
                zero_cost_ingredients.add(ing['name'])
                
    dishes.sort(key=lambda x: x["cost"], reverse=True)
    q_label = f" ({query.upper()})" if query else ""
    report = f"📊 **AUDITORIA DE FICHAS TÉCNICAS LIVE: {rest['name']}{q_label}**\n\n"
    
    z_limit = 40 if query else 15
    if zero_cost_ingredients:
        report += "🚨 **ALERTA CRÍTICO: INSUMOS COM CUSTO ZERO (R$ 0,00)!**\n"

