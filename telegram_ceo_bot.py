import telebot
import os
import openpyxl
import requests
from datetime import datetime, timedelta
import json
import re
import ai_manager
import ai_tools

# Bot Token
TOKEN = os.getenv("TELEGRAM_TOKEN", "")
bot = telebot.TeleBot(TOKEN)

# --- CONFIGURAÇÕES ---
DESKTOP = os.getenv("LOCAL_DATA_PATH", "")

# Credenciais do Portal
NET_USER = os.getenv("NET_USER", "")
NET_PASS = os.getenv("NET_PASS", "")

# API Configuration
API_REPORT_BASE = "https://aws.netcontroll.com.br/netreport/api/v1"
LOGIN_URL = "https://aws.netcontroll.com.br/netadm/api/v1/account/login/"
PARTNER_LOGIN_URL = "https://aws.netcontroll.com.br/netadm/api/v1/parceiro/login"
EXPENSES_URL          = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/financeiro/conta-pagar/plano/periodo"
EXPENSES_SUPPLIER_URL = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/financeiro/conta-pagar/fornecedor/periodo"
PAYMENT_METHODS_URL   = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/faturamento-forma-pagamento"  # Portal: /relatorio/faturamento-forma-pagamento

RESTAURANTS = [
    {"name": "Nauan Beach Club", "id": 18784, 
     "sales_file": os.path.join(DESKTOP, "Vendas_Nauan_LIVE.json"), 
     "stock_file": os.path.join(DESKTOP, "Estoque_Nauan_LIVE.json"),
     "expense_file": os.path.join(DESKTOP, "Despesas_Nauan_LIVE.json")},
    
    {"name": "Milagres do Toque", "id": 19165, 
     "sales_file": os.path.join(DESKTOP, "Vendas_Milagres_LIVE.json"), 
     "stock_file": os.path.join(DESKTOP, "Estoque_Milagres_LIVE.json"),
     "expense_file": os.path.join(DESKTOP, "Despesas_Milagres_LIVE.json")},
    
    {"name": "Ahau Arte e Cozinha", "id": 20814, 
     "sales_file": os.path.join(DESKTOP, "Vendas_Ahau_LIVE.json"), 
     "stock_file": os.path.join(DESKTOP, "Estoque_Ahau_LIVE.json"),
     "expense_file": os.path.join(DESKTOP, "Despesas_Ahau_LIVE.json")},
]

def get_session(partner_id=None):
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Content-Type': 'application/json',
        'App-Origin': 'Portal'
    })
    
    # 1. Main Login
    payload = {'Email': NET_USER, 'Senha': NET_PASS}
    try:
        resp = session.post(LOGIN_URL, json=payload, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get('data', {})
            token = data.get('access_token')
            if token:
                session.headers.update({'Authorization': f'Bearer {token}'})
                
                # 2. Optional Partner Login
                if partner_id:
                    # Pass ID as raw integer in body as discovered
                    p_resp = session.post(PARTNER_LOGIN_URL, data=str(partner_id), timeout=15)
                    if p_resp.status_code == 200:
                        p_data = p_resp.json().get('data', {})
                        p_token = p_data.get('access_token')
                        if p_token:
                            session.headers.update({'Authorization': f'Bearer {p_token}'})
                            return session
                else:
                    return session
        else:
            print(f"Login Error: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"Auth Exception: {e}")
    return None

def download_sales_json(session, target_date, rest_id, save_path):
    print(f"Baixando JSON de vendas (Casa: {rest_id}, Data: {target_date})...")
    date_str = f"{target_date}T03:00:00.000Z"
    
    r_partner = session.post(PARTNER_LOGIN_URL, data=str(rest_id))
    if r_partner.status_code == 200:
        p_token = r_partner.json().get('data', {}).get('access_token', '')
        if p_token:
             session.headers.update({'Authorization': f'Bearer {p_token}'})
             
    url = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/venda/produto/periodo"
    params = {
        'DataInicial': date_str,
        'DataFinal': date_str,
        'IncluirCusto': 'false',
        'IncluirCaixa': 'false',
        'IncluirTipoDoc': 'false',
        'IncluirDataCaixa': 'false'
    }
    
    try:
        resp = session.get(url, params=params)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                import json
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
                print(f" [OK] JSON salvo: {save_path} ({len(data)} registros)")
                return True
        else:
            print(f" [!] Erro API HTTP {resp.status_code}")
    except Exception as e:
        print(f" [!] Exceção download: {e}")
    return False


def download_stock_json(session, rest_id, save_path):
    print(f"Baixando JSON de estoque (Casa: {rest_id})...")
    r_partner = session.post(PARTNER_LOGIN_URL, data=str(rest_id))
    if r_partner.status_code == 200:
        p_token = r_partner.json().get('data', {}).get('access_token', '')
        if p_token:
             session.headers.update({'Authorization': f'Bearer {p_token}'})
             
    url = "https://aws.netcontroll.com.br/netreport/api/v1/netweb/relatorio/estoque"
    try:
        resp = session.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                import json
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
                return True
    except Exception as e:
        print(e)
    return False

def download_expenses_json(session, target_date, rest_id, save_path):
    print(f"Baixando JSON de despesas (Casa: {rest_id}, Data: {target_date})...")
    date_str = f"{target_date}T03:00:00.000Z"
    
    # Partner switch
    r_partner = session.post(PARTNER_LOGIN_URL, data=str(rest_id))
    if r_partner.status_code == 200:
        p_token = r_partner.json().get('data', {}).get('access_token', '')
        if p_token:
             session.headers.update({'Authorization': f'Bearer {p_token}'})

    params = {
        'DataInicial': date_str,
        'DataFinal': date_str,
        'TipoDataDespesa': 0 # Vencimento
    }
    
    try:
        resp = session.get(EXPENSES_URL, params=params)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
                return True
    except Exception as e:
        print(f"Error download_expenses: {e}")
    return False

def analyze_sales(file_path):
    print(f"Analisando Vendas: {file_path}")
    total_sales = 0
    top_items = []
    
    if not os.path.exists(file_path):
        return None
        
    if file_path.endswith('.json'):
        import json
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        item_sales = {}
        for row in data:
            name = row.get("nome", "")
            qty = row.get("qtde", 0)
            value = row.get("valor", 0)
            if not name: continue
            try:
                val = float(value)
                qty = float(qty)
                total_sales += val
                if name in item_sales:
                    item_sales[name]['revenue'] += val
                    item_sales[name]['qty'] += qty
                else:
                    item_sales[name] = {"name": name, "qty": qty, "revenue": val}
            except:
                pass
        top_items = list(item_sales.values())
        top_items.sort(key=lambda x: x['revenue'], reverse=True)
        return {
            'total_sales': total_sales,
            'top_items': top_items[:5]
        }
    return None # Should not happen if file_path ends with .json

def clean_val(v):
    if v is None: return 0.0
    if isinstance(v, (int, float)): return float(v)
    s = str(v).replace('R$', '').replace('\xa0', '').replace('.', '').replace(',', '.').strip()
    try: return float(s)
    except: return 0.0

# ── Mapa de ações pendentes por chat ──────────────────────────────────────────
# Guarda qual botão foi clicado enquanto aguardamos a seleção do restaurante
pending_actions: dict = {}   # { chat_id: "action_key" }

# Nomes curtos usados nos botões inline (callback_data tem limite de 64 bytes)
RESTAURANT_CHOICES = [
    ("🏖️ Nauan",   "Nauan Beach Club"),
    ("🌊 Milagres", "Milagres do Toque"),
    ("🎨 Ahau",     "Ahau Arte e Cozinha"),
    ("🏢 Todas as Casas", "__ALL__"),
]

def ask_restaurant(chat_id, action_key: str):
    """Envia botões inline para selecionar a casa antes de executar a ação."""
    pending_actions[chat_id] = action_key
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        telebot.types.InlineKeyboardButton(label, callback_data=f"rest|{code}")
        for label, code in RESTAURANT_CHOICES
    ]
    markup.add(*buttons)
    bot.send_message(
        chat_id,
        "🏠 *Para qual casa é essa análise?*",
        parse_mode="Markdown",
        reply_markup=markup
    )

def build_question_for_action(action_key: str, restaurant_name: str) -> str:
    """Monta a pergunta natural em PT para a IA com base na ação do botão e na casa."""
    prefix = f"Para o restaurante {restaurant_name}: "
    questions = {
        # Financeiro
        "dre":            prefix + "Gere o DRE gerencial completo do período atual.",
        "contas_pagar":   prefix + "Audite as contas a pagar detalhadas do período atual.",
        "raio_x":         prefix + "Gere o Raio-X financeiro completo (saldo, recebíveis, a pagar).",
        "break_even":     prefix + "Calcule o ponto de equilíbrio (break-even) mensal.",
        "conciliacao":    prefix + "Faça a conciliação de notas fiscais de entrada vs financeiro.",
        "fornecedores":   prefix + "Gere o ranking e análise completa de fornecedores do mês.",
        # Estoque e CMV
        "plano_compras":  prefix + "Gere o plano de compras inteligente para os próximos 7 dias.",
        "rupturas":       prefix + "Audite as rupturas e desperdícios de estoque (waste audit).",
        "cmv":            prefix + "Análise completa de CMV e markup do período atual.",
        "fichas":         prefix + "Audite todas as fichas técnicas e analise a lucratividade das receitas.",
        "markup_diario":  prefix + "Gere sugestões de precificação dinâmica baseada no clima e estoque.",
        "giro_insumo":    prefix + "Analise o giro de estoque e identifique capital empatado.",
        "cao_guarda":     "Rode o Cão de Guarda agora e me mostre todos os ingredientes que subiram 8% ou mais nas últimas 2 semanas em qualquer casa.",
        # RH e Operação
        "escala":         prefix + "Calcule a necessidade de RH e escala de equipe para amanhã.",
        "garcons":        prefix + "Gere o relatório de produtividade e comissões dos garçons.",
        "caixa":          prefix + "Audite as quebras de caixa dos últimos 30 dias.",
        "cancelamentos":  prefix + "Analise os motivos de cancelamentos de itens dos últimos 30 dias.",
        "review":         prefix + "Mostre o painel consolidado de avaliações Google das 3 casas (nota, reviews negativos, tendência).",
        "review_casa":    prefix + "Analise as avaliações Google + cancelamentos + faturamento de uma casa específica.",
        "fraude":         prefix + "Verifique alertas de fraude e anti-perdas em tempo real.",
        # Geral
        "resumo_diario":  prefix + "Gere o briefing executivo diário completo.",
        "alertas":        prefix + "Gere os alertas proativos estratégicos agora.",
        "cenario":        prefix + "Gere o cenário geral completo de vendas e despesas de hoje.",
    }
    return questions.get(action_key, prefix + action_key)

# ── Admin ─────────────────────────────────────────────────────────────────────
ADMIN_CHAT_ID = 7907362924  # chat_id do Diogo (dono do sistema)

# ── Rastreamento do restaurante ativo por conversa ────────────────────────────
# Garante que cada usuário sempre consulte o restaurante correto, mesmo em
# perguntas de continuação sem nome explícito ("e o estoque?", "e hoje?").
_last_restaurant: dict = {}  # {chat_id: restaurant_name}

_RESTAURANT_ALIASES = {
    "nauan":        "Nauan Beach Club",
    "milagres":     "Milagres do Toque",
    "toque":        "Milagres do Toque",
    "ahau":         "Ahau Arte e Cozinha",
}

def _get_allowed_restaurants(chat_id: int) -> list:
    """Lista de dicts de restaurantes permitidos para este chat_id."""
    if chat_id == ADMIN_CHAT_ID:
        return RESTAURANTS
    client = get_client(chat_id)
    if client:
        return client.get("restaurants", [])
    return []

def _detect_restaurant(text: str, allowed: list) -> str | None:
    """Detecta se o texto menciona explicitamente um restaurante permitido.
    Usa word boundary para evitar falsos positivos (ex: 'toque' em 'estoque').
    """
    import re
    if not text:
        return None
    allowed_names = {r["name"] for r in allowed}
    text_lower = text.lower()
    for alias, name in _RESTAURANT_ALIASES.items():
        if re.search(r'\b' + re.escape(alias) + r'\b', text_lower) and name in allowed_names:
            return name
    return None

def _resolve_restaurant(chat_id: int, text: str = "") -> str:
    """
    Resolve qual restaurante usar para esta mensagem:
    1. Se o texto menciona explicitamente um restaurante permitido → usa ele e memoriza.
    2. Senão, usa o último restaurante mencionado nesta conversa.
    3. Senão, usa o primeiro restaurante permitido do usuário.
    Para clientes com 1 só restaurante, retorna sempre esse único restaurante.
    """
    allowed = _get_allowed_restaurants(chat_id)
    if not allowed:
        return "Nauan Beach Club"

    detected = _detect_restaurant(text, allowed)
    if detected:
        _last_restaurant[chat_id] = detected
        return detected

    remembered = _last_restaurant.get(chat_id)
    if remembered and any(r["name"] == remembered for r in allowed):
        return remembered

    # Padrão: primeiro restaurante cadastrado
    default = allowed[0]["name"]
    _last_restaurant[chat_id] = default
    return default

# ── Registro de clientes (multi-tenant) ──────────────────────────────────────
import threading as _threading
from cryptography.fernet import Fernet, InvalidToken as _InvalidToken

CLIENTS_FILE = "clients.json"
_clients_lock = _threading.Lock()

def _get_fernet() -> Fernet | None:
    """Retorna instância Fernet usando FERNET_KEY do ambiente, ou None se não configurada."""
    key = os.getenv("FERNET_KEY", "")
    if not key:
        return None
    try:
        return Fernet(key.encode())
    except Exception:
        return None

def _encrypt_pass(plain: str) -> str:
    """Criptografa senha. Retorna texto cifrado ou o valor original se sem chave."""
    f = _get_fernet()
    if not f or not plain:
        return plain
    return f.encrypt(plain.encode()).decode()

def _decrypt_pass(stored: str) -> str:
    """Descriptografa senha. Retorna valor original se sem chave ou já em texto puro."""
    f = _get_fernet()
    if not f or not stored:
        return stored
    try:
        return f.decrypt(stored.encode()).decode()
    except _InvalidToken:
        return stored  # já estava em texto puro (migração gradual)

def load_clients() -> dict:
    with _clients_lock:
        if os.path.exists(CLIENTS_FILE):
            try:
                with open(CLIENTS_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except: return {}
        return {}

def save_clients(clients: dict):
    with _clients_lock:
        tmp = CLIENTS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(clients, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CLIENTS_FILE)  # atomic write — evita arquivo corrompido

def get_client(chat_id) -> dict | None:
    c = load_clients().get(str(chat_id))
    if c:
        # Descriptografa senha em memória, nunca modifica o arquivo
        c = dict(c)
        c["net_pass"] = _decrypt_pass(c.get("net_pass", ""))
    return c

def register_client(chat_id, net_user: str, net_pass: str, restaurants: list,
                    telegram_name: str = "", plano: str = "basico"):
    clients = load_clients()
    clients[str(chat_id)] = {
        "net_user": net_user,
        "net_pass": _encrypt_pass(net_pass),
        "restaurants": restaurants,
        "plano": plano,
        "plano_expira": None,  # preenchido pelo admin ao aprovar
        "active": False,
        "approved": False,
        "telegram_name": telegram_name,
        "registered_at": datetime.now().strftime("%Y-%m-%d"),
    }
    save_clients(clients)
    # Notifica admin para aprovação
    _notify_admin_new_client(chat_id, telegram_name, restaurants)

def _notify_admin_new_client(chat_id, telegram_name: str, restaurants: list):
    try:
        rest_names = ", ".join(r["name"] for r in restaurants) or "N/A"
        markup = telebot.types.InlineKeyboardMarkup()
        markup.row(
            telebot.types.InlineKeyboardButton("✅ Aprovar", callback_data=f"approve|{chat_id}"),
            telebot.types.InlineKeyboardButton("❌ Recusar", callback_data=f"deny|{chat_id}"),
        )
        bot.send_message(
            ADMIN_CHAT_ID,
            f"🆕 *Novo cadastro aguardando aprovação:*\n\n"
            f"👤 {telegram_name or 'Desconhecido'}  (chat\\_id: `{chat_id}`)\n"
            f"🏠 Restaurante(s): {rest_names}\n"
            f"📅 Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
            reply_markup=markup,
            parse_mode="Markdown",
        )
    except Exception as e:
        print(f"[ADMIN_NOTIFY] Erro ao notificar admin: {e}")

# ── Auth ──────────────────────────────────────────────────────────────────────
def check_auth(message) -> bool:
    # Limpa SEMPRE o contexto residual de qualquer requisição anterior nesta thread.
    # Essencial em thread pools onde threads são reutilizadas entre clientes.
    ai_tools.clear_client_context()

    # Admin eterno — nunca bloqueado, nunca precisa de aprovação
    if message.chat.id == ADMIN_CHAT_ID:
        ai_tools.set_client_context("", "", [])  # usa credenciais globais do env
        return True

    client = get_client(message.chat.id)
    if client:
        if not client.get("approved"):
            bot.send_message(
                message.chat.id,
                "⏳ Seu acesso está *pendente de aprovação* pelo administrador.\n"
                "Você será notificado assim que for liberado.",
                parse_mode="Markdown",
            )
            return False

        # Verifica expiração do plano
        expira = client.get("plano_expira")
        if expira:
            try:
                if datetime.strptime(expira, "%Y-%m-%d") < datetime.now():
                    # Suspende automaticamente e avisa o admin
                    clients_all = load_clients()
                    clients_all[str(message.chat.id)]["active"] = False
                    save_clients(clients_all)
                    bot.send_message(
                        message.chat.id,
                        "⏰ Seu plano *venceu*.\n"
                        "Entre em contato com o suporte para renovar o acesso.",
                        parse_mode="Markdown",
                    )
                    try:
                        name = client.get("telegram_name", str(message.chat.id))
                        plano_label = client.get("plano", "?").title()
                        bot.send_message(
                            ADMIN_CHAT_ID,
                            f"⏰ *Plano vencido:* {name}\n"
                            f"Plano: {plano_label} | Venceu em: {expira}\n"
                            f"chat\\_id: `{message.chat.id}`",
                            parse_mode="Markdown",
                        )
                    except: pass
                    return False
            except ValueError:
                pass  # data malformada — ignora

        if not client.get("active"):
            bot.send_message(
                message.chat.id,
                "🚫 Seu acesso foi *suspenso*.\n"
                "Entre em contato com o suporte para mais informações.",
                parse_mode="Markdown",
            )
            return False

        # Injeta credenciais + plano do cliente para esta thread
        ai_tools.set_client_context(
            client["net_user"],
            client["net_pass"],
            client["restaurants"],
            plano=client.get("plano", "premium"),
        )
        return True
    start_onboarding(message)
    return False

# ── Onboarding self-service ───────────────────────────────────────────────────
_onboarding: dict = {}  # {chat_id: {"step": str, "data": {}}}

PARTNER_LIST_URL = "https://aws.netcontroll.com.br/netadm/api/v1/parceiro"

def _try_login(email: str, senha: str):
    """Tenta autenticar no NetControll. Retorna (token, session) ou (None, None)."""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json", "App-Origin": "Portal"})
    try:
        r = session.post(LOGIN_URL, json={"Email": email, "Senha": senha}, timeout=12)
        if r.status_code == 200:
            token = r.json().get("data", {}).get("access_token")
            if token:
                session.headers.update({"Authorization": f"Bearer {token}"})
                return token, session
    except: pass
    return None, None

def _fetch_restaurants(session) -> list:
    """Tenta listar os parceiros/restaurantes disponíveis para a conta."""
    try:
        r = session.get(PARTNER_LIST_URL, timeout=10)
        if r.status_code == 200:
            data = r.json()
            # Normaliza lista de parceiros para {id, name}
            items = data if isinstance(data, list) else data.get("data", [])
            rests = []
            for item in items:
                rid  = item.get("id") or item.get("parceiro") or item.get("parceiroId")
                name = (item.get("nome") or item.get("nomeFantasia") or
                        item.get("name") or item.get("razaoSocial") or f"Restaurante {rid}")
                if rid:
                    rests.append({"id": int(rid), "name": str(name)})
            return rests
    except: pass
    return []

def start_onboarding(message):
    cid = message.chat.id
    _onboarding[cid] = {"step": "awaiting_email", "data": {}}
    bot.send_message(
        cid,
        "👋 *Bem-vindo ao XMenu Bot!*\n\n"
        "Para começar, preciso das suas credenciais do portal NetControll.\n\n"
        "📧 Digite seu *e-mail* de acesso ao portal:",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(message, _onboarding_email)

def _onboarding_email(message):
    cid = message.chat.id
    email = message.text.strip() if message.text else ""
    if not email or "@" not in email:
        bot.send_message(cid, "❌ E-mail inválido. Digite novamente:")
        bot.register_next_step_handler(message, _onboarding_email)
        return
    _onboarding.setdefault(cid, {"data": {}})["data"]["email"] = email
    _onboarding[cid]["step"] = "awaiting_password"
    bot.send_message(cid, "🔑 Agora digite sua *senha* do portal:", parse_mode="Markdown")
    bot.register_next_step_handler(message, _onboarding_password)

def _onboarding_password(message):
    cid = message.chat.id
    senha = message.text.strip() if message.text else ""
    if not senha:
        bot.send_message(cid, "❌ Senha não pode ser vazia. Digite novamente:")
        bot.register_next_step_handler(message, _onboarding_password)
        return

    email = _onboarding[cid]["data"]["email"]
    bot.send_message(cid, "⏳ Verificando credenciais no portal...")

    token, session = _try_login(email, senha)
    if not token:
        bot.send_message(
            cid,
            "❌ *Credenciais inválidas.* Verifique e-mail e senha e tente novamente.\n\n"
            "📧 Digite seu *e-mail*:",
            parse_mode="Markdown",
        )
        _onboarding[cid] = {"step": "awaiting_email", "data": {}}
        bot.register_next_step_handler(message, _onboarding_email)
        return

    _onboarding[cid]["data"]["senha"] = senha
    _onboarding[cid]["step"] = "confirming_restaurants"

    rests = _fetch_restaurants(session)
    _onboarding[cid]["data"]["restaurants"] = rests

    if rests:
        markup = telebot.types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
        for r in rests:
            markup.add(r["name"])
        markup.add("Todos")
        bot.send_message(
            cid,
            f"✅ *Login confirmado!* Encontrei {len(rests)} restaurante(s) na sua conta:\n\n" +
            "\n".join(f"• {r['name']}" for r in rests) +
            "\n\nQual(is) deseja monitorar? (selecione ou escreva *Todos*)",
            reply_markup=markup,
            parse_mode="Markdown",
        )
        bot.register_next_step_handler(message, _onboarding_confirm_restaurant)
    else:
        # Fallback: pede manualmente
        bot.send_message(
            cid,
            "✅ *Login confirmado!*\n\n"
            "Não consegui listar os restaurantes automaticamente.\n"
            "Digite o *nome do seu restaurante* como aparece no portal:",
            parse_mode="Markdown",
        )
        bot.register_next_step_handler(message, _onboarding_manual_restaurant)

def _get_telegram_name(message) -> str:
    u = message.from_user
    if u.username:
        return f"@{u.username}"
    return (u.first_name or "") + (" " + u.last_name if u.last_name else "")

def _onboarding_confirm_restaurant(message):
    cid = message.chat.id
    escolha = message.text.strip() if message.text else ""
    rests = _onboarding[cid]["data"].get("restaurants", [])
    email = _onboarding[cid]["data"]["email"]
    senha = _onboarding[cid]["data"]["senha"]

    if escolha.lower() == "todos" or not escolha:
        selected = rests
    else:
        selected = [r for r in rests if escolha.lower() in r["name"].lower()]
        if not selected:
            selected = rests  # fallback: usa todos

    register_client(cid, email, senha, selected, telegram_name=_get_telegram_name(message))
    _onboarding.pop(cid, None)
    bot.send_message(
        cid,
        f"✅ *Cadastro recebido!*\n\n"
        f"Restaurante(s): {', '.join(r['name'] for r in selected)}\n\n"
        "⏳ Seu acesso está sendo analisado pelo administrador.\n"
        "Você receberá uma mensagem assim que for aprovado.",
        parse_mode="Markdown",
    )

def _onboarding_manual_restaurant(message):
    cid = message.chat.id
    nome = message.text.strip() if message.text else "Meu Restaurante"
    email = _onboarding[cid]["data"]["email"]
    senha = _onboarding[cid]["data"]["senha"]
    register_client(cid, email, senha, [{"id": 0, "name": nome}], telegram_name=_get_telegram_name(message))
    _onboarding.pop(cid, None)
    bot.send_message(
        cid,
        f"✅ *Cadastro recebido!*\n\n"
        f"Restaurante: *{nome}*\n\n"
        "⏳ Seu acesso está sendo analisado pelo administrador.\n"
        "Você receberá uma mensagem assim que for aprovado.",
        parse_mode="Markdown",
    )

# Manager groups: map restaurant name keyword to a Telegram group chat ID
# To configure, send /register_group in the desired Telegram group
MANAGER_GROUPS_FILE = "manager_groups.json"

def load_manager_groups():
    if os.path.exists(MANAGER_GROUPS_FILE):
        try:
            with open(MANAGER_GROUPS_FILE, "r") as f:
                return json.load(f)
        except: return {}
    return {}

def save_manager_groups(groups):
    with open(MANAGER_GROUPS_FILE, "w") as f:
        json.dump(groups, f)

def get_main_keyboard():
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add("📊 Resumo Diário", "⚡️ Alertas Proativos")
    markup.add("💼 Área Financeira", "🛒 Estoque e CMV")
    markup.add("👥 RH e Operação", "🔄 Sincronizar Tudo")
    return markup

def get_financial_keyboard():
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add("📈 Gere um DRE", "💸 Contas a Pagar")
    markup.add("🏦 Raio-X Financeiro", "⚖️ Calcule o Break-even")
    markup.add("🧾 Conciliação de Notas", "🏭 Ranking de Fornecedores")
    markup.add("🔙 Voltar Principal")
    return markup

def get_stock_keyboard():
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add("🛒 Plano de Compras", "🔍 Auditoria de Rupturas")
    markup.add("📊 Análise de CMV", "📋 Fichas Técnicas")
    markup.add("💡 Markup Dinâmico", "📦 Giro de Estoque")
    markup.add("🐕 Cão de Guarda", "🔙 Voltar Principal")
    return markup

def get_hr_op_keyboard():
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add("👥 Escala de Equipe", "🏆 Produtividade Garçons")
    markup.add("💰 Quebra de Caixa", "❌ Cancelamentos")
    markup.add("⭐ Review de Clientes", "🚨 Fraude em Tempo Real")
    markup.add("🔙 Voltar Principal")
    return markup

# Mapa: texto do botão → action_key usado em build_question_for_action()
BUTTON_ACTION_MAP = {
    # Financeiro
    "📈 Gere um DRE":            "dre",
    "💸 Contas a Pagar":          "contas_pagar",
    "🏦 Raio-X Financeiro":       "raio_x",
    "⚖️ Calcule o Break-even":    "break_even",
    "🧾 Conciliação de Notas":    "conciliacao",
    "🏭 Ranking de Fornecedores": "fornecedores",
    # Estoque e CMV
    "🛒 Plano de Compras":        "plano_compras",
    "🔍 Auditoria de Rupturas":   "rupturas",
    "📊 Análise de CMV":          "cmv",
    "📋 Fichas Técnicas":         "fichas",
    "💡 Markup Dinâmico":         "markup_diario",
    "📦 Giro de Estoque":         "giro_insumo",
    # RH e Operação
    "👥 Escala de Equipe":        "escala",
    "🏆 Produtividade Garçons":   "garcons",
    "💰 Quebra de Caixa":         "caixa",
    "❌ Cancelamentos":           "cancelamentos",
    "⭐ Review de Clientes":      "review",
    "🚨 Fraude em Tempo Real":    "fraude",
}

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    if not check_auth(message): return
    text = (
        "🚀 *XMenu Portal LIVE Bot - Diogo Albuquerque*\n\n"
        "Comandos disponíveis:\n"
        "🔄 /sync - Sincroniza vendas das 3 casas agora\n"
        "💰 /resumo - Resumo financeiro consolidado de ontem\n"
        "⚡️ /proativo - Força a geração de alertas proativos agora\n"
        "👥 /register\_group nauan - Vincula este grupo ao restaurante\n\n"
        "Ou simplesmente me pergunte qualquer coisa em texto ou áudio!\n"
        "Ex: 'Faça a engenharia de cardápio do Nauan'\n"
        "Ex: 'Se eu aumentar a Heineken em R$2, quanto a mais faturaria?'\n"
        "Ex: 'Concilie as notas fiscais do Milagres'"
    )
    bot.reply_to(message, text, reply_markup=get_main_keyboard(), parse_mode='Markdown')

@bot.message_handler(commands=['register_group'])
def cmd_register_group(message):
    if not check_auth(message): return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "⚠️ Use: /register_group nauan (ou milagres, ahau)")
            return
        
        rest_keyword = parts[1].lower().strip()
        valid = ['nauan', 'milagres', 'ahau']
        if rest_keyword not in valid:
            bot.reply_to(message, f"⚠️ Restaurante inválido. Use: {', '.join(valid)}")
            return
        
        groups = load_manager_groups()
        groups[rest_keyword] = message.chat.id
        save_manager_groups(groups)
        
        bot.reply_to(message, f"✅ Este grupo/chat foi vinculado ao restaurante *{rest_keyword.title()}*!\n\nAlertas proativos específicos deste restaurante serão enviados aqui.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"❌ Erro: {e}")


@bot.message_handler(commands=['sync'])
def cmd_sync(message):
    if not check_auth(message): return
    msg = bot.reply_to(message, "⏳ *Iniciando sincronização completa...*", parse_mode='Markdown')
    success_count = 0
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    for rest in RESTAURANTS:
        bot.edit_message_text(f"📥 Baixando dados de: *{rest['name']}*...", message.chat.id, msg.message_id, parse_mode='Markdown')
        session = get_session(rest['id'])
        if session:
            # Endpoint based on discovery
            s1 = download_sales_json(session, target_date, rest['id'], rest['sales_file'])
            s2 = download_stock_json(session, rest['id'], rest['stock_file'])
            s3 = download_expenses_json(session, target_date, rest['id'], rest['expense_file'])
            if s1 or s2 or s3:
                success_count += 1
            else:
                bot.send_message(message.chat.id, f"⚠️ Falha no download de {rest['name']}. API indisponível (Erro 404).")
    
    if success_count > 0:
        bot.edit_message_text(f"✅ *Sincronização Concluída!* ({success_count}/{len(RESTAURANTS)} casas atualizadas)", message.chat.id, msg.message_id, parse_mode='Markdown')
    else:
        bot.edit_message_text("❌ *Falha Crítica:* Não consegui sincronizar nenhuma casa via API.", message.chat.id, msg.message_id, parse_mode='Markdown')

@bot.message_handler(commands=['resumo'])
def cmd_resumo(message):
    if not check_auth(message): return
    bot.send_chat_action(message.chat.id, 'typing')
    try:
        report = ai_tools.get_daily_briefing()
        send_long_msg(message, report)
    except Exception as e:
        bot.reply_to(message, f"❌ Erro ao gerar resumo: {e}")

def send_long_msg(message, text):
    if not text:
        bot.reply_to(message, "⚠️ Sem resposta gerada. Tente novamente.")
        return
    try:
        if len(text) <= 4000:
            bot.reply_to(message, text, parse_mode='Markdown')
        else:
            chunks = []
            while len(text) > 0:
                if len(text) <= 4000:
                    chunks.append(text)
                    break
                split_idx = text.rfind('\n', 0, 4000)
                if split_idx == -1: split_idx = 4000
                chunks.append(text[:split_idx])
                text = text[split_idx:].strip()
                
            for i, chunk in enumerate(chunks):
                if i == 0:
                    bot.reply_to(message, chunk, parse_mode='Markdown')
                else:
                    bot.send_message(message.chat.id, chunk, parse_mode='Markdown')
    except Exception as e:
        # Fallback without markdown if parsing fails (e.g., unmatched asterisks across chunks)
        try:
            bot.send_message(message.chat.id, text[:4000])
        except:
            pass

@bot.message_handler(content_types=['voice', 'audio'])
def handle_voice(message):
    if not check_auth(message): return
    try:
        bot.send_chat_action(message.chat.id, 'record_audio')
        
        file_id = message.voice.file_id if message.voice else message.audio.file_id
        file_info = bot.get_file(file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        file_extension = file_info.file_path.split('.')[-1]
        temp_file_name = f"temp_voice_{message.chat.id}.{file_extension}"
        
        with open(temp_file_name, 'wb') as new_file:
            new_file.write(downloaded_file)
            
        with open(temp_file_name, 'rb') as audio_file:
            transcript = ai_manager.client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                language="pt"
            )
            
        os.remove(temp_file_name)
        
        transcribed_text = transcript.text
        bot.reply_to(message, f"🎙️ *Transcrição:* _{transcribed_text}_", parse_mode='Markdown')
        
        # Passar para o Gestor de IA
        bot.send_chat_action(message.chat.id, 'typing')
        current = _resolve_restaurant(message.chat.id, transcribed_text)
        resposta_ia = ai_manager.process_ceo_question(
            transcribed_text,
            current_restaurant=current,
            chat_id=message.chat.id
        )
        send_long_msg(message, resposta_ia)
        
    except Exception as e:
        bot.reply_to(message, f"❌ Erro ao processar o áudio: {str(e)}")

@bot.message_handler(commands=['proativo'])
def force_proactive(message):
    if not check_auth(message): return
    bot.reply_to(message, "⚡️ Gerando análises proativas agora mesmo...")
    try:
        result = ai_tools.get_proactive_alerts()
        if isinstance(result, dict):
            if result.get('ceo'):
                bot.send_message(message.chat.id, result['ceo'], parse_mode='Markdown')
            else:
                bot.reply_to(message, "✅ Tudo em dia! Nenhuma anomalia crítica encontrada agora.")
        else:
            bot.reply_to(message, "❌ Erro ao gerar alertas.")
    except Exception as e:
        bot.reply_to(message, f"❌ Erro: {e}")

def _set_auth_context_for(chat_id: int) -> bool:
    """
    Injeta o _client_ctx correto para um chat_id (usado em callbacks que não passam
    por check_auth). Retorna False se o usuário não tiver acesso.
    """
    ai_tools.clear_client_context()
    if chat_id == ADMIN_CHAT_ID:
        ai_tools.set_client_context("", "", [])
        return True
    client = get_client(chat_id)
    if client and client.get("approved") and client.get("active"):
        ai_tools.set_client_context(
            client["net_user"],
            client["net_pass"],
            client["restaurants"],
            plano=client.get("plano", "premium"),
        )
        return True
    return False

@bot.callback_query_handler(func=lambda call: call.data.startswith("rest|"))
def handle_restaurant_choice(call):
    """Recebe a seleção de restaurante via InlineKeyboard e executa a ação pendente."""
    chat_id   = call.message.chat.id
    rest_code = call.data.split("|", 1)[1]          # "Nauan Beach Club" ou "__ALL__"
    action    = pending_actions.pop(chat_id, None)

    # Feedback visual: remove os botões inline
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
    except: pass

    if not action:
        bot.answer_callback_query(call.id, "Ação expirada. Use os botões novamente.")
        return

    # Injeta contexto de segurança — callbacks não passam por check_auth
    if not _set_auth_context_for(chat_id):
        bot.answer_callback_query(call.id, "Acesso não autorizado.")
        return

    bot.answer_callback_query(call.id)

    # Restaurantes que este usuário pode consultar (nunca o global RESTAURANTS)
    allowed = _get_allowed_restaurants(chat_id)

    if rest_code == "__ALL__":
        # Executa APENAS para as casas às quais este usuário tem acesso
        bot.send_message(chat_id, f"⏳ Analisando *Todas as Casas*...", parse_mode="Markdown")
        for rest in allowed:
            question = build_question_for_action(action, rest["name"])
            bot.send_chat_action(chat_id, "typing")
            bot.send_message(chat_id, f"🏠 *{rest['name']}*", parse_mode="Markdown")
            try:
                resposta = ai_manager.process_ceo_question(question, current_restaurant=rest["name"], chat_id=chat_id)
                _send_long(chat_id, resposta)
            except Exception as e:
                bot.send_message(chat_id, f"❌ Erro em {rest['name']}: {e}")
    else:
        # Valida que rest_code está na lista de restaurantes permitidos
        allowed_names = {r["name"] for r in allowed}
        if rest_code not in allowed_names:
            print(f"[SECURITY] chat_id={chat_id} tentou acessar '{rest_code}' (não permitido). Permitidos: {allowed_names}")
            bot.send_message(chat_id, "⚠️ Acesso negado a este restaurante.")
            return

        question = build_question_for_action(action, rest_code)
        bot.send_message(chat_id, f"⏳ Analisando *{rest_code}*...", parse_mode="Markdown")
        bot.send_chat_action(chat_id, "typing")
        try:
            resposta = ai_manager.process_ceo_question(question, current_restaurant=rest_code, chat_id=chat_id)
            _send_long(chat_id, resposta)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Erro: {e}")


def _send_long(chat_id: int, text: str):
    """Envia texto longo em chunks de 4000 chars para um chat_id."""
    if not text:
        bot.send_message(chat_id, "⚠️ Sem resposta gerada. Tente novamente.")
        return
    try:
        while text:
            if len(text) <= 4000:
                bot.send_message(chat_id, text, parse_mode="Markdown")
                break
            idx = text.rfind("\n", 0, 4000)
            if idx == -1: idx = 4000
            bot.send_message(chat_id, text[:idx], parse_mode="Markdown")
            text = text[idx:].strip()
    except Exception:
        try:
            bot.send_message(chat_id, text[:4000])
        except: pass


# ── Callbacks de aprovação de novos clientes ──────────────────────────────────
@bot.callback_query_handler(func=lambda call: call.data.startswith(("approve|", "deny|")))
def handle_admin_decision(call):
    if call.from_user.id != ADMIN_CHAT_ID:
        bot.answer_callback_query(call.id, "⛔ Sem permissão.")
        return

    action, target_cid = call.data.split("|", 1)
    clients = load_clients()
    client = clients.get(target_cid)
    if not client:
        bot.answer_callback_query(call.id, "Cliente não encontrado.")
        return

    name = client.get("telegram_name", target_cid)
    if action == "approve":
        # Pede ao admin qual plano atribuir antes de liberar
        markup = telebot.types.InlineKeyboardMarkup()
        markup.row(
            telebot.types.InlineKeyboardButton("⭐ Básico",   callback_data=f"setplan|{target_cid}|basico"),
            telebot.types.InlineKeyboardButton("🚀 Pro",      callback_data=f"setplan|{target_cid}|pro"),
            telebot.types.InlineKeyboardButton("💎 Premium",  callback_data=f"setplan|{target_cid}|premium"),
        )
        bot.edit_message_text(
            f"✅ Aprovar *{name}* — escolha o plano:",
            call.message.chat.id, call.message.message_id,
            reply_markup=markup, parse_mode="Markdown",
        )
    else:
        client["approved"] = False
        client["active"] = False
        save_clients(clients)
        try:
            bot.send_message(
                int(target_cid),
                "❌ Seu cadastro foi *recusado* pelo administrador.\n"
                "Entre em contato pelo suporte para mais informações.",
                parse_mode="Markdown",
            )
        except: pass
        bot.edit_message_text(f"❌ Recusado: {name}", call.message.chat.id, call.message.message_id)

    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("setplan|"))
def handle_set_plan(call):
    if call.from_user.id != ADMIN_CHAT_ID:
        bot.answer_callback_query(call.id, "⛔ Sem permissão.")
        return

    _, target_cid, plano = call.data.split("|", 2)
    clients = load_clients()
    client = clients.get(target_cid)
    if not client:
        bot.answer_callback_query(call.id, "Cliente não encontrado.")
        return

    from datetime import timedelta
    expira = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
    client["active"] = True
    client["approved"] = True
    client["plano"] = plano
    client["plano_expira"] = expira
    save_clients(clients)

    plano_labels = {"basico": "⭐ Básico", "pro": "🚀 Pro", "premium": "💎 Premium"}
    label = plano_labels.get(plano, plano.title())
    name = client.get("telegram_name", target_cid)

    # Tutorial personalizado por plano
    TUTORIAIS = {
        "basico": (
            "🎉 *Acesso liberado! Bem-vindo ao XMenu Bot.*\n\n"
            f"Seu plano: *{label}* — válido até {expira}\n\n"
            "📋 *O que você pode fazer:*\n"
            "• _'Quanto faturamos ontem?'_\n"
            "• _'Quais foram os 5 itens mais vendidos esta semana?'_\n"
            "• _'Compare o faturamento das casas em março'_\n"
            "• _'Como estão as avaliações do Google?'_\n\n"
            "💡 Pode digitar ou enviar áudio — eu entendo os dois!"
        ),
        "pro": (
            "🎉 *Acesso liberado! Bem-vindo ao XMenu Bot.*\n\n"
            f"Seu plano: *{label}* — válido até {expira}\n\n"
            "📋 *O que você pode fazer:*\n"
            "• _'Qual o CMV do mês?'_\n"
            "• _'Gere o DRE de fevereiro'_\n"
            "• _'Audite os NCMs dos produtos'_\n"
            "• _'Quais fornecedores aumentaram preço?'_\n"
            "• _'Qual o break-even mensal?'_\n"
            "• _'Preciso comprar o quê esta semana?'_\n\n"
            "💡 Pode digitar ou enviar áudio — eu entendo os dois!"
        ),
        "premium": (
            "🎉 *Acesso liberado! Bem-vindo ao XMenu Bot.*\n\n"
            f"Seu plano: *{label}* — válido até {expira}\n\n"
            "📋 *Você tem acesso completo — exemplos:*\n"
            "• _'Tem alguma fraude no caixa hoje?'_\n"
            "• _'Faça a engenharia de cardápio'_\n"
            "• _'Qual a previsão de RH para o fim de semana?'_\n"
            "• _'Detecte desperdício no estoque'_\n"
            "• _'Sugira reajuste de preços para proteger a margem'_\n"
            "• _'Auditoria cruzada completa'_\n\n"
            "💡 Pode digitar ou enviar áudio — eu entendo os dois!"
        ),
    }

    try:
        bot.send_message(
            int(target_cid),
            TUTORIAIS.get(plano, TUTORIAIS["basico"]),
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown",
        )
    except: pass

    bot.edit_message_text(
        f"✅ {name} aprovado — plano {label} até {expira}",
        call.message.chat.id, call.message.message_id,
    )
    bot.answer_callback_query(call.id)


# ── Comandos admin ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=['clientes'])
def cmd_clientes(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    clients = load_clients()
    if not clients:
        bot.reply_to(message, "Nenhum cliente cadastrado.")
        return

    markup = telebot.types.InlineKeyboardMarkup()
    linhas = ["👥 *Clientes cadastrados:*\n"]
    for i, (cid, c) in enumerate(clients.items(), 1):
        name = c.get("telegram_name") or cid
        rests = ", ".join(r["name"] for r in c.get("restaurants", [])) or "N/A"
        if not c.get("approved"):
            status = "⏳ Pendente"
            btn = telebot.types.InlineKeyboardButton(f"✅ Aprovar {name}", callback_data=f"approve|{cid}")
            markup.add(btn)
        elif c.get("active"):
            status = "✅ Ativo"
            btn = telebot.types.InlineKeyboardButton(f"🚫 Bloquear {name}", callback_data=f"block|{cid}")
            markup.add(btn)
        else:
            status = "🚫 Bloqueado"
            btn = telebot.types.InlineKeyboardButton(f"✅ Liberar {name}", callback_data=f"unblock|{cid}")
            markup.add(btn)
        linhas.append(f"{i}. {name} — {status}\n   🏠 {rests}\n   🆔 `{cid}`")

    bot.reply_to(message, "\n".join(linhas), reply_markup=markup, parse_mode="Markdown")


@bot.callback_query_handler(func=lambda call: call.data.startswith(("block|", "unblock|")))
def handle_admin_toggle(call):
    if call.from_user.id != ADMIN_CHAT_ID:
        bot.answer_callback_query(call.id, "⛔ Sem permissão.")
        return

    action, target_cid = call.data.split("|", 1)
    clients = load_clients()
    client = clients.get(target_cid)
    if not client:
        bot.answer_callback_query(call.id, "Cliente não encontrado.")
        return

    name = client.get("telegram_name", target_cid)
    if action == "block":
        client["active"] = False
        save_clients(clients)
        try:
            bot.send_message(int(target_cid),
                "🚫 Seu acesso foi *suspenso* pelo administrador.", parse_mode="Markdown")
        except: pass
        bot.answer_callback_query(call.id, f"🚫 {name} bloqueado.")
    else:
        client["active"] = True
        client["approved"] = True
        save_clients(clients)
        try:
            bot.send_message(int(target_cid),
                "✅ Seu acesso foi *restaurado!* Pode usar o bot normalmente.",
                reply_markup=get_main_keyboard(), parse_mode="Markdown")
        except: pass
        bot.answer_callback_query(call.id, f"✅ {name} liberado.")

    # Atualiza a lista de clientes
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except: pass
    cmd_clientes(call.message)


@bot.message_handler(commands=['bloquear'])
def cmd_bloquear(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "⚠️ Use: `/bloquear <chat_id>`", parse_mode="Markdown")
        return
    target_cid = parts[1].strip()
    clients = load_clients()
    if target_cid not in clients:
        bot.reply_to(message, f"❌ Cliente `{target_cid}` não encontrado.", parse_mode="Markdown")
        return
    clients[target_cid]["active"] = False
    save_clients(clients)
    name = clients[target_cid].get("telegram_name", target_cid)
    try:
        bot.send_message(int(target_cid),
            "🚫 Seu acesso foi *suspenso* pelo administrador.", parse_mode="Markdown")
    except: pass
    bot.reply_to(message, f"🚫 Acesso de *{name}* suspenso.", parse_mode="Markdown")


@bot.message_handler(commands=['liberar'])
def cmd_liberar(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "⚠️ Use: `/liberar <chat_id>`", parse_mode="Markdown")
        return
    target_cid = parts[1].strip()
    clients = load_clients()
    if target_cid not in clients:
        bot.reply_to(message, f"❌ Cliente `{target_cid}` não encontrado.", parse_mode="Markdown")
        return
    clients[target_cid]["active"] = True
    clients[target_cid]["approved"] = True
    save_clients(clients)
    name = clients[target_cid].get("telegram_name", target_cid)
    try:
        bot.send_message(int(target_cid),
            "✅ Seu acesso foi *restaurado!* Pode usar o bot normalmente.",
            reply_markup=get_main_keyboard(), parse_mode="Markdown")
    except: pass
    bot.reply_to(message, f"✅ Acesso de *{name}* restaurado.", parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# PAINEL DE CONTROLE ADMIN UNIFICADO
# ─────────────────────────────────────────────────────────────────────────────

PLANO_LABELS = {"basico": "⭐ Básico", "pro": "🚀 Pro", "premium": "💎 Premium"}

def _admin_client_status(c: dict) -> str:
    if not c.get("approved"):
        return "⏳ Pendente"
    if not c.get("active"):
        return "🚫 Bloqueado"
    return "✅ Ativo"

def _dias_restantes(expira_str) -> str:
    if not expira_str:
        return "sem data"
    try:
        dias = (datetime.strptime(expira_str, "%Y-%m-%d") - datetime.now()).days
        if dias < 0:
            return "VENCIDO"
        return f"{dias}d"
    except ValueError:
        return "?"

def _send_admin_dashboard(chat_id, message_id=None):
    clients = load_clients()
    ativos = sum(1 for c in clients.values() if c.get("active") and c.get("approved"))
    pendentes = sum(1 for c in clients.values() if not c.get("approved"))
    hoje = datetime.now()
    vencendo = sum(
        1 for c in clients.values()
        if c.get("active") and c.get("approved") and c.get("plano_expira")
        and 0 <= (datetime.strptime(c["plano_expira"], "%Y-%m-%d") - hoje).days <= 7
    )

    linhas = ["🎛️ *PAINEL DE CONTROLE*\n"]
    linhas.append(f"👥 {ativos} cliente(s) ativo(s)")
    if pendentes:
        linhas.append(f"⏳ {pendentes} pendente(s) de aprovação")
    if vencendo:
        linhas.append(f"⚠️ {vencendo} vencendo em 7 dias")
    texto = "\n".join(linhas)

    markup = telebot.types.InlineKeyboardMarkup()
    row1 = [telebot.types.InlineKeyboardButton("👥 Ver Clientes", callback_data="adm_list")]
    if pendentes:
        row1.append(telebot.types.InlineKeyboardButton(f"⏳ Pendentes ({pendentes})", callback_data="adm_pending"))
    markup.row(*row1)

    if message_id:
        try:
            bot.edit_message_text(texto, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
        except: pass
    else:
        bot.send_message(chat_id, texto, reply_markup=markup, parse_mode="Markdown")


def _send_admin_list(chat_id, message_id):
    clients = load_clients()
    if not clients:
        bot.edit_message_text("Nenhum cliente cadastrado.", chat_id, message_id)
        return

    linhas = ["👥 *CLIENTES CADASTRADOS*\n"]
    markup = telebot.types.InlineKeyboardMarkup()
    for cid, c in clients.items():
        name = c.get("telegram_name") or cid
        status = _admin_client_status(c)
        plano = PLANO_LABELS.get(c.get("plano", ""), "")
        expira = c.get("plano_expira", "")
        dias = _dias_restantes(expira) if c.get("approved") and c.get("active") else ""
        plano_info = f" — {plano} {dias}".rstrip() if plano else ""
        linhas.append(f"• {name} {status}{plano_info}")
        markup.add(telebot.types.InlineKeyboardButton(f"👤 {name}", callback_data=f"adm_client|{cid}"))

    markup.add(telebot.types.InlineKeyboardButton("⬅ Menu", callback_data="adm_menu"))
    bot.edit_message_text("\n".join(linhas), chat_id, message_id, reply_markup=markup, parse_mode="Markdown")


def _send_admin_client(chat_id, message_id, target_cid):
    clients = load_clients()
    c = clients.get(target_cid)
    if not c:
        bot.answer_callback_query(message_id, "Cliente não encontrado.")
        return

    name = c.get("telegram_name") or target_cid
    rests = ", ".join(r["name"] for r in c.get("restaurants", [])) or "N/A"
    plano = PLANO_LABELS.get(c.get("plano", ""), "N/A")
    expira = c.get("plano_expira") or "—"
    dias = _dias_restantes(c.get("plano_expira"))
    cadastro = c.get("registered_at", "—")
    status = _admin_client_status(c)

    texto = (
        f"👤 *{name}*\n"
        f"🆔 `{target_cid}`\n"
        f"🏠 {rests}\n"
        f"🎯 Plano: {plano}\n"
        f"📅 Vence: {expira} ({dias})\n"
        f"📆 Cadastro: {cadastro}\n"
        f"Status: {status}"
    )

    markup = telebot.types.InlineKeyboardMarkup()
    if c.get("approved") and c.get("active"):
        markup.row(
            telebot.types.InlineKeyboardButton("🔄 Mudar Plano", callback_data=f"adm_plan|{target_cid}"),
            telebot.types.InlineKeyboardButton("+30 dias", callback_data=f"adm_renew|{target_cid}"),
        )
        markup.row(
            telebot.types.InlineKeyboardButton("🚫 Bloquear", callback_data=f"adm_block|{target_cid}"),
            telebot.types.InlineKeyboardButton("⬅ Lista", callback_data="adm_list"),
        )
    elif not c.get("approved"):
        markup.row(
            telebot.types.InlineKeyboardButton("✅ Aprovar", callback_data=f"adm_approve|{target_cid}"),
            telebot.types.InlineKeyboardButton("❌ Recusar", callback_data=f"adm_deny|{target_cid}"),
        )
        markup.add(telebot.types.InlineKeyboardButton("⬅ Lista", callback_data="adm_list"))
    else:  # bloqueado
        markup.row(
            telebot.types.InlineKeyboardButton("✅ Liberar", callback_data=f"adm_unblock|{target_cid}"),
            telebot.types.InlineKeyboardButton("⬅ Lista", callback_data="adm_list"),
        )

    bot.edit_message_text(texto, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")


def _send_admin_pending(chat_id, message_id):
    clients = load_clients()
    pendentes = {cid: c for cid, c in clients.items() if not c.get("approved")}
    if not pendentes:
        bot.edit_message_text("Nenhum cliente pendente.", chat_id, message_id,
                              reply_markup=telebot.types.InlineKeyboardMarkup().add(
                                  telebot.types.InlineKeyboardButton("⬅ Menu", callback_data="adm_menu")))
        return

    linhas = ["⏳ *AGUARDANDO APROVAÇÃO*\n"]
    markup = telebot.types.InlineKeyboardMarkup()
    for cid, c in pendentes.items():
        name = c.get("telegram_name") or cid
        rests = ", ".join(r["name"] for r in c.get("restaurants", [])) or "N/A"
        cadastro = c.get("registered_at", "—")
        linhas.append(f"👤 {name} — {rests}\n   📆 {cadastro}")
        markup.row(
            telebot.types.InlineKeyboardButton(f"✅ Aprovar {name}", callback_data=f"adm_approve|{cid}"),
            telebot.types.InlineKeyboardButton(f"❌ Recusar {name}", callback_data=f"adm_deny|{cid}"),
        )
    markup.add(telebot.types.InlineKeyboardButton("⬅ Menu", callback_data="adm_menu"))
    bot.edit_message_text("\n".join(linhas), chat_id, message_id, reply_markup=markup, parse_mode="Markdown")


def _send_admin_plan_picker(chat_id, message_id, target_cid):
    clients = load_clients()
    c = clients.get(target_cid)
    name = c.get("telegram_name", target_cid) if c else target_cid
    markup = telebot.types.InlineKeyboardMarkup()
    markup.row(
        telebot.types.InlineKeyboardButton("⭐ Básico",  callback_data=f"adm_setplan|{target_cid}|basico"),
        telebot.types.InlineKeyboardButton("🚀 Pro",     callback_data=f"adm_setplan|{target_cid}|pro"),
        telebot.types.InlineKeyboardButton("💎 Premium", callback_data=f"adm_setplan|{target_cid}|premium"),
    )
    markup.add(telebot.types.InlineKeyboardButton("⬅ Cancelar", callback_data=f"adm_client|{target_cid}"))
    bot.edit_message_text(
        f"🔄 Escolha o novo plano para *{name}*:",
        chat_id, message_id, reply_markup=markup, parse_mode="Markdown"
    )


@bot.message_handler(commands=['admin'])
def cmd_admin(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    _send_admin_dashboard(message.chat.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_"))
def handle_admin_panel(call):
    if call.from_user.id != ADMIN_CHAT_ID:
        bot.answer_callback_query(call.id, "⛔ Sem permissão.")
        return

    data = call.data
    mid = call.message.message_id
    cid_admin = call.message.chat.id

    if data == "adm_menu":
        _send_admin_dashboard(cid_admin, mid)

    elif data == "adm_list":
        _send_admin_list(cid_admin, mid)

    elif data == "adm_pending":
        _send_admin_pending(cid_admin, mid)

    elif data.startswith("adm_client|"):
        target_cid = data.split("|", 1)[1]
        _send_admin_client(cid_admin, mid, target_cid)

    elif data.startswith("adm_plan|"):
        target_cid = data.split("|", 1)[1]
        _send_admin_plan_picker(cid_admin, mid, target_cid)

    elif data.startswith("adm_setplan|"):
        _, target_cid, plano = data.split("|", 2)
        clients = load_clients()
        c = clients.get(target_cid)
        if not c:
            bot.answer_callback_query(call.id, "Cliente não encontrado.")
            return
        from datetime import timedelta
        expira = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        c["plano"] = plano
        c["plano_expira"] = expira
        c["active"] = True
        c["approved"] = True
        save_clients(clients)
        label = PLANO_LABELS.get(plano, plano)
        name = c.get("telegram_name", target_cid)
        # Envia tutorial se estiver sendo aprovado pela primeira vez via painel
        TUTORIAIS = {
            "basico": (
                "🎉 *Acesso liberado! Bem-vindo ao XMenu Bot.*\n\n"
                f"Seu plano: *{label}* — válido até {expira}\n\n"
                "📋 *O que você pode fazer:*\n"
                "• _'Quanto faturamos ontem?'_\n"
                "• _'Quais foram os 5 itens mais vendidos esta semana?'_\n"
                "• _'Como estão as avaliações do Google?'_\n\n"
                "💡 Pode digitar ou enviar áudio — eu entendo os dois!"
            ),
            "pro": (
                "🎉 *Acesso liberado! Bem-vindo ao XMenu Bot.*\n\n"
                f"Seu plano: *{label}* — válido até {expira}\n\n"
                "📋 *O que você pode fazer:*\n"
                "• _'Qual o CMV do mês?'_\n"
                "• _'Gere o DRE de fevereiro'_\n"
                "• _'Quais fornecedores aumentaram preço?'_\n"
                "• _'Qual o break-even mensal?'_\n\n"
                "💡 Pode digitar ou enviar áudio — eu entendo os dois!"
            ),
            "premium": (
                "🎉 *Acesso liberado! Bem-vindo ao XMenu Bot.*\n\n"
                f"Seu plano: *{label}* — válido até {expira}\n\n"
                "📋 *Acesso completo — exemplos:*\n"
                "• _'Tem alguma fraude no caixa hoje?'_\n"
                "• _'Sugira reajuste de preços para proteger a margem'_\n"
                "• _'Auditoria cruzada completa'_\n\n"
                "💡 Pode digitar ou enviar áudio — eu entendo os dois!"
            ),
        }
        try:
            bot.send_message(int(target_cid), TUTORIAIS.get(plano, TUTORIAIS["basico"]),
                             reply_markup=get_main_keyboard(), parse_mode="Markdown")
        except: pass
        bot.answer_callback_query(call.id, f"✅ Plano {label} aplicado a {name}")
        _send_admin_client(cid_admin, mid, target_cid)

    elif data.startswith("adm_renew|"):
        target_cid = data.split("|", 1)[1]
        clients = load_clients()
        c = clients.get(target_cid)
        if not c:
            bot.answer_callback_query(call.id, "Cliente não encontrado.")
            return
        from datetime import timedelta
        atual = c.get("plano_expira")
        base = datetime.now()
        if atual:
            try:
                base = max(base, datetime.strptime(atual, "%Y-%m-%d"))
            except ValueError:
                pass
        nova_expira = (base + timedelta(days=30)).strftime("%Y-%m-%d")
        c["plano_expira"] = nova_expira
        save_clients(clients)
        name = c.get("telegram_name", target_cid)
        bot.answer_callback_query(call.id, f"✅ {name} renovado até {nova_expira}")
        _send_admin_client(cid_admin, mid, target_cid)

    elif data.startswith("adm_block|"):
        target_cid = data.split("|", 1)[1]
        clients = load_clients()
        c = clients.get(target_cid)
        if not c:
            bot.answer_callback_query(call.id, "Cliente não encontrado.")
            return
        c["active"] = False
        save_clients(clients)
        try:
            bot.send_message(int(target_cid),
                "🚫 Seu acesso foi *suspenso* pelo administrador.", parse_mode="Markdown")
        except: pass
        name = c.get("telegram_name", target_cid)
        bot.answer_callback_query(call.id, f"🚫 {name} bloqueado.")
        _send_admin_client(cid_admin, mid, target_cid)

    elif data.startswith("adm_unblock|"):
        target_cid = data.split("|", 1)[1]
        clients = load_clients()
        c = clients.get(target_cid)
        if not c:
            bot.answer_callback_query(call.id, "Cliente não encontrado.")
            return
        c["active"] = True
        c["approved"] = True
        save_clients(clients)
        try:
            bot.send_message(int(target_cid),
                "✅ Seu acesso foi *restaurado!* Pode usar o bot normalmente.",
                reply_markup=get_main_keyboard(), parse_mode="Markdown")
        except: pass
        name = c.get("telegram_name", target_cid)
        bot.answer_callback_query(call.id, f"✅ {name} liberado.")
        _send_admin_client(cid_admin, mid, target_cid)

    elif data.startswith("adm_approve|"):
        target_cid = data.split("|", 1)[1]
        # Mostra seletor de plano (aprovação via painel)
        _send_admin_plan_picker(cid_admin, mid, target_cid)

    elif data.startswith("adm_deny|"):
        target_cid = data.split("|", 1)[1]
        clients = load_clients()
        c = clients.get(target_cid)
        if not c:
            bot.answer_callback_query(call.id, "Cliente não encontrado.")
            return
        c["approved"] = False
        c["active"] = False
        save_clients(clients)
        name = c.get("telegram_name", target_cid)
        try:
            bot.send_message(int(target_cid),
                "❌ Seu cadastro foi *recusado* pelo administrador.\n"
                "Entre em contato pelo suporte para mais informações.",
                parse_mode="Markdown")
        except: pass
        bot.answer_callback_query(call.id, f"❌ {name} recusado.")
        _send_admin_pending(cid_admin, mid)

    bot.answer_callback_query(call.id)


@bot.message_handler(func=lambda message: True)
def handle_msg(message):
    if not check_auth(message): return
    print(f"[{datetime.now()}] Recebi mensagem: {message.text}")

    text  = message.text or ""
    query = text.lower()

    # ── Navegação de Menus ────────────────────────────────────────────────────
    if text == "🔙 Voltar Principal":
        bot.reply_to(message, "Retornando ao menu principal...", reply_markup=get_main_keyboard())
        return
    elif text == "💼 Área Financeira":
        bot.reply_to(message, "💼 O que deseja analisar no Financeiro?", reply_markup=get_financial_keyboard())
        return
    elif text == "🛒 Estoque e CMV":
        bot.reply_to(message, "🛒 O que deseja analisar no Estoque e CMV?", reply_markup=get_stock_keyboard())
        return
    elif text == "👥 RH e Operação":
        bot.reply_to(message, "👥 O que deseja analisar em RH e Operação?", reply_markup=get_hr_op_keyboard())
        return

    # ── Botões que não precisam de seleção de casa ────────────────────────────
    elif text == "📊 Resumo Diário":
        cmd_resumo(message)
        return
    elif text == "⚡️ Alertas Proativos":
        force_proactive(message)
        return
    elif text == "🔄 Sincronizar Tudo":
        cmd_sync(message)
        return
    elif text == "🐕 Cão de Guarda":
        bot.send_chat_action(message.chat.id, 'typing')
        bot.reply_to(message, "🐕 *Cão de Guarda ativo!* Verificando preços das últimas 2 semanas...", parse_mode='Markdown')
        try:
            report = ai_tools.get_watchdog_consolidated()
            send_long_msg(message, report)
        except Exception as e:
            bot.reply_to(message, f"❌ Erro no Cão de Guarda: {e}")
        return

    # ── Botões de submenu → perguntar a casa primeiro ─────────────────────────
    elif text in BUTTON_ACTION_MAP:
        action_key = BUTTON_ACTION_MAP[text]
        ask_restaurant(message.chat.id, action_key)
        return

    # ── Comandos legados ──────────────────────────────────────────────────────
    if query == "/resumo":
        cmd_resumo(message)
    elif query == "/sync":
        cmd_sync(message)
    else:
        # Texto livre ou áudio transcrito → manda direto para IA
        bot.send_chat_action(message.chat.id, 'typing')
        try:
            current = _resolve_restaurant(message.chat.id, message.text or "")
            resposta_ia = ai_manager.process_ceo_question(
                message.text,
                current_restaurant=current,
                chat_id=message.chat.id
            )
            send_long_msg(message, resposta_ia)
        except Exception as e:
            bot.reply_to(message, f"❌ Erro na IA: {str(e)}")

def warm_up_cache():
    """Silently populate the cache engine with heavy reports for all restaurants."""
    print(f"[{datetime.now()}] Iniciando Warm-Up do Cache (Pesados/30d)...")
    from ai_tools import fetch_sales_data, fetch_expenses_data, fetch_expenses_supplier_data, fetch_cmv_data, fetch_inbound_data
    
    today = datetime.now()
    d30 = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    d1 = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    hoje_str = today.strftime('%Y-%m-%d')
    d_mtd = today.replace(day=1).strftime('%Y-%m-%d')
    
    for rest in RESTAURANTS:
        try:
            # 1. Immutable Cache (Mês passado até ontem - fica rápido para sempre)
            fetch_sales_data(rest['id'], d30, d1)
            fetch_expenses_data(rest['id'], d30, d1)
            fetch_cmv_data(rest['id'], d30, d1)
            fetch_expenses_supplier_data(rest['id'], d30, d1)
            fetch_inbound_data(rest['id'], d30, d1)
            
            # 2. Ephemeral Cache (MTD - ajuda na lentidão de hoje)
            fetch_sales_data(rest['id'], d_mtd, hoje_str)
            fetch_expenses_data(rest['id'], d_mtd, hoje_str)
        except Exception as e:
            print(f"Erro no warm-up {rest['name']}: {e}")

def _send_to_chat(chat_id: int, text: str):
    """Envia texto (em chunks se necessário) para um chat_id específico."""
    if not text: return
    try:
        t = text
        while t:
            if len(t) <= 4000:
                bot.send_message(chat_id, t, parse_mode='Markdown')
                break
            idx = t.rfind('\n', 0, 4000)
            if idx == -1: idx = 4000
            bot.send_message(chat_id, t[:idx], parse_mode='Markdown')
            t = t[idx:].strip()
    except Exception as e:
        print(f"[scheduler] Erro ao enviar para chat_id={chat_id}: {e}")


def send_to_ceo(text):
    """
    Envia mensagem APENAS ao admin (ADMIN_CHAT_ID).
    Relatórios consolidados (briefing, ranking, auditoria) são CEO-only.
    Clientes recebem apenas dados dos seus próprios restaurantes — ver _send_alerts_per_client.
    """
    _send_to_chat(ADMIN_CHAT_ID, text)


def _extract_restaurant_sections(text: str, allowed_names: list) -> str:
    """
    Extrai de um relatório multi-restaurante apenas as seções permitidas.
    Seções são delimitadas por linhas '🏠 Nome do Restaurante'.
    Inclui sempre o cabeçalho do relatório (antes do primeiro 🏠).
    """
    if not text: return ""
    # Divide consumindo o \n imediatamente antes de cada '🏠 ' — evita splits duplos
    parts = re.split(r'\n(?=🏠 )', text)
    header = parts[0]  # tudo antes do primeiro 🏠 (totais, título)
    filtered = []
    for part in parts[1:]:
        first_line = part.split('\n', 1)[0].replace('🏠 ', '').strip()
        if any(name.lower() in first_line.lower() for name in allowed_names):
            filtered.append(part.strip())
    if not filtered:
        return ""
    return (header.strip() + '\n\n' + '\n\n'.join(filtered)).strip()


def _send_alerts_per_client(ceo_text: str):
    """
    Envia para cada cliente ativo (não-admin) APENAS as seções dos alertas
    referentes aos restaurantes que ele tem permissão de ver.
    """
    if not ceo_text: return
    clients_data = load_clients()
    for cid, client in clients_data.items():
        if not client.get("active") or not client.get("approved"):
            continue
        chat_id = int(cid)
        if chat_id == ADMIN_CHAT_ID:
            continue  # admin já recebe via send_to_ceo
        allowed = client.get("restaurants", [])
        if not allowed:
            continue
        allowed_names = [r["name"] for r in allowed]
        filtered = _extract_restaurant_sections(ceo_text, allowed_names)
        if filtered:
            _send_to_chat(chat_id, filtered)


if __name__ == "__main__":
    print("XMenu Live Bot is running...")
    import time
    import threading

    def scheduler_loop():
        """Background scheduler: Briefing at 7AM, Proactive Alerts every 4h, Weekly Ranking on Sundays."""
        _data_dir = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
        os.makedirs(_data_dir, exist_ok=True)
        STATE_FILE = os.path.join(_data_dir, "scheduler_state.json")
        
        def load_state():
            if os.path.exists(STATE_FILE):
                try:
                    with open(STATE_FILE, 'r') as f: return json.load(f)
                except: return {}
            return {}
            
        def save_state(state):
            try:
                with open(STATE_FILE, 'w') as f: json.dump(state, f)
            except: pass

        state = load_state()
        
        while True:
            try:
                now = datetime.now()
                today = str(now.date())
                
                # ── Segunda-feira Poderosa (8h) ─────────────────────────────────
                if now.weekday() == 0 and now.hour >= 8 and state.get('weekly_report') != today:
                    try:
                        report = ai_tools.get_weekly_consolidated_report()
                        # Adiciona raio-x do mês: comparativo mês atual vs anterior
                        try:
                            import calendar
                            mes_act_start = now.replace(day=1).strftime('%Y-%m-%d')
                            mes_ant_end   = (now.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
                            mes_ant_start = (now.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
                            total_at = 0; total_ant = 0
                            for r in RESTAURANTS:
                                try:
                                    s_at  = ai_tools.fetch_sales_data(r['id'], mes_act_start, now.strftime('%Y-%m-%d'))
                                    total_at  += sum(ai_tools.safe_float(s.get('valor',0)) for s in s_at)
                                    s_ant = ai_tools.fetch_sales_data(r['id'], mes_ant_start, mes_ant_end)
                                    total_ant += sum(ai_tools.safe_float(s.get('valor',0)) for s in s_ant)
                                except: pass
                            if total_ant > 0:
                                dias = now.day
                                dias_mes = calendar.monthrange(now.year, now.month)[1]
                                projecao = total_at / dias * dias_mes if dias > 0 else 0
                                var = (total_at - total_ant) / total_ant * 100
                                report += (f"\n\n📅 **RAIO-X DO MÊS (até dia {dias}):**\n"
                                           f"  Mês atual: R$ {total_at:,.2f} ({var:+.1f}% vs. mês passado)\n"
                                           f"  Projeção de fechamento: R$ {projecao:,.2f}\n")
                        except: pass
                        
                        # Top Fornecedores da semana
                        try:
                            sem_start = (now - timedelta(days=7)).strftime('%Y-%m-%d')
                            forn_map = {}
                            for r in RESTAURANTS:
                                try:
                                    exps = ai_tools.fetch_expenses_data(r['id'], sem_start, now.strftime('%Y-%m-%d'))
                                    for e in exps:
                                        fn = (e.get('fornecedor') or 'N/A').strip()
                                        fv = ai_tools.safe_float(e.get('valor', 0))
                                        if fn and fv > 0:
                                            forn_map[fn] = forn_map.get(fn, 0) + fv
                                except: pass
                            top_forn = sorted(forn_map.items(), key=lambda x: -x[1])[:8]
                            if top_forn:
                                report += f"\n\n📦 **TOP FORNECEDORES DA SEMANA:**\n"
                                for i, (fn, fv) in enumerate(top_forn, 1):
                                    report += f"  {i}. {fn[:40]}: R$ {fv:,.2f}\n"
                        except: pass

                        # Sugestão de Compras da Semana
                        try:
                            report += f"\n\n🛒 **PLANO DE COMPRAS DA SEMANA:**\n"
                            for r in RESTAURANTS:
                                compras = ai_tools.get_purchasing_plan(r['name'])
                                if compras:
                                    linhas = [l for l in compras.split('\n') if '•' in l or '🛒' in l]
                                    if linhas:
                                        report += f"\n🏠 **{r['name']}**\n"
                                        report += '\n'.join(linhas[:8]) + "\n" # Limita 8 itens por casa para não exceder limites
                        except: pass

                        if report:
                            send_to_ceo(report)
                            state['weekly_report'] = today
                            save_state(state)
                            print(f"[{now}] Relatório segunda-feira enviado.")
                    except Exception as e:
                        print(f"[{now}] Erro no relatório segunda-feira: {e}")
                
                # ── Ranking Domingo 21h ─────────────────────────────────────────
                if now.weekday() == 6 and now.hour >= 21 and state.get('ranking') != today:
                    try:
                        ranking = ai_tools.get_weekly_ranking()
                        if ranking:
                            send_to_ceo(ranking)
                            state['ranking'] = today
                            save_state(state)
                            print(f"[{now}] Ranking semanal enviado.")
                    except Exception as e:
                        print(f"[{now}] Erro no ranking semanal: {e}")
                
                # ── Auto-Sync / Cache Warmer ──────────────────────────────────────────────
                current_sync_key = f"{today}_{now.hour}"
                # Roda a cada 4 horas
                if now.hour % 4 == 0 and now.minute >= 10 and state.get('last_sync') != current_sync_key:
                    try:
                        warm_up_cache()
                        state['last_sync'] = current_sync_key
                        save_state(state)
                        print(f"[{now}] Cache Warmer concluído com sucesso.")
                    except Exception as e:
                        print(f"[{now}] Erro no Cache Warmer: {e}")
                
                # ── Briefing das 7h (via IA para síntese executiva) ────────────
                if now.hour >= 7 and state.get('briefing') != today:
                    try:
                        # 1. Coleta os dados brutos do briefing
                        briefing_raw = ai_tools.get_daily_briefing()
                        # 2. Passa os dados pelo modelo para síntese executiva real
                        briefing_prompt = (
                            f"BRIEFING MATINAL DO DIA {today} — dados das 3 casas:\n\n"
                            f"{briefing_raw}\n\n"
                            "Sintetize este briefing em linguagem executiva CEO. "
                            "Apresente: (1) panorama consolidado do grupo com variação vs ontem, "
                            "(2) destaque da casa com melhor e pior desempenho e por quê, "
                            "(3) alertas críticos que precisam de decisão hoje, "
                            "(4) as 3 prioridades do dia com responsável e prazo. "
                            "Seja direto, use números precisos, sem texto genérico."
                        )
                        briefing_ai = ai_manager.process_ceo_question(briefing_prompt, skip_tool_guard=True)
                        if briefing_ai:
                            header = f"☀️ *BOM DIA, CEO* — {now.strftime('%d/%m/%Y')}\n\n"
                            send_to_ceo(header + briefing_ai)
                        elif briefing_raw:
                            send_to_ceo(briefing_raw)  # fallback para o relatório bruto
                        state['briefing'] = today
                        save_state(state)
                        print(f"[{now}] Briefing diário (IA) enviado.")
                    except Exception as e:
                        print(f"[{now}] Erro no briefing: {e}")
                        # Fallback: tenta enviar o briefing bruto
                        try:
                            briefing_raw = ai_tools.get_daily_briefing()
                            if briefing_raw:
                                send_to_ceo(briefing_raw)
                                state['briefing'] = today
                                save_state(state)
                        except: pass
                
                # ── Fechamento das 23h ─────────────────────────────────────────
                if now.hour >= 23 and state.get('closing') != today:
                    try:
                        closing = ai_tools.get_daily_closing_report()
                        if closing:
                            send_to_ceo(closing)
                            state['closing'] = today
                            save_state(state)
                            print(f"[{now}] Relatório de fechamento enviado.")
                    except Exception as e:
                        print(f"[{now}] Erro no fechamento: {e}")

                # ── Cão de Guarda — Alertas de Preço Diários às 21h ──────────
                if now.hour >= 21 and state.get('cao_guarda') != today:
                    try:
                        report = ai_tools.get_watchdog_consolidated()
                        # Só envia se houver alertas reais
                        if "Tudo sob controle" not in report:
                            header = f"🐕 *CÃO DE GUARDA — {now.strftime('%d/%m/%Y')}*\n\n"
                            send_to_ceo(header + report[:3800])
                            print(f"[{now}] Cão de Guarda consolidado enviado.")
                        else:
                            print(f"[{now}] Cão de Guarda: sem alertas críticos.")
                        state['cao_guarda'] = today
                        save_state(state)
                    except Exception as e:
                        print(f"[{now}] Erro no Cão de Guarda: {e}")

                # ── Auditoria Cruzada Diária às 22h ────────────────────────────
                if now.hour >= 22 and state.get('audit_daily') != today:
                    try:
                        from ai_tools import RESTAURANTS
                        audit_sections = []
                        for rest in RESTAURANTS:
                            audit_raw = ai_tools.get_complete_audit(
                                rest['name'], start_date=today, end_date=today
                            )
                            if audit_raw and 'Erro' not in audit_raw[:30]:
                                audit_sections.append(audit_raw)

                        if audit_sections:
                            header = f"🔍 *AUDITORIA CRUZADA DIÁRIA — {now.strftime('%d/%m/%Y')}*\n\n"
                            full_audit = header + "\n\n─────────────────────────\n\n".join(audit_sections)
                            # Passa pela IA para síntese executiva
                            audit_prompt = (
                                f"AUDITORIA CRUZADA DO DIA {today} — dados das unidades:\n\n"
                                f"{full_audit[:8000]}\n\n"
                                "Sintetize em linguagem executiva CEO: (1) cite APENAS os desvios e alertas "
                                "reais encontrados, com nomes e valores exatos; (2) classifique por criticidade "
                                "(🔴 crítico, 🟡 atenção); (3) para cada item crítico, recomende uma ação imediata "
                                "com responsável. Se não houver desvios relevantes, informe brevemente."
                            )
                            audit_ai = ai_manager.process_ceo_question(audit_prompt, skip_tool_guard=True)
                            send_to_ceo((audit_ai or full_audit)[:4000])

                        state['audit_daily'] = today
                        save_state(state)
                        print(f"[{now}] Auditoria cruzada diária enviada.")
                    except Exception as e:
                        print(f"[{now}] Erro na auditoria diária: {e}")

                # ── Auditoria Semanal Profunda (Domingo às 10h, 7 dias) ─────────
                if now.weekday() == 6 and now.hour >= 10 and state.get('audit_weekly') != today:
                    try:
                        from ai_tools import RESTAURANTS
                        week_start = (now - timedelta(days=7)).strftime('%Y-%m-%d')
                        audit_sections = []
                        for rest in RESTAURANTS:
                            audit_raw = ai_tools.get_complete_audit(
                                rest['name'], start_date=week_start, end_date=today
                            )
                            if audit_raw and 'Erro' not in audit_raw[:30]:
                                audit_sections.append(audit_raw)

                        if audit_sections:
                            header = f"📋 *AUDITORIA SEMANAL CRUZADA — semana {week_start} a {today}*\n\n"
                            full_audit = header + "\n\n─────────────────────────\n\n".join(audit_sections)
                            audit_prompt = (
                                f"AUDITORIA CRUZADA SEMANAL ({week_start} a {today}):\n\n"
                                f"{full_audit[:8000]}\n\n"
                                "Como CEO, preciso do resumo semanal de desvios. Apresente: "
                                "(1) top 3 alertas críticos da semana com valores exatos; "
                                "(2) padrões recorrentes identificados (mesmo operador, mesmo fornecedor); "
                                "(3) total estimado em risco financeiro; "
                                "(4) 3 ações prioritárias com responsável e prazo para a próxima semana."
                            )
                            audit_ai = ai_manager.process_ceo_question(audit_prompt, skip_tool_guard=True)
                            send_to_ceo((audit_ai or full_audit)[:4000])

                        state['audit_weekly'] = today
                        save_state(state)
                        print(f"[{now}] Auditoria semanal enviada.")
                    except Exception as e:
                        print(f"[{now}] Erro na auditoria semanal: {e}")
                
                # ── Snapshot de Estoque — Segunda 07h e último dia do mês ──────
                import calendar as _cal
                _last_day = _cal.monthrange(now.year, now.month)[1]
                _is_monday = now.weekday() == 0
                _is_month_end = now.day == _last_day
                _snap_key = f"snapshot_{today}"

                if (_is_monday or _is_month_end) and now.hour >= 7 and state.get('inventory_snapshot') != _snap_key:
                    try:
                        from ai_tools import RESTAURANTS
                        snap_lines = []
                        for rest in RESTAURANTS:
                            result = ai_tools.save_inventory_snapshot(rest['name'], today)
                            snap_lines.append(result)
                            print(f"[{now}] {result.splitlines()[0]}")

                        label = "FIM DE MÊS" if _is_month_end else "SEMANAL"
                        msg = (f"📦 *SNAPSHOT DE ESTOQUE — {label}*\n"
                               f"_{today}_\n\n" + "\n\n".join(snap_lines))
                        send_to_ceo(msg)
                        state['inventory_snapshot'] = _snap_key
                        save_state(state)
                    except Exception as e:
                        print(f"[{now}] Erro no snapshot de estoque: {e}")

                # ── Alertas Proativos 5x/dia (8, 12, 16, 20, 00h) ─────────────
                if now.hour in [8, 12, 16, 20, 0]:
                    alert_key = f"{today}_{now.hour}"
                    if state.get('last_alert') != alert_key:
                        try:
                            result = ai_tools.get_proactive_alerts()
                            if isinstance(result, dict):
                                ceo_text = result.get('ceo', '')
                                if ceo_text:
                                    # Admin recebe o relatório completo (todas as casas)
                                    send_to_ceo(ceo_text)
                                    # Cada cliente recebe APENAS as seções dos seus restaurantes
                                    _send_alerts_per_client(ceo_text)

                                # Grupos de gerentes registrados via /register_group
                                manager_groups = load_manager_groups()
                                for rest_key, rest_alerts in result.get('per_restaurant', {}).items():
                                    if rest_key in manager_groups:
                                        group_id = manager_groups[rest_key]
                                        msg = f"🔔 **ALERTAS {rest_key.upper()}**\n\n"
                                        for a in rest_alerts[:10]:
                                            msg += f"{a}\n\n"
                                        try: bot.send_message(group_id, msg[:4000], parse_mode='Markdown')
                                        except: pass

                                state['last_alert'] = alert_key
                                save_state(state)
                                print(f"[{now}] Alertas proativos enviados (admin + clientes filtrados).")
                        except Exception as e:
                            print(f"[{now}] Erro nos alertas: {e}")
                
            except Exception as e:
                print(f"[{datetime.now()}] Erro no loop do agendador: {e}")
            
            time.sleep(60)  # Check every minute
    
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()
    
    # Aguarda a sessão anterior expirar antes de iniciar polling.
    # infinity_polling engole exceções internamente — usamos polling() para ter
    # controle total sobre erros 409 (conflito de instâncias).
    print("[startup] Aguardando sessão anterior expirar (60s)...")
    time.sleep(60)
    print("[startup] Iniciando polling.")

    while True:
        try:
            bot.polling(non_stop=False, timeout=20, allowed_updates=["message", "callback_query", "voice"])
        except Exception as e:
            err = str(e)
            if "409" in err or "Conflict" in err:
                print(f"[polling] 409 — aguardando 60s antes de tentar novamente...")
                time.sleep(60)
            else:
                print(f"[polling] erro: {err[:120]} — aguardando 5s...")
                time.sleep(5)
