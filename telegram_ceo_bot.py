import telebot
import os
import openpyxl
import requests
from datetime import datetime, timedelta
import json
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

AUTH_FILE = "authorized_chats.json"

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

ALLOWED_PHONES = ["5582991333541", "+5582991333541", "82991333541"]

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

def load_auth_chats():
    if os.path.exists(AUTH_FILE):
        try:
            with open(AUTH_FILE, "r") as f:
                return json.load(f)
        except: return []
    return []

def save_auth_chats(chats):
    with open(AUTH_FILE, "w") as f:
        json.dump(chats, f)

def check_auth(message):
    chats = load_auth_chats()
    if message.chat.id in chats: return True
    
    markup = telebot.types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    btn = telebot.types.KeyboardButton("Autorizar meu número", request_contact=True)
    markup.add(btn)
    bot.send_message(
        message.chat.id, 
        "⛔ *Acesso Restrito ao CEO!*\n\nPara a segurança dos dados financeiros, por favor, clique no botão abaixo para compartilhar seu Contato do Telegram, validando seu número (Diogo Albuquerque).", 
        reply_markup=markup, 
        parse_mode="Markdown"
    )
    return False

@bot.message_handler(content_types=['contact'])
def handle_contact(message):
    try:
        phone = message.contact.phone_number.replace("+", "").replace("-", "").replace(" ", "")
        allowed = [p.replace("+", "") for p in ALLOWED_PHONES]
        
        if phone in allowed:
            chats = load_auth_chats()
            if message.chat.id not in chats:
                chats.append(message.chat.id)
                save_auth_chats(chats)
            
            bot.send_message(
                message.chat.id, 
                "✅ *Acesso Liberado!* Identidade confirmada. Você já pode enviar seus comandos para a inteligência financeira.", 
                reply_markup=get_main_keyboard(), 
                parse_mode="Markdown"
            )
        else:
            bot.send_message(message.chat.id, f"❌ Número `{phone}` não autorizado. Acesso negado.", parse_mode="Markdown")
    except Exception as e:
        bot.send_message(message.chat.id, f"Erro na verificação: {e}")

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
    results = []
    total_geral = 0.0
    
    for rest in RESTAURANTS:
        if os.path.exists(rest['sales_file']):
            try:
                data = analyze_sales(rest['sales_file'])
                if data:
                    total_casa = data['total_sales']
                    results.append(f"• *{rest['name']}:* R$ {total_casa:,.2f}")
                    total_geral += total_casa
                else:
                    results.append(f"• *{rest['name']}:* Erro de formato.")
            except:
                results.append(f"• *{rest['name']}:* Erro ao ler JSON.")
        else:
            results.append(f"• *{rest['name']}:* Sem dados (use /sync)")
            
    target_date_str = (datetime.now() - timedelta(days=1)).strftime('%d/%m')
    header = f"💰 *Resumo Consolidado ({target_date_str})*\n\n"

    body = "\n".join(results)
    footer = f"\n\n💵 *TOTAL GERAL: R$ {total_geral:,.2f}*"

    
    bot.reply_to(message, header + body + footer, parse_mode='Markdown')

def send_long_msg(message, text):
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
        resposta_ia = ai_manager.process_ceo_question(transcribed_text, chat_id=message.chat.id)
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

    bot.answer_callback_query(call.id)

    if rest_code == "__ALL__":
        # Executa para todas as casas em sequência
        label = "Todas as Casas"
        bot.send_message(chat_id, f"⏳ Analisando *{label}*...", parse_mode="Markdown")
        for rest in RESTAURANTS:
            question = build_question_for_action(action, rest["name"])
            bot.send_chat_action(chat_id, "typing")
            bot.send_message(chat_id, f"🏠 *{rest['name']}*", parse_mode="Markdown")
            try:
                resposta = ai_manager.process_ceo_question(question, current_restaurant=rest["name"], chat_id=chat_id)
                # send_long_msg needs a message object; simulate with chat_id
                _send_long(chat_id, resposta)
            except Exception as e:
                bot.send_message(chat_id, f"❌ Erro em {rest['name']}: {e}")
    else:
        # Executa para a casa selecionada
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
            resposta_ia = ai_manager.process_ceo_question(message.text, chat_id=message.chat.id)
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

if __name__ == "__main__":
    print("XMenu Live Bot is running...")
    import time
    import threading
    
    def send_to_ceo(text):
        """Send a proactive message to all authorized chat IDs."""
        try:
            chats = load_auth_chats()
            for chat_id in chats:
                try:
                    if len(text) <= 4000:
                        bot.send_message(chat_id, text, parse_mode='Markdown')
                    else:
                        chunks = []
                        t = text
                        while len(t) > 0:
                            if len(t) <= 4000:
                                chunks.append(t)
                                break
                            idx = t.rfind('\n', 0, 4000)
                            if idx == -1: idx = 4000
                            chunks.append(t[:idx])
                            t = t[idx:].strip()
                        for c in chunks:
                            bot.send_message(chat_id, c, parse_mode='Markdown')
                except: pass
        except: pass

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
                        briefing_ai = ai_manager.process_ceo_question(briefing_prompt)
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
                            audit_ai = ai_manager.process_ceo_question(audit_prompt)
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
                            audit_ai = ai_manager.process_ceo_question(audit_prompt)
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
                                if result.get('ceo'):
                                    send_to_ceo(result['ceo'])
                                
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
                                print(f"[{now}] Alertas proativos enviados.")
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
