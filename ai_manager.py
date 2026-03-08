import openai
import json
import os
import ai_tools
from datetime import datetime

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OLLAMA_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
client = openai.OpenAI(api_key=OPENAI_API_KEY)

# --- CONVERSATION MEMORY ---
_DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
os.makedirs(_DATA_DIR, exist_ok=True)
MEMORY_FILE = os.path.join(_DATA_DIR, "conversation_memory.json")
MAX_HISTORY = 10  # Keep last 10 exchanges per chat

def load_memory():
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: return {}
    return {}

def save_memory(memory):
    try:
        with open(MEMORY_FILE, "w", encoding="utf-8") as f:
            json.dump(memory, f, ensure_ascii=False)
    except: pass

def get_chat_history(chat_id):
    memory = load_memory()
    return memory.get(str(chat_id), [])

def add_to_history(chat_id, role, content):
    memory = load_memory()
    key = str(chat_id)
    if key not in memory:
        memory[key] = []
    # Truncate content to avoid token overload (max 500 chars per entry)
    truncated = content[:500] if content else ""
    memory[key].append({"role": role, "content": truncated})
    # Keep only last MAX_HISTORY * 2 entries (pairs of user+assistant)
    memory[key] = memory[key][-(MAX_HISTORY * 2):]
    save_memory(memory)

tools = [
    {
        "type": "function",
        "function": {
            "name": "get_revenue",
            "description": "Obtém o faturamento total em Reais (R$) para o restaurante num período específico.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": { "type": "string", "description": "O nome do restaurante (Nauan, Milagres, Ahau)." },
                    "start_date": { "type": "string", "description": "Data inicial YYYY-MM-DD. Opcional, padrão ontem." },
                    "end_date": { "type": "string", "description": "Data final YYYY-MM-DD. Opcional." }
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_selling_items",
            "description": "Obtém a lista dos itens mais vendidos no restaurante (Top N) num período.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string", "description": "Nome do restaurante."},
                    "top_n": {"type": "integer", "description": "Número de itens para retornar (padrão 5)."},
                    "start_date": { "type": "string", "description": "Data inicial YYYY-MM-DD." },
                    "end_date": { "type": "string", "description": "Data final YYYY-MM-DD." }
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_sales",
            "description": "Procura pelas vendas ou quantidade vendida de um item específico (ex: Day Use) num período.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Nome do produto a buscar, ex: Cerveja, Day Use, Vinho."},
                    "start_date": { "type": "string", "description": "Data inicial YYYY-MM-DD." },
                    "end_date": { "type": "string", "description": "Data final YYYY-MM-DD." }
                },
                "required": ["restaurant_name", "query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_expenses",
            "description": "Consulta as despesas/contas a pagar filtradas num período (ou seja, os pagamentos FEITOS pelo restaurante para fornecedores, impostos, folha, compras, etc).",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Termo de busca opcional (ex: fornecedor ou categoria)."},
                    "start_date": { "type": "string", "description": "Data inicial YYYY-MM-DD." },
                    "end_date": { "type": "string", "description": "Data final YYYY-MM-DD." }
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock",
            "description": "Procura a quantidade atual no estoque e o custo unitário de um ingrediente ou produto.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Nome do produto no estoque, ex: Batata, Heineken, Salmão."}
                },
                "required": ["restaurant_name", "query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_recipe",
            "description": "Consulta a Ficha Técnica de um prato ou drink.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "dish_name": {"type": "string"}
                },
                "required": ["restaurant_name", "dish_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_recipes_profitability",
            "description": "Faz uma análise e auditoria completa de fichas técnicas. Retorna erros de cadastro (custo zero), pratos mais caros de produzir e dicas.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Opcional. Apenas UMA PALAVRA ou termo curto para filtrar (ex: 'Drinks', 'Cozinha', 'Camarão'). NUNCA envie frases completas."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_inbound_purchases",
            "description": "Lista as compras mais recentes de produtos na planilha de mercadorias.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD."}
                },
                "required": ["restaurant_name", "query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_scenario",
            "description": "Obtém o cenário geral de um ou mais restaurantes para um período específico. Retorna vendas, despesas e KPI.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_names": {"type": "array", "items": {"type": "string"}},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_names", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_ingredient_consumption",
            "description": "Calcula o consumo e lista as vendas dos pratos que levam um insumo específico (Ex: Filé Mignon, Camarão) cruzando Vendas e Ficha Técnica.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Nome do insumo. Ex: Mignon, Camarão."},
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"}
                },
                "required": ["restaurant_name", "query", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_audit",

            "description": "Faz uma auditoria/controle de um insumo ou fornecedor, cruzando estoque atual, compras e vendas num período fechado.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Item, insumo ou fornecedor (Ex: Heineken, Ambev, Day Use)."},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name", "query", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_purchasing_plan",
            "description": "Calcula a sugestão de compras para Itens de Revenda usando média de vendas vs estoque atual. Opcionalmente informa os dias ou usa o default 7.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Categoria, grupo, ou nome do insumo (ex: 'Limpeza', 'Bebidas')."},
                    "days_history": {"type": "integer", "description": "Quantos dias olhar para calcular a média de venda"},
                    "coverage_days": {"type": "integer", "description": "Quantos dias queremos ter como alvo para cobertura de estoque"}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_waste_audit",
            "description": "Gera um dossiê de auditoria antifurto e desperdício. Calcula o consumo teórico exato baseado nas fichas técnicas vs vendas da semana, e confronta com o estoque atual do sistema para instruir uma contagem cega física pelo gerente.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Insumo, grupo ou Categoria. Pode ser vazio para gerar geral (Top piores)"},
                    "days_history": {"type": "integer", "description": "Quantos dias para trás auditar."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_menu_engineering",
            "description": "Calcula a Matriz BCG e a Engenharia de Cardápio cruzando Vendas vs. CMV. Categoriza pratos em Estrelas, Cavalos de Batalha (Burros de Carga), Quebra-Cabeças e Cachorros.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_supplier_inflation",
            "description": "Rastreia aumentos ou reduções ocultas de preço de fornecedores comparando notas fiscais recentes vs anteriores. Alerta se algum insumo subiu mais de 3%.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Produto ou fornecedor específico para filtrar (ex: Heineken, Ambev)."},
                    "days_recent": {"type": "integer", "description": "Janela recente em dias (padrão: 15)."},
                    "days_old": {"type": "integer", "description": "Janela anterior em dias (padrão: 30)."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_cashflow_runway",
            "description": "Projeta o fluxo de caixa líquido cruzando a média de faturamento diário com as contas a pagar pendentes nos próximos X dias. Alerta se haverá déficit.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "days_forward": {"type": "integer", "description": "Quantos dias para frente projetar (padrão: 7)."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_weather_forecast",
            "description": "Consulta a previsão do tempo dos próximos 7 dias para a região dos restaurantes (Litoral AL) e analisa o impacto nos negócios (chuva = menos movimento, sol = lotação máxima).",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string", "description": "Nome do restaurante (opcional, região é a mesma para todos)."}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_invoice_reconciliation",
            "description": "Concilia notas fiscais de entrada (estoque) com o contas a pagar (financeiro). Detecta notas que deram entrada no estoque mas não foram lançadas no financeiro.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "days": {"type": "integer", "description": "Quantos dias para trás verificar (padrão: 15)."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_complete_audit",
            "description": "Auditoria Cruzada Completa CEO: a ferramenta mais poderosa de auditoria. Cruza PROGRAMATICAMENTE cancelamentos × caixa por operador (detecta fraude), entradas NF × despesas (detecta passivo oculto), pagamentos × entradas (detecta pagamento sem recebimento), duplicatas de pagamento, e produtos com alto cancelamento suspeito. Use quando o CEO pedir auditoria completa, cruzamento de dados, desvios, fraudes operacionais ou pagamentos injustificados.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD (padrão: 7 dias atrás)."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (padrão: hoje)."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_daily_briefing",
            "description": "Gera o briefing executivo diário do CEO com faturamento de ontem das 3 casas, variação vs semana passada, alertas de estoque negativo, clima e contas do dia.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "simulate_price_change",
            "description": "Simula o impacto financeiro de aumentar ou diminuir o preço de um produto. Calcula quanto a mais ou a menos o restaurante faturaria por semana e por mês.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "product_name": {"type": "string", "description": "Nome do produto (ex: Filé Mignon, Heineken, Caipirinha)."},
                    "price_change": {"type": "number", "description": "Valor da alteração em reais. Positivo = aumento, negativo = redução. Ex: 5 para +R$5."},
                    "days_history": {"type": "integer", "description": "Dias de histórico para base de cálculo (padrão: 7)."}
                },
                "required": ["restaurant_name", "product_name", "price_change"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_cmv_report",
            "description": "Busca o relatório oficial de CMV e Markup no portal, revelando margem de lucro de bebidas e comidas. Cruza Custos x Venda.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Item, grupo ou subgrupo para investigar (ex: Drinks, Heineken, Pratos)."},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_weekly_ranking",
            "description": "Gera um ranking comparativo semanal entre todas as casas (Nauan, Milagres, Ahau), comparando Faturamento, CMV e Risco de Estoque.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_revenue_tracker",
            "description": "Calcula o progresso das metas de faturamento mensal de cada casa vs o alvo definido, mostrando se estão no ritmo ou abaixo.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string", "description": "Opcional. Se vazio, traz de todas."}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_product_specs",
            "description": "Busca o Dossiê Técnico de um produto no portal, trazendo NCM, CEST, ID, Grupo e a Composição (Ficha Técnica / Receita) completa com ingredientes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "product_name": {"type": "string", "description": "Nome do produto (ex: Filé, Heineken, Moscow Mule)."}
                },
                "required": ["restaurant_name", "product_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_inventory_turnover",
            "description": "Analisa o giro de estoque, identificando capital empatado (itens parados há 30 dias) e estoques excessivos que estão prejudicando o fluxo de caixa.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "query": {"type": "string", "description": "Opcional. Filtrar por item ou grupo."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_cancellation_report",
            "description": "Analisa os motivos de cancelamentos de itens em um período, identificando perdas operacionais, erros de lançamento ou problemas de estoque.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_supplier_report",
            "description": "Gera um ranking dos maiores fornecedores e um detalhamento de títulos (auditoria) para um período, conforme solicitado pelo CEO.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_commission_report",
            "description": "Analisa as comissões dos garçons e produtividade da brigada para um período.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_report",
            "description": "Analisa o faturamento RECEBIDO DOS CLIENTES por formas de pagamento (Pix, Crédito, Débito, Dinheiro) em um período. NUNCA use para pagamentos feitos pelo restaurante.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_cashier_closure_report",
            "description": "Analisa as quebras de caixa (diferenças entre sistema e valor apurado) para um período.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_fiscal_report",
            "description": "Analisa a emissão fiscal (NFC-e), notas emitidas, canceladas e conformidade tributária para um período.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name", "start_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "audit_product_registration",
            "description": "Audita o cadastro de produtos procurando por erros fiscais (NCM incorreto, falta de grupos, nomes genéricos).",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_tax_combo_suggestions",
            "description": "Analisa oportunidades de economia fiscal usando regime monofásico de PIS/COFINS. Identifica bebidas (já isentas de PIS/COFINS na revenda) e sugere combos Bebida + Comida para reduzir legalmente a base tributável. Calcula a economia estimada.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "Data inicial YYYY-MM-DD para base de vendas (padrão: últimos 30 dias)."},
                    "end_date": {"type": "string", "description": "Data final YYYY-MM-DD (opcional)."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_financial_snapshot",
            "description": "Gera um Raio-X de saúde financeira, cruzando saldo bancário real, faturamento previsto e contas a pagar dos próximos 15 dias. Crucial para gestão de fluxo de caixa.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_break_even_analysis",

            "description": "Calcula o Ponto de Equilíbrio (Break-Even) Mensal da casa. Analisa a estrutura de custos fixos, CMV médio e taxas variáveis para definir quanto o restaurante precisa vender para começar a ter lucro.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "apply_price_change",
            "description": "Aplica (ou tenta aplicar no ERP) um reajuste de preço (ex: 10%, 5, -2) em um prato/produto.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "product_name": {"type": "string"},
                    "price_change": {"type": "string", "description": "Ex: '10%' ou '5'"}
                },
                "required": ["restaurant_name", "product_name", "price_change"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_customer_success_report",
            "description": "Análise completa de satisfação de clientes: avaliações reais do Google (nota, temas, reviews negativos), cancelamentos do sistema e tendência de faturamento. Use quando pedirem 'avaliações', 'reviews', 'satisfação', 'nota Google', 'reclamações' de uma casa específica.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "days": {"type": "integer", "description": "Período de análise em dias. Padrão: 30."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_reviews_consolidated",
            "description": "Painel consolidado de avaliações Google das 3 casas (nota, total de avaliações, reviews negativos recentes). Use quando pedirem 'avaliações das casas', 'notas Google do grupo', 'ranking de satisfação'.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "save_inventory_snapshot",
            "description": "Salva o snapshot do estoque atual com data específica. Use no último dia do mês ou quando o CEO pedir 'salvar estoque', 'fechar estoque do mês', 'snapshot de estoque'. Necessário para calcular o CMV por movimentação (EI + Compras - EF) no DRE.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "date_str": {"type": "string", "description": "YYYY-MM-DD. Padrão: hoje."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_dre_report",
            "description": "Gera o DRE gerencial do restaurante. Retorna Receita, Impostos, CMV Real, CMV Teórico, Folha, Despesas e EBITDA. Quando snapshots de estoque existirem, calcula o CMV por movimentação (EI + Compras - EF) — mais preciso.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "end_date": {"type": "string", "description": "YYYY-MM-DD"}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_balancete",
            "description": "Busca o Balancete Contábil diretamente do portal netcontroll (rota #/relatorio/balancete). Apresenta saldos de débito, crédito e saldo por plano de contas para o período. Use quando o CEO pedir 'balancete', 'saldos contábeis', 'balanço de contas' ou 'extrato contábil'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "start_date": {"type": "string", "description": "YYYY-MM-DD. Padrão: início do mês atual."},
                    "end_date":   {"type": "string", "description": "YYYY-MM-DD. Padrão: hoje."}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_realtime_fraud_alert",
            "description": "Verifica em tempo real (hoje) por quebras de caixa suspeitas e cancelamentos de alto valor feitos por garçons.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_dynamic_pricing_suggestions",
            "description": "Retorna sugestões de precificação dinâmica (aumentar/reduzir preços no ERP) cruzando Clima, Ocupação e Estoque parado.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"}
                },
                "required": ["restaurant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_predictive_hr_scale",
            "description": "Calcula a necessidade de RH (Garçons/Cozinha) no futuro usando média móvel de vendas ajustada pelo Clima.",
            "parameters": {
                "type": "object",
                "properties": {
                    "restaurant_name": {"type": "string"},
                    "target_date": {"type": "string", "description": "YYYY-MM-DD. Opcional, assume amanhã se não for enviado."}
                },
                "required": ["restaurant_name"]
            }
        }
    }
]

def _build_analysis_prompt(tools_called):
    """Inject deep-analysis instructions AFTER tool data is loaded, BEFORE second LLM call."""
    instructions = [
        """⚠️ ATENÇÃO ANTES DE ESCREVER QUALQUER NÚMERO:
Você acabou de receber dados reais das ferramentas do sistema. Siga estas regras absolutas agora:
1. Cada número que você escrever DEVE estar literalmente no retorno da ferramenta acima.
2. Se um campo não foi retornado → escreva "_não disponível no sistema_", nunca estime.
3. Ticket médio só se o nº de atendimentos veio da ferramenta. CMV% só se custo E receita vieram.
4. Cite nomes de produtos, fornecedores e garçons EXATAMENTE como aparecem nos dados.
5. NUNCA use "aproximadamente", "cerca de", "estimado" para dados que deveriam ser exatos.

Agora produza o relatório CEO seguindo as instruções específicas abaixo:"""
    ]
    tool_set = set(tools_called)

    if "get_balancete" in tool_set:
        instructions.append("""
📒 BALANCETE — ANÁLISE CONTÁBIL:
- Apresente TODOS os grupos do plano de contas com débito, crédito e saldo
- Classifique: grupos com saldo credor = receitas/passivos; grupos com saldo devedor = despesas/ativos
- Calcule: Total Créditos vs Total Débitos → Resultado do período (superávit ou déficit)
- Compare com DRE se disponível: os valores batem? Diferença indica lançamentos pendentes
- Identifique: alguma conta com movimento atípico (muito acima do histórico)?
- Diagnóstico: empresa está em superávit ou déficit contábil no período?""")

    if "get_dre_report" in tool_set:
        instructions.append("""
📊 SOBRE O DRE GERENCIAL:
- OBRIGATÓRIO: Quando citar a linha de Custos (CMV), você DEVE mencionar OBRIGATORIAMENTE tanto o "CMV Real" quanto o "CMV Teórico Esperado", evidenciando qualquer desvio ou ineficiência entre o que foi consumido na realidade e o que a ficha técnica mandava gastar.

📊 DRE — CASCATA COMPLETA LINHA POR LINHA (obrigatório, não pule nenhuma linha):
1. Receita Bruta Total → discriminada por categoria se disponível
2. (-) Impostos → regime tributário, alíquota % e valor R$
3. (=) Receita Líquida + % sobre Receita Bruta
4. (-) CMV → valor R$, % sobre Receita Líquida, benchmark F&B 28-35% — diga se está dentro ou fora
5. (=) Lucro Bruto + margem bruta %
6. (-) Folha de Pagamento → valor R$ e % sobre receita
7. (-) Despesas Fixas → liste CADA despesa nominalmente com valor
8. (-) Despesas Variáveis → liste CADA despesa nominalmente com valor
9. (=) EBITDA / Resultado Operacional + margem %
10. Diagnóstico: acima ou abaixo do break-even? Quanto sobra ou falta?
Se um valor não veio da ferramenta, escreva "Não informado" — nunca omita a linha.""")

    if "get_cmv_report" in tool_set:
        instructions.append("""
📦 CMV — RENTABILIDADE POR ITEM:
- Liste TODOS os itens: CMV unitário R$, preço venda R$, markup real, % CMV
- Top 5 destruidores de margem (CMV% alto + volume alto): para cada um, sugira ajuste de preço OU redução de porção OU substituição com impacto R$ estimado
- Top 5 heróis de lucro (markup alto + volume alto)
- Benchmark: CMV F&B ≤ 35%, bebidas ≤ 25%. Se acima, diga quanto e o impacto em R$ no resultado""")

    if "get_scenario" in tool_set:
        instructions.append("""
🏢 CENÁRIO EXECUTIVO — BOARD ROOM REVIEW:
- Faturamento: cite o valor EXATO retornado. Variação vs período anterior SÓ se a ferramenta trouxe dado comparativo.
- Ticket médio: SÓ calcule se nº de atendimentos estiver nos dados. Caso contrário: "_atendimentos não disponível_".
- CMV %: SÓ calcule se custo E receita estiverem nos dados. Cite ambos os valores usados no cálculo.
- Despesas: SÓ cite se vieram da ferramenta. Nunca infira despesas não listadas.
- Resultado líquido: SÓ calcule se Receita, CMV e Despesas estiverem TODOS presentes nos dados.
- Top produtos: cite EXATAMENTE os nomes e valores como vieram. Não reordene, não resuma.
- Anomalia: cite o valor real que está fora do padrão + benchmark real que você está comparando.""")

    if "get_revenue" in tool_set or "get_top_selling_items" in tool_set or "search_sales" in tool_set:
        instructions.append("""
💰 ANÁLISE DE VENDAS:
- Faturamento: cite o valor EXATO retornado + período confirmado da ferramenta.
- Ticket médio: SÓ calcule se nº de atendimentos veio da ferramenta. Mostre a conta: R$X ÷ Y atendimentos = R$Z. Se não veio: "_não disponível_".
- Produtos: liste com nomes EXATOS da ferramenta, quantidade e valor. Não reordene, não traduza nomes.
- Classificação BCG (Estrela/Cavalo/Questionamento/Abacaxi): SÓ se a ferramenta retornou dado de margem E volume.
- Tendência vs período anterior: SÓ se a ferramenta trouxe dados comparativos. Caso contrário omita.""")

    if "get_expenses" in tool_set:
        instructions.append("""
💳 DESPESAS — ANÁLISE LINHA POR LINHA:
- Liste CADA fornecedor/despesa exatamente como veio: nome real, valor real, categoria real.
- % do total: calcule com os valores reais retornados. Mostre: R$X ÷ R$Total × 100 = Y%.
- Despesas Fixas vs Variáveis: SÓ se a ferramenta classificou. Não infira a categoria.
- Concentração (>15%): só alerte se o dado de percentual estiver calculado corretamente a partir dos dados reais.
- Comparação com receita: SÓ se a receita também veio de outra ferramenta nesta mesma sessão.""")

    if "get_stock" in tool_set or "get_inventory_turnover" in tool_set:
        instructions.append("""
📦 ESTOQUE — CAPITAL EMPATADO:
- Liste todos os itens: nome, quantidade, unidade, valor unitário, valor total
- Capital total empatado = soma de todos os itens × custo
- Itens sem movimento há mais de 7 dias → risco de vencimento, sugira ação (promoção, uso no menu do dia)
- Giro de estoque: quantos dias de operação o estoque atual cobre?""")

    if "get_recipe" in tool_set or "analyze_recipes_profitability" in tool_set:
        instructions.append("""
🍽️ FICHAS TÉCNICAS — LUCRATIVIDADE:
- Para cada prato: ingredientes com custo unitário + custo total da ficha
- CMV do prato = custo / preço de venda × 100
- Markup = preço de venda / custo da ficha — benchmark mínimo 2,5x (ideal 3x+)
- Pratos abaixo de 2,5x markup: recomende ajuste de preço ou reformulação com impacto R$ estimado""")

    if "get_inbound_purchases" in tool_set:
        instructions.append("""
🚚 COMPRAS RECEBIDAS:
- Liste TODAS as notas: fornecedor, produto, quantidade, valor unitário, valor total, data
- Total comprado no período em R$ — representa X% da receita (ideal < 35%)
- Fornecedores com maior peso: concentração de risco?
- Variação de preço vs compras anteriores: houve aumento? Impacto no CMV do prato afetado?""")

    if "get_cancellation_report" in tool_set:
        instructions.append("""
❌ CANCELAMENTOS — ANÁLISE ANTIFRAUDE:
- Liste cada cancelamento: garçom, item, valor, hora, mesa
- Ranking por número de cancelamentos E por valor total cancelado
- Alerta vermelho: qualquer garçom com cancelamentos > 2× a média da equipe
- Padrão suspeito: mesmo horário, mesmo garçom, itens de alto valor repetidos
- Impacto financeiro total no faturamento do período""")

    if "get_commission_report" in tool_set:
        instructions.append("""
👨‍💼 COMISSÕES — PERFORMANCE DE GARÇONS:
- Liste cada garçom: faturamento gerado, % comissão, valor a receber, nº atendimentos, ticket médio
- Ranking por faturamento gerado
- Disparidade > 30% entre melhor e pior → treinamento necessário
- Garçom com alto volume mas baixo ticket médio: quantidade sem valor agregado — ação de upselling""")

    if "get_menu_engineering" in tool_set:
        instructions.append("""
🎯 ENGENHARIA DE CARDÁPIO — MATRIZ BCG:
- Classifique CADA item: ⭐ Estrela | 🐎 Cavalo de Batalha | ❓ Questionamento | 💀 Abacaxi
- Cavalos de Batalha: aumente preço ou troque ingrediente para elevar margem — impacto estimado R$
- Questionamentos: promoção, destaque no cardápio ou upselling ativo
- Abacaxis: retire do cardápio ou reformule urgentemente
- Se Cavalos virassem Estrelas, quanto o lucro mensal aumentaria?""")

    if "get_waste_audit" in tool_set or "get_audit" in tool_set:
        instructions.append("""
🔍 AUDITORIA DE DESPERDÍCIO/INSUMO:
- Liste cada discrepância: item, sistema vs físico, diferença, valor da diferença
- Total de perda em R$ + % do CMV total
- Padrão: concentrado em categoria? Turno? Colaborador?
- Ação imediata recomendada: inventário surpresa, câmera, responsabilização por setor""")

    if "get_break_even_analysis" in tool_set:
        instructions.append("""
📈 BREAK-EVEN — PONTO DE EQUILÍBRIO:
- Custos Fixos Totais em R$ (listados nominalmente)
- Margem de Contribuição Média % = (Receita − CMV − Custos Variáveis) / Receita
- Break-even = Custos Fixos / Margem de Contribuição = R$ X necessário por mês
- Faturamento atual está X% acima/abaixo do break-even
- Clientes adicionais necessários para atingir break-even a ticket médio de R$ Z
- Meta de segurança: break-even + 20% = R$ [valor]""")

    if "get_financial_snapshot" in tool_set:
        instructions.append("""
💼 RAIO-X FINANCEIRO:
- Saldo em caixa vs compromissos a vencer em 30 dias
- Índice de liquidez = saldo / compromissos (aceitável > 1,2x)
- Contas a receber vs contas a pagar: posição líquida
- Alerta: compromisso relevante nos próximos 7 dias sem cobertura de caixa?""")

    if "get_cashflow_runway" in tool_set:
        instructions.append("""
🔮 FLUXO DE CAIXA — RUNWAY:
- Com saldo atual e queima média: quantos dias a operação sobrevive sem nova receita?
- Data crítica em que o caixa zeraria
- Receita diária necessária para manter saldo positivo
- Se runway < 45 dias: acionar antecipação de recebíveis, cortar despesa variável ou linha de crédito""")

    if "get_supplier_report" in tool_set or "get_supplier_inflation" in tool_set:
        instructions.append("""
🏭 FORNECEDORES — RISCO E CUSTO:
- Ranking por peso no total de despesas (% e R$)
- Algum fornecedor > 20% do total? → risco de dependência
- Quem subiu preço mais de 5% nos últimos pedidos? → impacto no CMV do prato afetado
- Top 3 por valor: recomende cotação paralela com prazo e responsável""")

    if "get_payment_report" in tool_set:
        instructions.append("""
💳 FORMAS DE PAGAMENTO:
- Liste cada forma: valor total R$, % do faturamento, nº transações, ticket médio por modalidade
- Taxa de cartão estimada sobre o total (débito ~1,5%, crédito ~2-3%, Pix 0%)
- Custo financeiro das taxas em R$ no período
- Recomendação: incentivar Pix ou débito para reduzir custo financeiro — impacto estimado R$""")

    if "get_cashier_closure_report" in tool_set:
        instructions.append("""
🏦 FECHAMENTO DE CAIXA:
- Diferença entre caixa esperado e caixa físico por turno e operador
- Ranking de operadores por divergência total
- Padrão de quebra: recorrente no mesmo turno? Mesmo operador?
- Alerta: qualquer quebra acima de R$ 50 sem justificativa documentada""")

    if "get_realtime_fraud_alert" in tool_set:
        instructions.append("""
🚨 ALERTAS DE FRAUDE EM TEMPO REAL:
- Liste cada alerta com: tipo, valor, responsável, hora, detalhes
- Classifique por criticidade: crítico / suspeito / observação
- Para cada alerta crítico: ação imediata recomendada (bloquear, acionar gerente, auditoria)
- Operadores com AMBOS (cancela alto + quebra de caixa) = fraude provável — nomear e acionar""")

    if "get_complete_audit" in tool_set:
        instructions.append("""
🔍 AUDITORIA CRUZADA COMPLETA — PROTOCOLO OBRIGATÓRIO:

A) CRUZAMENTO CANCELAMENTOS × CAIXA (Seção A dos dados):
- Nomear CADA operador com sinal cruzado (cancela + quebra). Valor cancelado, % do total, diferença de caixa.
- Se simultâneo: classificar como FRAUDE PROVÁVEL e recomendar ação disciplinar/investigação imediata.
- Operadores só com cancelamento alto: listar com motivos e padrão identificado.

B) ENTRADAS NF SEM LANÇAMENTO FINANCEIRO (Seção B):
- Listar TODAS as NFs pendentes: número, fornecedor, valor, data.
- Total do passivo oculto em R$.
- Risco: estoque recebido = dívida não registrada. Ação: regularizar no contas a pagar até [data].

C) PAGAMENTOS SEM ENTRADA (Seção C):
- Listar CADA fornecedor pago sem NF de entrada: nome, valor total, histórico.
- Distinguir: pode ser serviço (aluguel, folha) OU pagamento indevido. Citar para validação.
- Total pago sem comprovação de entrega em R$.

D) DUPLICATAS DE PAGAMENTO (Seção D):
- Para cada padrão duplicado: fornecedor, quantidade de lançamentos, valor total, histórico.
- Recomendar: verificar extrato bancário para confirmar se ambos foram debitados.

E) PRODUTOS COM CANCELAMENTO SUSPEITO (Seção E):
- Listar top produtos cancelados com motivo "SEM MOTIVO" ou suspeito.
- Calcular perda acumulada em R$ e sugerir controle operacional específico.

F) QUEBRAS DE CAIXA (Seção F):
- Ranking por operador com maior diferença acumulada.
- Recomendar: conferência física, câmera, supervisor de turno.

IMPORTANTE: Cite os valores EXATOS que vieram nos dados. Não estime nem calcule fora dos dados recebidos.""")

    if "get_daily_briefing" in tool_set:
        instructions.append("""
☀️ BRIEFING DIÁRIO:
- Resumo completo do dia: faturamento, atendimentos, ticket médio, CMV estimado
- Destaques positivos do dia (o que funcionou bem)
- Alertas do dia (o que precisa de atenção imediata)
- 3 prioridades para amanhã com responsável e prazo""")

    if "get_weekly_ranking" in tool_set:
        instructions.append("""
🏆 RANKING SEMANAL — 3 CASAS:
- Compare Nauan, Milagres e Ahau: faturamento R$, ticket médio, atendimentos, CMV %
- Vencedor da semana com margem de diferença
- Casa com pior performance: diagnóstico e plano de recuperação específico
- KPI mais preocupante em cada casa: ação recomendada por unidade""")

    # Universal closing — always present
    instructions.append("""
---
ENCERRE SEMPRE COM:
💡 **PLANO DE AÇÃO CEO — TOP 5 AÇÕES:**

⚠️ **REGRA DE OURO PARA O PLANO DE AÇÃO**: 
1) Se ABSOLUTAMENTE NENHUM dado (0 dados) foi retornado pelas ferramentas para o período analisado (ou seja, tudo veio como 'indisponível' ou 'vazio'), responda APENAS: "⚠️ Plano de Ação suspenso: Sem dados sistêmicos (vendas/despesas) neste recorte para formular ações concretas." e encerre.
2) Se HOUVER PELO MENOS UM dado real válido (ex: só tem despesa mas não tem faturamento, ou vice-versa), você DEVE formular ações focadas EXCLUSIVAMENTE nos dados que retornaram, sem inventar sobre o que faltou.

Regra: cada ação DEVE citar o dado real dos dados recebidos que a justifica.
✓ "Produto X custou R$ 9,20 na última compra (dado da ferramenta). Preço atual R$ 12,00 = markup 1,3x, abaixo do mínimo 2,5x. Ajustar para R$ 23,00 — gerente — hoje."
✗ "Revisar preços" — proibido. Ação sem dado que a suporte = não enviar.
✗ "O custo provavelmente subiu" — proibido usar "provavelmente" ou "deve ter".

Para cada ação:
- Dado real que justifica (cite o número exato)
- O que fazer (ação concreta)
- Responsável (nome/cargo se veio nos dados, senão "gerente responsável")
- Prazo
- Impacto em R$ (SÓ se calculável a partir dos dados reais — senão omita o valor)""")

    return "\n".join(instructions)


def _build_system_prompt(current_restaurant):
    today = datetime.now().strftime('%Y-%m-%d')
    return f"""Você é o Consultor Estratégico CEO do Grupo Milagres (Nauan Beach Club, Milagres do Toque, Ahau Arte e Cozinha). Restaurante atual: {current_restaurant}. Hoje: {today}.

Suas fontes de dados principais no portal NetControll são:
- Vendas por Produto: https://portal.netcontroll.com.br/#/relatorio/venda-produto
- Vendas por Hora: https://portal.netcontroll.com.br/#/relatorio/venda-produto-hora
- Faturamento por Forma de Pagamento: https://portal.netcontroll.com.br/#/relatorio/faturamento-forma-pagamento
- Contas a Pagar (Plano de Contas): https://portal.netcontroll.com.br/#/relatorio/financeiro-conta-pagar-plano
- Contas a Pagar (Fornecedor): https://portal.netcontroll.com.br/#/relatorio/financeiro-conta-pagar-fornecedor

════════════════════════════════════════
⛔ REGRAS DE FIDELIDADE — INVIOLÁVEIS
════════════════════════════════════════

**REGRA 1 — DADO NÃO VEIO DA FERRAMENTA = NÃO EXISTE.**
Você NUNCA pode inventar, estimar, aproximar ou extrapolar faturamentos, quantidades, nomes, custos ou qualquer valor numérico. Se o dado não está no retorno da ferramenta, ele não existe para você.

**REGRA 2 — CHAME A FERRAMENTA ANTES DE QUALQUER RESPOSTA SOBRE DADOS.**
Antes de dizer qualquer número, execute a ferramenta correspondente. Sem chamada de ferramenta = sem número. Nunca responda com base em suposições sobre "o que provavelmente é".

**REGRA 3 — CITE OS VALORES EXATAMENTE COMO VIERAM DA FERRAMENTA.**
Se a ferramenta retornou "R$ 12.450,00", você escreve "R$ 12.450,00". Não arredonde, não parafraseie, não converta.

**REGRA 4 — DADO AUSENTE = DECLARAR AUSÊNCIA, NUNCA ESTIMAR.**
Se a ferramenta não retornou folha de pagamento, você escreve: "Folha: _dado não disponível no sistema_". NUNCA escreva um valor estimado. NUNCA diga "aproximadamente".

**REGRA 5 — LISTA COMPLETA, NUNCA RESUMIDA.**
Se a ferramenta retornar 40 itens, cite todos os 40. "Top 5" só é aceitável se a ferramenta explicitamente retornou apenas 5 itens.

**REGRA 6 — NUNCA CALCULE ALGO QUE A FERRAMENTA NÃO FORNECEU OS COMPONENTES.**
Ticket médio = Receita ÷ Atendimentos. Se atendimentos não veio da ferramenta, NÃO calcule ticket médio. Escreva: "Ticket médio: _nº de atendimentos não disponível_".
CMV % = Custo ÷ Receita × 100. Só calcule se AMBOS os valores vieram da ferramenta.

**REGRA 7 — VERIFICAÇÃO FINAL ANTES DE ENVIAR.**
Antes de escrever cada número na resposta, pergunte mentalmente: "Este valor está literalmente no retorno da ferramenta?" Se não → remova ou marque como "não disponível".

════════════════════════════════════════
🔧 FERRAMENTA CERTA PARA CADA PEDIDO
════════════════════════════════════════
- Faturamento / receita → get_revenue
- Itens mais vendidos → get_top_selling_items
    - Buscar produto específico nas vendas → search_sales
    - Pagamentos FEITOS pelo restaurante (despesas, fornecedores, contas a pagar) → get_expenses  [portal: /financeiro-conta-pagar-plano]
    - Pagamentos RECEBIDOS de clientes (faturamento / Pix / Cartão / Dinheiro) → get_payment_report  [portal: /faturamento-forma-pagamento]
    - Despesas por fornecedor / ranking de quem recebeu mais → get_expenses query="por fornecedor" OU get_supplier_report  [portal: /financeiro-conta-pagar-fornecedor]
- Estoque / inventário → get_stock  [portal: #/estoque/inventario]
- Ficha técnica de prato → get_recipe
- Lucratividade de fichas → analyze_recipes_profitability
- Compras recebidas / notas de entrada → get_inbound_purchases  [portal: #/estoque/entrada-mercadoria]
- Dashboard compras → get_inbound_purchases + get_purchasing_plan  [portal: #/dashboard/compra]
- Cenário geral / dashboard resumo → get_scenario  [portal: #/dashboard/resumo]
- Consumo de insumo → get_ingredient_consumption
- Auditoria de insumo ou fornecedor → get_audit
- Plano de compras → get_purchasing_plan
- Desperdício / antifurto → get_waste_audit
- Engenharia de cardápio / Matriz BCG → get_menu_engineering
- Inflação de fornecedores → get_supplier_inflation
- Projeção de fluxo de caixa → get_cashflow_runway
- Clima → get_weather_forecast
- Conciliação de NF-e → get_invoice_reconciliation
- Fechamento / quebra / conciliação de caixa → get_cashier_closure_report  [portal: #/financeiro/conciliacao]
- Briefing diário → get_daily_briefing
- Simular mudança de preço → simulate_price_change
- CMV / markup / margem → get_cmv_report
- Ranking semanal 3 casas → get_weekly_ranking
- Metas mensais → get_revenue_tracker
- Dossiê de produto (NCM) → get_product_specs
- Giro de estoque / capital empatado → get_inventory_turnover
- Cancelamentos → get_cancellation_report
- Ranking de fornecedores → get_supplier_report
- Comissões de garçons → get_commission_report
- Quebra de caixa → get_cashier_closure_report
- Fiscal / NFC-e → get_fiscal_report
- Break-even → get_break_even_analysis
- Raio-X financeiro → get_financial_snapshot
- Auditoria de cadastro → audit_product_registration
- Alterar preço no ERP → apply_price_change
- Avaliações de uma casa (Google + cancelamentos + faturamento) → get_customer_success_report
- Painel de avaliações das 3 casas → get_reviews_consolidated
- DRE gerencial → get_dre_report
- Balancete / saldos contábeis → get_balancete  [portal: #/relatorio/balancete]
- Fraude em tempo real (hoje) → get_realtime_fraud_alert
- Auditoria cruzada completa / desvios / pagamentos injustificados / fraude operacional → get_complete_audit
- Precificação dinâmica → get_dynamic_pricing_suggestions
- Escala de RH → get_predictive_hr_scale
- Economia fiscal PIS/COFINS → get_tax_combo_suggestions

Se período não especificado → assuma hoje ({today}), reafirme na resposta.
Se pedir as 3 casas → chame get_scenario para cada uma separadamente.
Auditoria Completa → SEMPRE usar get_complete_audit (não substituir por get_scenario).

════════════════════════════════════════
📋 PROTOCOLO DE RESPOSTA COM DADOS
════════════════════════════════════════

**Passo 1 — ÂNCORA DE DADOS (obrigatório, antes de qualquer análise):**
Comece listando os dados exatos recebidos das ferramentas:
> "📋 *Dados do sistema ({today}):*"
> "• Faturamento: R$ [valor exato da ferramenta]"
> "• Produtos: [nomes exatos como vieram]"
> "• [campo]: [valor exato] | [campo ausente]: _não disponível_"

**Passo 2 — ANÁLISE (só sobre o que está na Âncora):**
📍 **Cenário Factual** — transcreva os dados da âncora com contexto operacional. Zero inferência.
✅ **Alavancas de Lucro** — o que está performando bem, com valores nominais exatos da ferramenta.
🚨 **Red Flags** — anomalias reais nos dados. Só cite se o dado existe na ferramenta.

**Passo 3 — PLANO DE AÇÃO (baseado exclusivamente nos dados reais):**
💡 **Plano de Ação CEO** — mínimo 3 ações. Cada ação deve citar o dado real que a justifica.
✓ "Heineken: custo subiu de R$ 8,50 para R$ 9,20 na última nota [dado da ferramenta]. Preço de venda atual R$ 12,00 não cobre markup 2,5x. Ajustar para R$ 13,00 no ERP hoje."
✗ "Revisar preços" — proibido. Sem dado = sem ação.

════════════════════════════════════════
📐 REGRAS DE FORMATO
════════════════════════════════════════
- Números BRL: R$ 1.234,50 — NUNCA R$ 1,234.50
- Percentuais: 34,5% — NUNCA 34.5%
- Datas: DD/MM/AAAA
- Se a ferramenta retornou erro ou lista vazia: "Não encontrei dados no sistema para esta consulta no período informado. Verifique se há dados sincronizados (/sync)."
- NUNCA use expressões como "aproximadamente", "cerca de", "estimado em" para dados que deveriam vir da ferramenta."""

def process_ceo_question(user_message, current_restaurant="Nauan Beach Club", chat_id=None):
    print(f"[IA] Processando pergunta: {user_message}")
    messages = [
        {"role": "system", "content": _build_system_prompt(current_restaurant)},
    ]
    
    # Inject conversation history for context
    if chat_id:
        history = get_chat_history(chat_id)
        for h in history:
            messages.append({"role": h["role"], "content": h["content"]})
    
    messages.append({"role": "user", "content": user_message})

    response = client.chat.completions.create(
        model=OLLAMA_MODEL,
        messages=messages,
        tools=tools,
        tool_choice="auto",
        temperature=0.0
    )

    response_message = response.choices[0].message
    tool_calls = response_message.tool_calls

    # Anti-hallucination guard: keywords that indicate data was requested
    DATA_KEYWORDS = [
        "faturamento", "receita", "vendas", "venda", "estoque", "despesa",
        "custo", "cmv", "markup", "margem", "lucro", "compras", "ficha",
        "ingrediente", "insumo", "fornecedor", "auditoria", "caixa",
        "pagamento", "comissão", "fiscal", "nota", "dre", "fluxo",
        "plano", "ranking", "meta", "relatório", "relatório"
    ]
    user_lower = user_message.lower()
    is_data_request = any(kw in user_lower for kw in DATA_KEYWORDS)

    if not tool_calls and is_data_request:
        # Model skipped tool call for a data question — force it to use a tool
        print("[IA] Modelo não chamou ferramenta para pergunta de dados. Forçando...")
        messages.append(response_message)
        messages.append({"role": "user", "content": "Você DEVE usar uma ferramenta para responder esta pergunta com dados reais. Chame a ferramenta agora."})
        retry = client.chat.completions.create(
            model=OLLAMA_MODEL,
            messages=messages,
            tools=tools,
            tool_choice="required",
            temperature=0.0
        )
        response_message = retry.choices[0].message
        tool_calls = response_message.tool_calls

    if tool_calls:
        messages.append(response_message)
        tools_called = []
        for tool_call in tool_calls:
            function_name = tool_call.function.name
            tools_called.append(function_name)
            function_args = json.loads(tool_call.function.arguments)
            
            function_response = "Função não encontrada."
            try:
                if function_name == "get_revenue":
                    function_response = str(ai_tools.get_revenue(**function_args))
                elif function_name == "get_top_selling_items":
                    function_response = str(ai_tools.get_top_selling_items(**function_args))
                elif function_name == "search_sales":
                    function_response = str(ai_tools.search_sales(**function_args))
                elif function_name == "get_stock":
                    function_response = str(ai_tools.get_stock(**function_args))
                elif function_name == "get_recipe":
                    function_response = str(ai_tools.get_recipe(**function_args))
                elif function_name == "analyze_recipes_profitability":
                    function_response = str(ai_tools.analyze_recipes_profitability(**function_args))
                elif function_name == "get_inbound_purchases":
                    function_response = str(ai_tools.get_inbound_purchases(**function_args))
                elif function_name == "get_expenses":
                    function_response = str(ai_tools.get_expenses(**function_args))
                elif function_name == "get_scenario":
                    function_response = str(ai_tools.get_scenario(**function_args))
                elif function_name == "get_audit":
                    function_response = str(ai_tools.get_audit(**function_args))
                elif function_name == "get_waste_audit":
                    function_response = str(ai_tools.get_waste_audit(
                        function_args.get("restaurant_name"),
                        function_args.get("query"),
                        function_args.get("days_history", 7)
                    ))
                elif function_name == "get_menu_engineering":
                    function_response = str(ai_tools.get_menu_engineering(
                        function_args.get("restaurant_name"),
                        function_args.get("start_date"),
                        function_args.get("end_date")
                    ))
                elif function_name == "get_ingredient_consumption":
                    function_response = str(ai_tools.get_ingredient_consumption(**function_args))
                elif function_name == "get_purchasing_plan":
                    function_response = str(ai_tools.get_purchasing_plan(**function_args))
                elif function_name == "get_supplier_inflation":
                    function_response = str(ai_tools.get_supplier_inflation(**function_args))
                elif function_name == "get_cashflow_runway":
                    function_response = str(ai_tools.get_cashflow_runway(**function_args))
                elif function_name == "get_weather_forecast":
                    function_response = str(ai_tools.get_weather_forecast(**function_args))
                elif function_name == "get_invoice_reconciliation":
                    function_response = str(ai_tools.get_invoice_reconciliation(**function_args))
                elif function_name == "get_complete_audit":
                    function_response = str(ai_tools.get_complete_audit(**function_args))
                elif function_name == "get_daily_briefing":
                    function_response = str(ai_tools.get_daily_briefing())
                elif function_name == "get_weekly_ranking":
                    function_response = str(ai_tools.get_weekly_ranking())
                elif function_name == "get_revenue_tracker":
                    function_response = str(ai_tools.get_revenue_tracker(**function_args))
                elif function_name == "get_product_specs":
                    function_response = str(ai_tools.get_product_specs(**function_args))
                elif function_name == "get_inventory_turnover":
                    function_response = str(ai_tools.get_inventory_turnover(**function_args))
                elif function_name == "get_cancellation_report":
                    function_response = str(ai_tools.get_cancellation_report(**function_args))
                elif function_name == "get_supplier_report":
                    function_response = str(ai_tools.get_supplier_report(**function_args))
                elif function_name == "get_commission_report":
                    function_response = str(ai_tools.get_commission_report(**function_args))
                elif function_name == "get_payment_report":
                    function_response = str(ai_tools.get_payment_report(**function_args))
                elif function_name == "get_cashier_closure_report":
                    function_response = str(ai_tools.get_cashier_closure_report(**function_args))
                elif function_name == "get_fiscal_report":
                    function_response = str(ai_tools.get_fiscal_report(**function_args))
                elif function_name == "get_break_even_analysis":
                    function_response = str(ai_tools.get_break_even_analysis(**function_args))
                elif function_name == "get_financial_snapshot":
                    function_response = str(ai_tools.get_financial_snapshot(**function_args))
                elif function_name == "audit_product_registration":
                    function_response = str(ai_tools.audit_product_registration(**function_args))
                elif function_name == "apply_price_change":
                    function_response = str(ai_tools.apply_price_change(**function_args))
                elif function_name == "get_customer_success_report":
                    function_response = str(ai_tools.get_customer_success_report(
                        restaurant_name=function_args.get("restaurant_name")
                    ))
                elif function_name == "get_dynamic_pricing_suggestions":
                    function_response = str(ai_tools.get_dynamic_pricing_suggestions(**function_args))
                elif function_name == "get_predictive_hr_scale":
                    function_response = str(ai_tools.get_predictive_hr_scale(**function_args))
                elif function_name == "get_reviews_consolidated":
                    function_response = str(ai_tools.get_reviews_consolidated())
                elif function_name == "save_inventory_snapshot":
                    function_response = str(ai_tools.save_inventory_snapshot(**function_args))
                elif function_name == "get_dre_report":
                    function_response = str(ai_tools.get_dre_report(**function_args))
                elif function_name == "get_balancete":
                    function_response = str(ai_tools.get_balancete(**function_args))
                elif function_name == "get_realtime_fraud_alert":
                    function_response = str(ai_tools.get_realtime_fraud_alert(
                        restaurant_name=function_args.get("restaurant_name")
                    ))

                elif function_name == "get_tax_combo_suggestions":
                    function_response = str(ai_tools.get_tax_combo_suggestions(**function_args))
                elif function_name == "simulate_price_change":
                    function_response = str(ai_tools.simulate_price_change(**function_args))
                elif function_name == "get_cmv_report":
                    function_response = str(ai_tools.get_cmv_report(**function_args))
            except Exception as e:
                function_response = f"Erro ao executar a ferramenta: {e}"
            
            messages.append(
                {
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": function_name,
                    "content": function_response,
                }
            )
        
        analysis_prompt = _build_analysis_prompt(tools_called)
        messages.append({"role": "user", "content": analysis_prompt})

        second_response = client.chat.completions.create(
            model=OLLAMA_MODEL,
            messages=messages,
            temperature=0.0
        )
        result = second_response.choices[0].message.content
        
        # Save to memory
        if chat_id:
            add_to_history(chat_id, "user", user_message)
            add_to_history(chat_id, "assistant", result)
        
        return result
        
    result = response_message.content
    
    # Save to memory
    if chat_id:
        add_to_history(chat_id, "user", user_message)
        add_to_history(chat_id, "assistant", result)
    
    return result

if __name__ == "__main__":
    print(process_ceo_question("Qual foi o faturamento ontem do Nauan? Quantos Day Use vendemos?"))
