import os
import time
import sqlite3
import logging
from typing import List, Dict, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import google.generativeai as genai
import pandas as pd
import schedule
import random
from mailgun.client import Client as MailgunClient
from twilio.rest import Client as TwilioClient
import telegram

# Configuração inicial
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sales_agent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuração do Gemini
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY não encontrada no arquivo .env")

genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-pro')

# Configurações do sistema
CONFIG = {
    'database_file': 'leads.db',
    'max_leads_per_run': 50,
    'min_time_between_messages': 30,  # em segundos
    'max_messages_per_day': 100,
    'affiliate_links': {
        'produto1': os.getenv('AFFILIATE_LINK_1'),
        'produto2': os.getenv('AFFILIATE_LINK_2'),
    },
    'message_templates': {
        'email': """
        Olá {nome},
        
        Descobrimos que você pode se interessar por {produto}. Como especialista na área, 
        queria compartilhar essa oportunidade exclusiva com você.
        
        {produto} pode ajudar você a {beneficio}.
        
        Confira agora mesmo: {link_afiliado}
        
        Atenciosamente,
        Equipe de Recomendações
        """,
        'whatsapp': """
        Olá {nome}, tudo bem?
        
        Vi que você tem interesse em {tema} e pensei no {produto} pra você. 
        Ele ajuda pessoas como você a {beneficio}.
        
        Dá uma olhada aqui: {link_afiliado}
        
        Se quiser mais informações, é só responder essa mensagem!
        """
    }
}

# Inicialização do banco de dados SQLite
def init_database():
    conn = sqlite3.connect(CONFIG['database_file'])
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT,
        email TEXT,
        telefone TEXT,
        origem TEXT,
        interesses TEXT,
        data_coleta TEXT,
        status TEXT DEFAULT 'novo',
        ultimo_contato TEXT,
        tentativas INTEGER DEFAULT 0
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mensagens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER,
        canal TEXT,
        conteudo TEXT,
        data_envio TEXT,
        status TEXT,
        FOREIGN KEY (lead_id) REFERENCES leads (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS conversoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER,
        produto TEXT,
        valor REAL,
        data_conversao TEXT,
        comissao REAL,
        FOREIGN KEY (lead_id) REFERENCES leads (id)
    )
    ''')
    
    conn.commit()
    return conn

# Classe principal do coletor de leads
class LeadCollector:
    def __init__(self):
        self.driver = self._setup_selenium()
        self.db_conn = init_database()
        
    def _setup_selenium(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    
    def collect_from_website(self, url: str, selectors: Dict):
        logger.info(f"Coletando leads do website: {url}")
        
        try:
            self.driver.get(url)
            time.sleep(3)
            
            leads = []
            elements = self.driver.find_elements(By.CSS_SELECTOR, selectors['container'])
            
            for element in elements[:CONFIG['max_leads_per_run']]:
                try:
                    nome = element.find_element(By.CSS_SELECTOR, selectors['nome']).text
                    email = element.find_element(By.CSS_SELECTOR, selectors['email']).text
                    telefone = element.find_element(By.CSS_SELECTOR, selectors['telefone']).text if 'telefone' in selectors else None
                    
                    lead = {
                        'nome': nome,
                        'email': email,
                        'telefone': telefone,
                        'origem': url,
                        'interesses': self._detect_interests(nome, email)
                    }
                    
                    leads.append(lead)
                except Exception as e:
                    logger.warning(f"Erro ao processar elemento: {str(e)}")
                    continue
            
            return leads
        except Exception as e:
            logger.error(f"Erro ao coletar leads do website {url}: {str(e)}")
            return []
    
    def collect_from_api(self, api_url: str, params: Dict):
        logger.info(f"Coletando leads da API: {api_url}")
        
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            leads = []
            for item in data[:CONFIG['max_leads_per_run']]:
                lead = {
                    'nome': item.get('name', ''),
                    'email': item.get('email', ''),
                    'telefone': item.get('phone', ''),
                    'origem': api_url,
                    'interesses': self._detect_interests(item.get('name', ''), item.get('email', ''))
                }
                leads.append(lead)
            
            return leads
        except Exception as e:
            logger.error(f"Erro ao coletar leads da API {api_url}: {str(e)}")
            return []
    
    def _detect_interests(self, nome: str, email: str) -> str:
        try:
            prompt = f"""
            Com base no nome '{nome}' e email '{email}', sugira possíveis interesses 
            para marketing de afiliados. Retorne apenas palavras-chave separadas por vírgula.
            """
            
            response = gemini_model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logger.warning(f"Erro ao detectar interesses com Gemini: {str(e)}")
            return "geral"
    
    def save_leads(self, leads: List[Dict]):
        cursor = self.db_conn.cursor()
        
        for lead in leads:
            cursor.execute(
                "SELECT id FROM leads WHERE email = ? OR telefone = ?",
                (lead['email'], lead['telefone'])
            )
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(
                    """
                    INSERT INTO leads (nome, email, telefone, origem, interesses, data_coleta)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        lead['nome'],
                        lead['email'],
                        lead['telefone'],
                        lead['origem'],
                        lead['interesses'],
                        datetime.now().isoformat()
                    )
                )
                logger.info(f"Novo lead adicionado: {lead['email']}")
        
        self.db_conn.commit()
    
    def close(self):
        self.driver.quit()
        self.db_conn.close()

# Classe para geração de mensagens personalizadas
class MessageGenerator:
    @staticmethod
    def generate_persuasive_message(lead: Dict, produto: str, canal: str) -> str:
        try:
            template = CONFIG['message_templates'].get(canal, '')
            beneficios = MessageGenerator._generate_benefits(lead['interesses'], produto)
            
            link_afiliado = CONFIG['affiliate_links'].get(produto, '')
            if isinstance(link_afiliado, list):
                link_afiliado = random.choice(link_afiliado)
            
            message = template.format(
                nome=lead['nome'].split()[0] if lead['nome'] else 'cliente',
                produto=produto,
                beneficio=beneficios,
                link_afiliado=link_afiliado,
                tema=lead['interesses'].split(',')[0] if lead['interesses'] else 'este assunto'
            )
            
            return message
        except Exception as e:
            logger.error(f"Erro ao gerar mensagem: {str(e)}")
            return ""
    
    @staticmethod
    def _generate_benefits(interesses: str, produto: str) -> str:
        try:
            prompt = f"""
            Gere 3 benefícios convincentes do produto '{produto}' para alguém interessado em: {interesses}.
            Retorne uma única frase persuasiva com os benefícios.
            """
            
            response = gemini_model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logger.warning(f"Erro ao gerar benefícios com Gemini: {str(e)}")
            return f"beneficiar com {produto}"

# Classe para envio de mensagens
class MessageSender:
    def __init__(self):
        self.db_conn = init_database()
        self.sent_today = 0
        
        # Configurações de APIs reais
        self.mailgun_client = MailgunClient(
            api_key=os.getenv('MAILGUN_API_KEY'),
            domain=os.getenv('MAILGUN_DOMAIN')
        )
        
        self.twilio_client = TwilioClient(
            os.getenv('TWILIO_ACCOUNT_SID'),
            os.getenv('TWILIO_AUTH_TOKEN')
        )
        
        self.telegram_bot = telegram.Bot(token=os.getenv('TELEGRAM_BOT_TOKEN'))
    
    def send_messages(self):
        if self.sent_today >= CONFIG['max_messages_per_day']:
            logger.info("Limite diário de mensagens atingido.")
            return
        
        leads = self._get_qualified_leads()
        logger.info(f"Enviando mensagens para {len(leads)} leads qualificados")
        
        for lead in leads:
            if self.sent_today >= CONFIG['max_messages_per_day']:
                break
            
            produto = self._select_product_for_lead(lead)
            if not produto:
                continue
            
            canal = self._select_channel_for_lead(lead)
            mensagem = MessageGenerator.generate_persuasive_message(lead, produto, canal)
            
            if not mensagem:
                continue
            
            status = "enviado" if self._send_message(lead, canal, mensagem) else "falha"
            self._record_message(lead['id'], canal, mensagem, status)
            
            if status == "enviado":
                self.sent_today += 1
                self._update_lead_status(lead['id'])
            
            time.sleep(CONFIG['min_time_between_messages'])
    
    def _get_qualified_leads(self) -> List[Dict]:
        cursor = self.db_conn.cursor()
        
        cursor.execute('''
        SELECT id, nome, email, telefone, interesses 
        FROM leads 
        WHERE status = 'novo' OR (status = 'contatado' AND tentativas < 3)
        ORDER BY data_coleta ASC
        LIMIT ?
        ''', (CONFIG['max_messages_per_day'] - self.sent_today,))
        
        columns = [desc[0] for desc in cursor.description]
        leads = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return leads
    
    def _select_product_for_lead(self, lead: Dict) -> Optional[str]:
        interesses = lead['interesses'].lower() if lead['interesses'] else ''
        
        if 'marketing' in interesses or 'vendas' in interesses:
            return 'produto1'
        elif 'investimento' in interesses or 'dinheiro' in interesses:
            return 'produto2'
        elif 'empreendedor' in interesses or 'negócio' in interesses:
            return random.choice(['produto1', 'produto2'])
        
        return None
    
    def _select_channel_for_lead(self, lead: Dict) -> str:
        if lead['telefone']:
            return random.choice(['whatsapp', 'telegram'])
        elif lead['email']:
            return 'email'
        return 'email'
    
    def _send_message(self, lead: Dict, canal: str, mensagem: str) -> bool:
        try:
            if canal == 'email':
                return self._send_email(lead['email'], mensagem)
            elif canal == 'whatsapp':
                return self._send_whatsapp(lead['telefone'], mensagem)
            elif canal == 'telegram':
                return self._send_telegram(lead['telefone'], mensagem)
            return False
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem via {canal}: {str(e)}")
            return False
    
    def _send_email(self, email: str, mensagem: str) -> bool:
        try:
            response = self.mailgun_client.send_email(
                from_email="contato@seudominio.com",
                to_email=email,
                subject="Oportunidade exclusiva para você",
                text=mensagem
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Erro ao enviar email para {email}: {str(e)}")
            return False
    
    def _send_whatsapp(self, telefone: str, mensagem: str) -> bool:
        try:
            message = self.twilio_client.messages.create(
                body=mensagem,
                from_=f"whatsapp:{os.getenv('TWILIO_WHATSAPP_NUMBER')}",
                to=f"whatsapp:{telefone}"
            )
            return message.sid is not None
        except Exception as e:
            logger.error(f"Erro ao enviar WhatsApp para {telefone}: {str(e)}")
            return False
    
    def _send_telegram(self, telefone: str, mensagem: str) -> bool:
        try:
            # Primeiro, formatamos o número de telefone para o padrão internacional
            telefone_formatado = self._formatar_telefone_telegram(telefone)
            if not telefone_formatado:
                logger.error(f"Número de telefone inválido para Telegram: {telefone}")
                return False

            # Verificamos se já temos o chat_id desse número no banco de dados
            chat_id = self._get_telegram_chat_id(telefone_formatado)
            
            if chat_id:
                # Se já temos o chat_id, enviamos a mensagem diretamente
                self.telegram_bot.send_message(chat_id=chat_id, text=mensagem)
                return True
            else:
                # Se não temos o chat_id, precisamos iniciar uma conversa
                return self._iniciar_conversa_telegram(telefone_formatado, mensagem)
                
        except Exception as e:
            logger.error(f"Erro ao enviar Telegram para {telefone}: {str(e)}")
            return False

    def _formatar_telefone_telegram(self, telefone: str) -> Optional[str]:
        """Formata o número de telefone para o padrão internacional (+5511999999999)"""
        try:
            # Remove todos os caracteres não numéricos
            numeros = ''.join(filter(str.isdigit, telefone))
            
            # Verifica se o número tem um código de país
            if not numeros.startswith('55') and len(numeros) == 11:  # Assume Brasil se não tiver código
                numeros = '55' + numeros
                
            # Adiciona o '+' no início
            return f"+{numeros}"
        except Exception as e:
            logger.error(f"Erro ao formatar telefone {telefone}: {str(e)}")
            return None

    def _iniciar_conversa_telegram(self, telefone: str, mensagem: str) -> bool:
        """Inicia uma nova conversa no Telegram via deep link"""
        try:
            # Cria um deep link do Telegram
            deep_link = f"https://t.me/{os.getenv('TELEGRAM_BOT_USERNAME')}?start=contato_{telefone[1:]}"
            
            # Primeiro envia uma mensagem via WhatsApp ou SMS com o link de convite
            mensagem_convite = (
                f"Olá! Temos uma mensagem importante para você no Telegram. "
                f"Clique neste link para iniciar a conversa: {deep_link}"
            )
            
            # Tenta enviar via WhatsApp primeiro
            if self._send_whatsapp(telefone, mensagem_convite):
                logger.info(f"Convite Telegram enviado via WhatsApp para {telefone}")
                return True
            else:
                # Se falhar, tenta enviar via SMS
                if self._send_sms(telefone, mensagem_convite):
                    logger.info(f"Convite Telegram enviado via SMS para {telefone}")
                    return True
            
            logger.error(f"Não foi possível enviar convite do Telegram para {telefone}")
            return False
        except Exception as e:
            logger.error(f"Erro ao iniciar conversa no Telegram: {str(e)}")
            return False

    def _send_sms(self, telefone: str, mensagem: str) -> bool:
        """Envia SMS usando a API da Twilio"""
        try:
            message = self.twilio_client.messages.create(
                body=mensagem,
                from_=os.getenv('TWILIO_PHONE_NUMBER'),
                to=telefone
            )
            return message.sid is not None
        except Exception as e:
            logger.error(f"Erro ao enviar SMS para {telefone}: {str(e)}")
            return False

    def _get_telegram_chat_id(self, telefone: str) -> Optional[int]:
        """Obtém o chat_id do Telegram para um número de telefone"""
        try:
            # Verifica no banco de dados primeiro
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT telegram_chat_id FROM leads WHERE telefone = ?", (telefone,))
            result = cursor.fetchone()
            
            if result and result[0]:
                return int(result[0])
                
            # Se não encontrou no banco, tenta verificar via API do bot
            updates = self.telegram_bot.get_updates()
            for update in updates:
                if update.message and update.message.contact:
                    if update.message.contact.phone_number == telefone:
                        chat_id = update.message.chat.id
                        # Salva no banco de dados para uso futuro
                        cursor.execute(
                            "UPDATE leads SET telegram_chat_id = ? WHERE telefone = ?",
                            (str(chat_id), telefone)
                        )
                        self.db_conn.commit()
                        return chat_id
            
            return None
        except Exception as e:
            logger.error(f"Erro ao obter chat_id do Telegram: {str(e)}")
            return None
    
    def _record_message(self, lead_id: int, canal: str, mensagem: str, status: str):
        cursor = self.db_conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO mensagens (lead_id, canal, conteudo, data_envio, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                lead_id,
                canal,
                mensagem,
                datetime.now().isoformat(),
                status
            )
        )
        
        if status == "enviado":
            cursor.execute(
                "UPDATE leads SET status = 'contatado', ultimo_contato = ?, tentativas = tentativas + 1 WHERE id = ?",
                (datetime.now().isoformat(), lead_id)
            )
        
        self.db_conn.commit()
    
    def _update_lead_status(self, lead_id: int):
        cursor = self.db_conn.cursor()
        cursor.execute(
            "UPDATE leads SET status = 'contatado', ultimo_contato = ? WHERE id = ?",
            (datetime.now().isoformat(), lead_id)
        )
        self.db_conn.commit()

# Classe para monitoramento de conversões
class ConversionMonitor:
    def __init__(self):
        self.db_conn = init_database()
    
    def check_conversions(self):
        logger.info("Verificando conversões nas plataformas de afiliados")
        
        # Hotmart
        hotmart_conversions = self._check_hotmart_conversions()
        for conv in hotmart_conversions:
            self._record_conversion(conv)
        
        # Monetizze
        monetizze_conversions = self._check_monetizze_conversions()
        for conv in monetizze_conversions:
            self._record_conversion(conv)
        
        # Eduzz
        eduzz_conversions = self._check_eduzz_conversions()
        for conv in eduzz_conversions:
            self._record_conversion(conv)
    
    def _check_hotmart_conversions(self) -> List[Dict]:
        try:
            url = "https://api-hotmart-com-br.affiliates.afilias.com.br/v1/affiliates/sales"
            headers = {
                "Authorization": f"Bearer {os.getenv('HOTMART_API_KEY')}",
                "Content-Type": "application/json"
            }
            params = {
                "start_date": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                "end_date": datetime.now().strftime('%Y-%m-%d')
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            conversions = []
            for sale in data.get('sales', []):
                lead_id = self._find_lead_by_email(sale['buyer']['email'])
                if lead_id:
                    conversions.append({
                        'lead_id': lead_id,
                        'produto': sale['product']['name'],
                        'valor': float(sale['price']['value']),
                        'comissao': float(sale['commission']['value'])
                    })
            
            return conversions
        except Exception as e:
            logger.error(f"Erro ao verificar conversões na Hotmart: {str(e)}")
            return []
    
    def _check_monetizze_conversions(self) -> List[Dict]:
        try:
            url = "https://api.monetizze.com.br/2.1/transactions"
            headers = {
                "Authorization": f"Token {os.getenv('MONETIZZE_API_KEY')}",
                "Content-Type": "application/json"
            }
            params = {
                "start_date": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                "end_date": datetime.now().strftime('%Y-%m-%d'),
                "status": "approved"
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            conversions = []
            for transaction in data.get('transactions', []):
                lead_id = self._find_lead_by_email(transaction['customer']['email'])
                if lead_id:
                    conversions.append({
                        'lead_id': lead_id,
                        'produto': transaction['product']['name'],
                        'valor': float(transaction['price']),
                        'comissao': float(transaction['commission_value'])
                    })
            
            return conversions
        except Exception as e:
            logger.error(f"Erro ao verificar conversões na Monetizze: {str(e)}")
            return []
    
    def _check_eduzz_conversions(self) -> List[Dict]:
        try:
            url = "https://api.eduzz.com/sale"
            headers = {
                "Authorization": f"Bearer {os.getenv('EDUZZ_API_KEY')}",
                "Content-Type": "application/json"
            }
            params = {
                "start_date": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                "end_date": datetime.now().strftime('%Y-%m-%d'),
                "status": "complete"
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            conversions = []
            for sale in data.get('data', []):
                lead_id = self._find_lead_by_email(sale['customer']['email'])
                if lead_id:
                    conversions.append({
                        'lead_id': lead_id,
                        'produto': sale['product']['name'],
                        'valor': float(sale['amount']),
                        'comissao': float(sale['commission'])
                    })
            
            return conversions
        except Exception as e:
            logger.error(f"Erro ao verificar conversões na Eduzz: {str(e)}")
            return []
    
    def _find_lead_by_email(self, email: str) -> Optional[int]:
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT id FROM leads WHERE email = ?", (email,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _record_conversion(self, conversion: Dict):
        cursor = self.db_conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO conversoes (lead_id, produto, valor, data_conversao, comissao)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                conversion['lead_id'],
                conversion['produto'],
                conversion['valor'],
                datetime.now().isoformat(),
                conversion['comissao']
            )
        )
        
        cursor.execute(
            "UPDATE leads SET status = 'convertido' WHERE id = ?",
            (conversion['lead_id'],)
        )
        
        self.db_conn.commit()
        logger.info(f"Conversão registrada para o lead {conversion['lead_id']} - Comissão: R${conversion['comissao']:.2f}")
        
class ReportGenerator:
    def __init__(self):
        self.db_conn = init_database()
    
    def generate_daily_report(self):
        """Gera um relatório diário de performance"""
        logger.info("Gerando relatório diário")
        
        report = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'leads_collected': self._get_leads_collected(),
            'messages_sent': self._get_messages_sent(),
            'conversions': self._get_conversions(),
            'revenue': self._get_total_revenue(),
            'top_performing_products': self._get_top_products(),
            'suggestions': self._generate_suggestions()
        }
        
        self._save_report(report)
        self._display_report(report)
        
        return report
    
    def _get_leads_collected(self) -> int:
        """Retorna o número de leads coletados hoje"""
        cursor = self.db_conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("SELECT COUNT(*) FROM leads WHERE date(data_coleta) = ?", (today,))
        return cursor.fetchone()[0]
    
    def _get_messages_sent(self) -> int:
        """Retorna o número de mensagens enviadas hoje"""
        cursor = self.db_conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("SELECT COUNT(*) FROM mensagens WHERE date(data_envio) = ? AND status = 'enviado'", (today,))
        return cursor.fetchone()[0]
    
    def _get_conversions(self) -> int:
        """Retorna o número de conversões hoje"""
        cursor = self.db_conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("SELECT COUNT(*) FROM conversoes WHERE date(data_conversao) = ?", (today,))
        return cursor.fetchone()[0]
    
    def _get_total_revenue(self) -> float:
        """Retorna o total de comissões hoje"""
        cursor = self.db_conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("SELECT SUM(comissao) FROM conversoes WHERE date(data_conversao) = ?", (today,))
        result = cursor.fetchone()[0]
        return result if result else 0.0
    
    def _get_top_products(self) -> List[Dict]:
        """Retorna os produtos mais vendidos"""
        cursor = self.db_conn.cursor()
        cursor.execute('''
        SELECT produto, COUNT(*) as vendas, SUM(comissao) as receita 
        FROM conversoes 
        GROUP BY produto 
        ORDER BY receita DESC 
        LIMIT 3
        ''')
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def _generate_suggestions(self) -> str:
        """Usa o Gemini para gerar sugestões de melhoria"""
        report_data = {
            'leads_collected': self._get_leads_collected(),
            'messages_sent': self._get_messages_sent(),
            'conversions': self._get_conversions(),
            'conversion_rate': self._get_conversion_rate(),
            'top_products': self._get_top_products()
        }
        
        try:
            prompt = f"""
            Com base nos seguintes dados de desempenho de um agente de vendas de afiliados, 
            gere 3 sugestões concisas para melhorar os resultados. Seja específico e acionável.
            
            Dados:
            - Leads coletados hoje: {report_data['leads_collected']}
            - Mensagens enviadas hoje: {report_data['messages_sent']}
            - Conversões hoje: {report_data['conversions']}
            - Taxa de conversão: {report_data['conversion_rate']:.2f}%
            - Produtos mais vendidos: {', '.join([p['produto'] for p in report_data['top_products']])}
            
            Sugestões:
            1. 
            2. 
            3. 
            """
            
            response = gemini_model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"Erro ao gerar sugestões com Gemini: {str(e)}")
            return "Não foi possível gerar sugestões automáticas."
    
    def _get_conversion_rate(self) -> float:
        """Calcula a taxa de conversão"""
        messages_sent = self._get_messages_sent()
        conversions = self._get_conversions()
        
        if messages_sent == 0:
            return 0.0
        return (conversions / messages_sent) * 100
    
    def _save_report(self, report: Dict):
        """Salva o relatório em um arquivo"""
        filename = f"relatorio_{report['date']}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Relatório Diário - {report['date']}\n")
            f.write("="*40 + "\n")
            f.write(f"Leads coletados: {report['leads_collected']}\n")
            f.write(f"Mensagens enviadas: {report['messages_sent']}\n")
            f.write(f"Conversões: {report['conversions']}\n")
            f.write(f"Receita total: R${report['revenue']:.2f}\n")
            f.write(f"Taxa de conversão: {report['conversion_rate']:.2f}%\n\n")
            
            f.write("Produtos mais vendidos:\n")
            for prod in report['top_performing_products']:
                f.write(f"- {prod['produto']}: {prod['vendas']} vendas (R${prod['receita']:.2f})\n")
            
            f.write("\nSugestões de melhoria:\n")
            f.write(report['suggestions'] + "\n")
    
    def _display_report(self, report: Dict):
        """Exibe o relatório no console"""
        print(f"\n{'='*40}")
        print(f"Relatório Diário - {report['date']}")
        print(f"{'='*40}")
        print(f"Leads coletados: {report['leads_collected']}")
        print(f"Mensagens enviadas: {report['messages_sent']}")
        print(f"Conversões: {report['conversions']}")
        print(f"Receita total: R${report['revenue']:.2f}")
        print(f"Taxa de conversão: {report['conversion_rate']:.2f}%")
        
        print("\nProdutos mais vendidos:")
        for prod in report['top_performing_products']:
            print(f"- {prod['produto']}: {prod['vendas']} vendas (R${prod['receita']:.2f})")
        
        print("\nSugestões de melhoria:")
        print(report['suggestions'])
        print("="*40 + "\n")


class AutonomousSalesAgent:
    def __init__(self):
        self.lead_collector = LeadCollector()
        self.message_sender = MessageSender()
        self.conversion_monitor = ConversionMonitor()
        self.report_generator = ReportGenerator()  # Agora corretamente inicializado
        self._setup_scheduler()
    
    def _setup_scheduler(self):
        """Configura as tarefas agendadas"""
        schedule.every().day.at("09:00").do(self.collect_leads)
        schedule.every().day.at("11:00").do(self.send_messages)
        schedule.every().day.at("14:00").do(self.send_messages)
        schedule.every().day.at("17:00").do(self.check_conversions)
        schedule.every().day.at("18:00").do(self.generate_report)
    
    def collect_leads(self):
        """Coleta leads de várias fontes"""
        logger.info("Iniciando coleta de leads")
        
        # Coleta de um website de exemplo (substitua pelos seus alvos)
        website_leads = self.lead_collector.collect_from_website(
            url="https://exemplo.com/leads",
            selectors={
                'container': '.lead-item',
                'nome': '.name',
                'email': '.email',
                'telefone': '.phone'
            }
        )
        
        # Coleta de uma API de exemplo (substitua pela sua API real)
        api_leads = self.lead_collector.collect_from_api(
            api_url="https://api.exemplo.com/leads",
            params={'limit': 10, 'status': 'active'}
        )
        
        # Salva todos os leads coletados
        all_leads = website_leads + api_leads
        self.lead_collector.save_leads(all_leads)
        
        logger.info(f"Coleta concluída. {len(all_leads)} novos leads adicionados")
    
    def send_messages(self):
        """Envia mensagens para leads"""
        logger.info("Iniciando envio de mensagens")
        self.message_sender.send_messages()
        logger.info("Envio de mensagens concluído")
    
    def check_conversions(self):
        """Verifica conversões"""
        logger.info("Verificando conversões")
        self.conversion_monitor.check_conversions()
        logger.info("Verificação de conversões concluída")
    
    def generate_report(self):
        """Gera relatório de performance"""
        logger.info("Gerando relatório diário")
        self.report_generator.generate_daily_report()  # Agora chamando o método correto
        logger.info("Relatório gerado")
    
    def run(self):
        """Executa o sistema continuamente"""
        logger.info("Iniciando Agente Autônomo de Vendas e Comissões")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Encerrando o agente de vendas")
            self.lead_collector.close()

if __name__ == "__main__":
    agent = AutonomousSalesAgent()
    agent.run()
