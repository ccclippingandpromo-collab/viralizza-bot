import os
import re
import time
import sqlite3
import threading
import asyncio
import secrets
import string
import signal
import traceback
from typing import Optional, Tuple, List, Dict

import aiohttp
import discord
from discord.ext import commands, tasks
from flask import Flask

# =========================
# TOKEN (Render env: TOKEN)
# =========================
def get_bot_token() -> str:
    tok = (os.getenv("TOKEN") or os.getenv("DISCORD_TOKEN") or "").strip()
    if not tok:
        raise RuntimeError("TOKEN/DISCORD_TOKEN está vazio no Render Environment.")
    print(f"[BOOT] TOKEN length={len(tok)} last4={tok[-4:]}")
    return tok

BOT_TOKEN = get_bot_token()

# =========================
# CONFIG (TEUS IDS)
# =========================
SERVER_ID = 1473469552917741678

# CANAIS
BEM_VINDO_CHANNEL_ID = 1473469553815191667
REGRAS_CHANNEL_ID = 1474972531583746340
LIGAR_CONTA_E_VERIFICAR_CHANNEL_ID = 1473488368741519464
VERIFICACOES_CHANNEL_ID = 1473886076476067850
COMO_FUNCIONA_CHANNEL_ID = 1474927252625035274
CHAT_CHANNEL_ID = 1475084891279462460
CAMPANHAS_CHANNEL_ID = 1473888170256105584
SUPORTE_CHANNEL_ID = 1474937040972939355
SUPORTE_STAFF_CHANNEL_ID = 1474938549181874320

# ROLES / ADMIN
VERIFICADO_ROLE_ID = 1473886534439538699
ADMIN_USER_ID = 1376499031890460714

# DB (Render Disk -> /var/data)
DB_PATH = os.getenv("DB_PATH", "/var/data/database.sqlite3").strip()

# APIFY (views automáticas)
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()
APIFY_ACTOR_TIKTOK = os.getenv("APIFY_ACTOR_TIKTOK", "clockworks/tiktok-scraper").strip()
APIFY_ACTOR_INSTAGRAM = os.getenv("APIFY_ACTOR_INSTAGRAM", "apify/instagram-scraper").strip()
VIEWS_REFRESH_MINUTES = int(os.getenv("VIEWS_REFRESH_MINUTES", "10").strip() or "10")

# Threshold para travar novas submissões (95%)
SUBMISSION_CLOSE_AT_PERCENT = float(os.getenv("SUBMISSION_CLOSE_AT_PERCENT", "0.95").strip() or "0.95")

print("DISCORD VERSION:", getattr(discord, "__version__", "unknown"))
print("DB_PATH:", DB_PATH)
print("APIFY_TOKEN set:", bool(APIFY_TOKEN))
print("APIFY_ACTOR_TIKTOK:", APIFY_ACTOR_TIKTOK)
print("APIFY_ACTOR_INSTAGRAM:", APIFY_ACTOR_INSTAGRAM)
print("VIEWS_REFRESH_MINUTES:", VIEWS_REFRESH_MINUTES)
print("SUBMISSION_CLOSE_AT_PERCENT:", SUBMISSION_CLOSE_AT_PERCENT)

# =========================
# BOT / INTENTS
# =========================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# HTTP SESSION (GLOBAL)
# =========================
HTTP_SESSION: Optional[aiohttp.ClientSession] = None

async def get_http_session() -> aiohttp.ClientSession:
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        timeout = aiohttp.ClientTimeout(total=45)
        HTTP_SESSION = aiohttp.ClientSession(timeout=timeout)
    return HTTP_SESSION

async def close_http_session():
    global HTTP_SESSION
    try:
        if HTTP_SESSION and not HTTP_SESSION.closed:
            await HTTP_SESSION.close()
    except Exception:
        pass
    HTTP_SESSION = None

# =========================
# HELPERS
# =========================
def _now() -> int:
    return int(time.time())

def _ensure_db_dir(path: str):
    try:
        d = os.path.dirname(path)
        if d and d not in (".", "./") and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
    except Exception as e:
        print("⚠️ Não consegui criar pasta do DB:", e)

def db_conn():
    _ensure_db_dir(DB_PATH)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def generate_verification_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "VZ-" + "".join(secrets.choice(alphabet) for _ in range(7))

def is_verified(member: discord.Member) -> bool:
    role = member.guild.get_role(VERIFICADO_ROLE_ID)
    return bool(role) and (role in member.roles)

async def fetch_member_safe(guild: discord.Guild, user_id: int):
    m = guild.get_member(user_id)
    if m:
        return m
    try:
        return await guild.fetch_member(user_id)
    except:
        return None

async def get_bot_member_safe(guild: discord.Guild) -> Optional[discord.Member]:
    try:
        if guild.me:
            return guild.me
    except:
        pass
    try:
        if bot.user:
            m = guild.get_member(bot.user.id)
            if m:
                return m
    except:
        pass
    try:
        if bot.user:
            return await guild.fetch_member(bot.user.id)
    except:
        pass
    return None

async def safe_defer(interaction: discord.Interaction, ephemeral: bool = True):
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
    except Exception:
        pass

async def safe_reply(
    interaction: discord.Interaction,
    content: str,
    ephemeral: bool = True,
    view: Optional[discord.ui.View] = None
):
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=ephemeral, view=view)
        else:
            await interaction.response.send_message(content, ephemeral=ephemeral, view=view)
    except Exception as e:
        print("⚠️ safe_reply falhou:", e)

async def notify_user(member: discord.Member, content: str, fallback_channel_id: Optional[int] = None):
    try:
        await member.send(content)
        return True
    except:
        if fallback_channel_id:
            try:
                ch = member.guild.get_channel(fallback_channel_id)
                if ch:
                    await ch.send(f"{member.mention} {content}")
            except:
                pass
        return False

async def notify_user_with_view(member: discord.Member, content: str, view: discord.ui.View, fallback_channel_id: Optional[int] = None):
    try:
        await member.send(content, view=view)
        return True
    except:
        if fallback_channel_id:
            try:
                ch = member.guild.get_channel(fallback_channel_id)
                if ch:
                    await ch.send(f"{member.mention} {content}", view=view)
            except:
                pass
        return False

def detect_platform(url: str) -> str:
    u = (url or "").lower()
    if "tiktok.com" in u:
        return "tiktok"
    if "instagram.com" in u:
        return "instagram"
    return "unknown"

def slugify(text: str) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"[^a-z0-9]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-")
    return t[:64] if t else "campanha"

def parse_platforms(platforms_str: str) -> List[str]:
    # "TikTok,Instagram" -> ["tiktok","instagram"]
    s = (platforms_str or "")
    items = [x.strip().lower() for x in s.split(",") if x.strip()]
    norm = []
    for it in items:
        if it in ("tiktok", "tik tok"):
            norm.append("tiktok")
        elif it in ("instagram", "insta"):
            norm.append("instagram")
        elif it in ("youtube", "yt"):
            norm.append("youtube")
        else:
            norm.append(it)
    return sorted(list(set(norm)))

def fmt_percent(x: float) -> str:
    try:
        return f"{x*100:.1f}%"
    except:
        return "0%"

def safe_int(x, default=0) -> int:
    try:
        return int(x)
    except:
        return int(default)

# =========================
# DB INIT + MIGRATIONS
# =========================
def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

def init_db():
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ibans (
        user_id INTEGER PRIMARY KEY,
        iban TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS verification_requests (
        user_id INTEGER PRIMARY KEY,
        social TEXT NOT NULL,
        username TEXT NOT NULL,
        code TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        verify_message_id INTEGER,
        verify_channel_id INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        slug TEXT NOT NULL UNIQUE,
        platforms TEXT NOT NULL,
        content_types TEXT NOT NULL,
        audio_url TEXT,
        rate_kz_per_1k INTEGER NOT NULL,
        budget_total_kz INTEGER NOT NULL,
        spent_kz INTEGER NOT NULL DEFAULT 0,
        max_payout_user_kz INTEGER NOT NULL,
        max_posts_total INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        campaigns_channel_id INTEGER,
        post_message_id INTEGER,
        category_id INTEGER,
        details_channel_id INTEGER,
        requirements_channel_id INTEGER,
        submit_channel_id INTEGER,
        submit_panel_message_id INTEGER,
        leaderboard_channel_id INTEGER,
        leaderboard_message_id INTEGER,
        created_at INTEGER NOT NULL
    )
    """)

    # MIGRATION: role por campanha (para acesso privado)
    if not _column_exists(conn, "campaigns", "campaign_role_id"):
        try:
            cur.execute("ALTER TABLE campaigns ADD COLUMN campaign_role_id INTEGER")
            print("✅ MIGRATION: campaigns.campaign_role_id adicionado")
        except Exception as e:
            print("⚠️ MIGRATION falhou (talvez já exista):", e)

    # MIGRATION: estado "closing" opcional (só via status string, sem coluna)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        post_url TEXT NOT NULL,
        platform TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        views_current INTEGER NOT NULL DEFAULT 0,
        paid_views INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        approved_at INTEGER,
        UNIQUE(campaign_id, post_url)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS campaign_users (
        campaign_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        paid_kz INTEGER NOT NULL DEFAULT 0,
        total_views_paid INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (campaign_id, user_id)
    )
    """)

    conn.commit()
    conn.close()

# ===== IBAN HELPERS =====
def set_iban(user_id: int, iban: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO ibans (user_id, iban, updated_at)
    VALUES (?, ?, datetime('now'))
    ON CONFLICT(user_id) DO UPDATE SET
        iban=excluded.iban,
        updated_at=datetime('now')
    """, (user_id, iban))
    conn.commit()
    conn.close()

def get_iban(user_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT iban, updated_at FROM ibans WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row

# ===== VERIFICAÇÃO HELPERS =====
def upsert_verification_request(user_id: int, social: str, username: str, code: str, status: str = "pending"):
    conn = db_conn()
    cur = conn.cursor()
    now = _now()
    cur.execute("""
    INSERT INTO verification_requests (user_id, social, username, code, status, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
        social=excluded.social,
        username=excluded.username,
        code=excluded.code,
        status=excluded.status,
        updated_at=excluded.updated_at
    """, (user_id, social, username, code, status, now, now))
    conn.commit()
    conn.close()

def set_verification_message(user_id: int, channel_id: int, message_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    UPDATE verification_requests
    SET verify_channel_id=?, verify_message_id=?, updated_at=?
    WHERE user_id=?
    """, (channel_id, message_id, _now(), user_id))
    conn.commit()
    conn.close()

def get_verification_request(user_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id, social, username, code, status, verify_channel_id, verify_message_id
    FROM verification_requests WHERE user_id=?
    """, (user_id,))
    row = cur.fetchone()
    conn.close()
    return row

def set_verification_status(user_id: int, status: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    UPDATE verification_requests
    SET status=?, updated_at=?
    WHERE user_id=?
    """, (status, _now(), user_id))
    conn.commit()
    conn.close()

def list_pending_verifications():
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id, social, username, code, verify_channel_id, verify_message_id
    FROM verification_requests
    WHERE status='pending'
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# ===== CAMPAIGN HELPERS =====
def get_campaign_by_slug(conn, slug: str):
    cur = conn.cursor()
    cur.execute("""
    SELECT id, name, slug, platforms, content_types, audio_url,
           rate_kz_per_1k, budget_total_kz, spent_kz,
           max_payout_user_kz, max_posts_total, status,
           campaigns_channel_id, post_message_id,
           category_id, details_channel_id, requirements_channel_id,
           submit_channel_id, submit_panel_message_id,
           leaderboard_channel_id, leaderboard_message_id,
           campaign_role_id
    FROM campaigns WHERE slug=?
    """, (slug,))
    return cur.fetchone()

def get_campaign_by_id(conn, campaign_id: int):
    cur = conn.cursor()
    cur.execute("""
    SELECT id, name, slug, platforms, content_types, audio_url,
           rate_kz_per_1k, budget_total_kz, spent_kz,
           max_payout_user_kz, max_posts_total, status,
           campaigns_channel_id, post_message_id,
           category_id, details_channel_id, requirements_channel_id,
           submit_channel_id, submit_panel_message_id,
           leaderboard_channel_id, leaderboard_message_id,
           campaign_role_id
    FROM campaigns WHERE id=?
    """, (campaign_id,))
    return cur.fetchone()

def set_campaign_post_message_id(slug: str, msg_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE campaigns SET post_message_id=? WHERE slug=?", (int(msg_id), slug))
    conn.commit()
    conn.close()

def set_campaign_role_id(campaign_id: int, role_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE campaigns SET campaign_role_id=? WHERE id=?", (int(role_id), int(campaign_id)))
    conn.commit()
    conn.close()

def set_campaign_workspace_ids(
    campaign_id: int,
    category_id: int,
    details_id: int,
    req_id: int,
    submit_id: int,
    submit_panel_id: int,
    lb_id: int,
    lb_msg_id: int
):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE campaigns SET
            category_id=?,
            details_channel_id=?,
            requirements_channel_id=?,
            submit_channel_id=?,
            submit_panel_message_id=?,
            leaderboard_channel_id=?,
            leaderboard_message_id=?
        WHERE id=?
    """, (category_id, details_id, req_id, submit_id, submit_panel_id, lb_id, lb_msg_id, campaign_id))
    conn.commit()
    conn.close()

def compute_spent_ratio(spent_kz: int, budget_total_kz: int) -> float:
    if budget_total_kz <= 0:
        return 0.0
    return max(0.0, min(1.0, float(spent_kz) / float(budget_total_kz)))

def campaign_is_closed_for_new_submissions(spent_kz: int, budget_total_kz: int) -> bool:
    return compute_spent_ratio(spent_kz, budget_total_kz) >= SUBMISSION_CLOSE_AT_PERCENT

# =========================
# CAMPANHA TESTE
# =========================
TREEZY_TEST_CAMPAIGN = {
    "name": "Treezy Flacko – Kwarran",
    "slug": "treezy-flacko-kwarran",
    "platforms": "TikTok,Instagram",
    "content_types": "dança,cantar,edits",
    "audio_url": "https://vm.tiktok.com/ZG9eXXb3dbgoJ-LW9HG/",
    "rate_kz_per_1k": 800,
    "budget_total_kz": 167_000,
    "max_payout_user_kz": 50_000,
    "max_posts_total": 8,
}

def campaign_post_text(c):
    return (
        f"🎵 **Título:** {c['name']}\n\n"
        f"• **Plataformas:** {c['platforms']}\n"
        f"• **Tipo:** {c['content_types'].replace(',', ', ')}\n"
        f"• **Taxa:** {c['rate_kz_per_1k']} Kz / 1000 views\n\n"
        f"👇 Clica no botão para aderir"
    )

def details_channel_text(c):
    return (
        f"📊 **Plataformas:** {c['platforms']}\n\n"
        f"🎥 **Tipo:** {c['content_types'].replace(',', ', ')}\n\n"
        f"💸 **Taxa:** {c['rate_kz_per_1k']} Kz / 1000 visualizações\n\n"
        f"💰 **Budget:** {c['budget_total_kz']:,} Kz\n"
        f"🧾 **Máx por pessoa:** {c['max_payout_user_kz']:,} Kz\n"
        f"📦 **Máx posts (campanha):** {c['max_posts_total']}\n"
    )

def requirements_text(c):
    return (
        "📌 **REGRAS:**\n"
        "• Mínimo: **2.000 views** (somadas)\n"
        f"• Conteúdo: {c['content_types'].replace(',', ', ')}\n\n"
        "🎵 **Áudio (se aplicável):**\n"
        f"{c.get('audio_url','')}\n"
    )

# =========================
# UI VIEWS (persistent)
# =========================
class MainView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="Conectar rede social", style=discord.ButtonStyle.green, custom_id="vz:connect"))
        self.add_item(discord.ui.Button(label="Ver minha conta", style=discord.ButtonStyle.blurple, custom_id="vz:view_account"))

class IbanButtons(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="Adicionar / Atualizar IBAN", style=discord.ButtonStyle.primary, custom_id="vz:iban:add"))
        self.add_item(discord.ui.Button(label="Ver meu IBAN", style=discord.ButtonStyle.secondary, custom_id="vz:iban:view"))

class JoinCampaignView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="🔥 Aderir à Campanha", style=discord.ButtonStyle.success, custom_id="vz:camp:join"))

def submit_view(campaign_id: int) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(discord.ui.Button(label="📥 Submeter link", style=discord.ButtonStyle.primary, custom_id=f"vz:submit:open:{campaign_id}"))
    v.add_item(discord.ui.Button(label="📊 Ver estatísticas", style=discord.ButtonStyle.secondary, custom_id=f"vz:submit:stats:{campaign_id}"))
    v.add_item(discord.ui.Button(label="🗑️ Remover vídeo", style=discord.ButtonStyle.danger, custom_id=f"vz:submit:remove:{campaign_id}"))
    return v

def verify_approval_view(user_id: int) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(discord.ui.Button(label="✅ Aprovar", style=discord.ButtonStyle.green, custom_id=f"vz:verify:approve:{user_id}"))
    v.add_item(discord.ui.Button(label="❌ Rejeitar", style=discord.ButtonStyle.red, custom_id=f"vz:verify:reject:{user_id}"))
    return v

def submission_approval_view(submission_id: int) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(discord.ui.Button(label="✅ Aprovar link", style=discord.ButtonStyle.green, custom_id=f"vz:sub:approve:{submission_id}"))
    v.add_item(discord.ui.Button(label="❌ Rejeitar link", style=discord.ButtonStyle.red, custom_id=f"vz:sub:reject:{submission_id}"))
    return v

# =========================
# SUPORTE (como tu pediste)
# =========================
class SupportCampaignModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Problema com Campanha")
        self.campaign_name = discord.ui.TextInput(
            label="Nome da campanha",
            placeholder="Ex: Treezy Flacko – Kwarran",
            required=True,
            max_length=80
        )
        self.problem = discord.ui.TextInput(
            label="Qual é o problema?",
            placeholder="Explica o que está a acontecer (erro, botão, link, etc.)",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=800
        )
        self.add_item(self.campaign_name)
        self.add_item(self.problem)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_reply(interaction, "✅ Recebido. O staff vai analisar e responder-te.", ephemeral=True)
        guild = interaction.guild or bot.get_guild(SERVER_ID)
        staff_ch = guild.get_channel(SUPORTE_STAFF_CHANNEL_ID) if guild else None
        if staff_ch:
            await staff_ch.send(
                "🆘 **Novo ticket — Problema com Campanha**\n"
                f"👤 {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"🎯 Campanha: **{self.campaign_name.value}**\n"
                f"📝 Problema:\n{self.problem.value}"
            )

class SupportQuestionModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Dúvidas")
        self.question = discord.ui.TextInput(
            label="Escreve a tua dúvida",
            placeholder="Escreve a tua pergunta aqui…",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=900
        )
        self.add_item(self.question)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_reply(interaction, "✅ Recebido. O staff vai responder-te em breve.", ephemeral=True)
        guild = interaction.guild or bot.get_guild(SERVER_ID)
        staff_ch = guild.get_channel(SUPORTE_STAFF_CHANNEL_ID) if guild else None
        if staff_ch:
            await staff_ch.send(
                "❓ **Nova dúvida**\n"
                f"👤 {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"📝 Mensagem:\n{self.question.value}"
            )

class SupportView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📌 Problema com campanha", style=discord.ButtonStyle.danger, custom_id="vz:support:campaign")
    async def support_campaign(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SupportCampaignModal())

    @discord.ui.button(label="💬 Dúvidas", style=discord.ButtonStyle.primary, custom_id="vz:support:question")
    async def support_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SupportQuestionModal())

# =========================
# MODALS
# =========================
class UsernameModal(discord.ui.Modal):
    def __init__(self, social: str, code: str):
        super().__init__(title="Ligar Conta")
        self.social = social
        self.code = code
        self.username = discord.ui.TextInput(label="Coloca o teu username", placeholder="@teu_username", required=True, max_length=64)
        self.add_item(self.username)

    async def on_submit(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        username = str(self.username.value).strip()

        upsert_verification_request(user_id=user_id, social=self.social, username=username, code=self.code, status="pending")

        await safe_reply(
            interaction,
            "✅ Pedido enviado!\n\n"
            f"📱 Rede: {self.social}\n"
            f"👤 Username: {username}\n"
            f"🔑 Código: {self.code}\n\n"
            "🔒 Coloca este código na tua BIO para confirmar.\n"
            "⏳ Depois disso, aguarda aprovação do staff.",
            ephemeral=True
        )

        guild = bot.get_guild(SERVER_ID)
        if not guild:
            return
        channel = guild.get_channel(VERIFICACOES_CHANNEL_ID)
        if not channel:
            return

        msg = await channel.send(
            f"🆕 **Novo pedido de verificação**\n"
            f"👤 User: {interaction.user.mention} (`{user_id}`)\n"
            f"📱 Rede: **{self.social}**\n"
            f"🏷️ Username: **{username}**\n"
            f"🔑 Código: `{self.code}`\n"
            f"📌 Status: **PENDENTE**",
            view=verify_approval_view(user_id)
        )
        set_verification_message(user_id=user_id, channel_id=channel.id, message_id=msg.id)

class IbanModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Adicionar / Atualizar IBAN")
        self.iban = discord.ui.TextInput(label="Escreve o teu IBAN", placeholder="AO06 0000 0000 0000 0000 0000 0", required=True, max_length=64)
        self.add_item(self.iban)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild or bot.get_guild(SERVER_ID)
        if not guild:
            return await safe_reply(interaction, "⚠️ Servidor não encontrado.", ephemeral=True)
        member = await fetch_member_safe(guild, interaction.user.id)
        if not member or not is_verified(member):
            return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para guardar IBAN.", ephemeral=True)

        iban_val = str(self.iban.value).strip()
        if len(iban_val) < 10:
            return await safe_reply(interaction, "❌ IBAN inválido (muito curto).", ephemeral=True)

        set_iban(interaction.user.id, iban_val)
        await safe_reply(interaction, "✅ IBAN guardado com sucesso.", ephemeral=True)

class RemoveLinkModal(discord.ui.Modal):
    def __init__(self, campaign_id: int):
        super().__init__(title="Remover vídeo da campanha")
        self.campaign_id = int(campaign_id)
        self.url = discord.ui.TextInput(
            label="Link do vídeo a remover",
            placeholder="Cole aqui o link EXACTO que submeteste",
            required=True,
            max_length=300
        )
        self.add_item(self.url)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild or bot.get_guild(SERVER_ID)
        if not guild:
            return await safe_reply(interaction, "⚠️ Servidor não encontrado.", ephemeral=True)

        member = await fetch_member_safe(guild, interaction.user.id)
        if not member or not is_verified(member):
            return await safe_reply(interaction, "⛔ Tens de estar **Verificado**.", ephemeral=True)

        url = str(self.url.value).strip()
        conn = db_conn()
        cur = conn.cursor()

        # só permite remover do próprio user
        cur.execute("""
        SELECT id, status FROM submissions
        WHERE campaign_id=? AND user_id=? AND post_url=?
        """, (self.campaign_id, interaction.user.id, url))
        row = cur.fetchone()
        if not row:
            conn.close()
            return await safe_reply(interaction, "❌ Não encontrei esse link nas tuas submissões desta campanha.", ephemeral=True)

        sub_id, st = row
        if st in ("removed", "rejected"):
            conn.close()
            return await safe_reply(interaction, f"⚠️ Esse vídeo já está como **{st}**.", ephemeral=True)

        cur.execute("UPDATE submissions SET status='removed' WHERE id=?", (int(sub_id),))
        conn.commit()
        conn.close()

        await safe_reply(interaction, "✅ Vídeo removido da campanha (não conta mais para aprovação/pagamento).", ephemeral=True)

class SubmitLinkModal(discord.ui.Modal):
    def __init__(self, campaign_id: int):
        super().__init__(title="Submeter link (TikTok/Instagram)")
        self.campaign_id = int(campaign_id)
        self.url = discord.ui.TextInput(
            label="Link do teu post",
            placeholder="https://www.tiktok.com/@.../video/...  OU  https://www.instagram.com/reel/...",
            required=True,
            max_length=300
        )
        self.add_item(self.url)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild or bot.get_guild(SERVER_ID)
        if not guild:
            return await safe_reply(interaction, "⚠️ Servidor não encontrado.", ephemeral=True)

        member = await fetch_member_safe(guild, interaction.user.id)
        if not member or not is_verified(member):
            return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para submeter links.", ephemeral=True)

        url = str(self.url.value).strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            return await safe_reply(interaction, "❌ Link inválido. Envia um link completo com **https://**", ephemeral=True)

        platform = detect_platform(url)
        if platform not in ("tiktok", "instagram"):
            return await safe_reply(interaction, "❌ Só aceitamos links de **TikTok** ou **Instagram** por agora.", ephemeral=True)

        conn = db_conn()
        row = get_campaign_by_id(conn, self.campaign_id)
        if not row:
            conn.close()
            return await safe_reply(interaction, "❌ Campanha não encontrada.", ephemeral=True)

        (
            camp_id, name, slug, platforms_str, content_types, audio_url,
            rate, budget_total, spent_kz,
            max_user_kz, max_posts_total, status,
            campaigns_channel_id, post_message_id,
            category_id, details_id, req_id,
            submit_ch_id, submit_panel_id,
            lb_ch_id, lb_msg_id,
            campaign_role_id
        ) = row

        # campanha terminada
        if status == "ended":
            conn.close()
            return await safe_reply(interaction, "⚠️ Esta campanha já terminou.", ephemeral=True)

        # trava novas submissões a partir de 95%
        if campaign_is_closed_for_new_submissions(int(spent_kz), int(budget_total)):
            conn.close()
            return await safe_reply(
                interaction,
                f"⛔ A campanha está em **{fmt_percent(compute_spent_ratio(int(spent_kz), int(budget_total)))}** do budget.\n"
                "✅ Estamos a aceitar apenas contagem de views dos vídeos já submetidos.\n"
                "❌ Novas submissões estão fechadas.",
                ephemeral=True
            )

        # valida se a plataforma do link faz parte da campanha
        allowed = parse_platforms(platforms_str)
        if platform not in allowed:
            conn.close()
            return await safe_reply(
                interaction,
                f"❌ Este link é de **{platform.upper()}**, mas a campanha aceita apenas: **{', '.join([p.upper() for p in allowed])}**.",
                ephemeral=True
            )

        cur = conn.cursor()

        # limite total de posts na campanha (não conta rejected/removed)
        cur.execute("""
        SELECT COUNT(*)
        FROM submissions
        WHERE campaign_id=? AND status IN ('pending','approved','frozen')
        """, (self.campaign_id,))
        total_active_posts = int((cur.fetchone() or [0])[0])

        if total_active_posts >= int(max_posts_total):
            conn.close()
            return await safe_reply(interaction, "⛔ Limite de posts desta campanha já foi atingido.", ephemeral=True)

        now = _now()
        try:
            cur.execute("""
            INSERT INTO submissions (campaign_id, user_id, post_url, platform, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
            """, (self.campaign_id, interaction.user.id, url, platform, now))
            submission_id = int(cur.lastrowid)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return await safe_reply(interaction, "⚠️ Este vídeo já foi submetido nesta campanha.", ephemeral=True)

        # manda para aprovação no canal de verificações/staff
        appr = guild.get_channel(VERIFICACOES_CHANNEL_ID)
        if appr:
            await appr.send(
                f"📥 **Novo link submetido**\n"
                f"🆔 Submission: `{submission_id}`\n"
                f"🎯 Campanha: **{name}** (`{self.campaign_id}`)\n"
                f"👤 User: {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"🌐 Plataforma: **{platform.upper()}**\n"
                f"🔗 {url}\n"
                f"📌 Status: **PENDENTE**",
                view=submission_approval_view(submission_id)
            )

        conn.close()
        await safe_reply(interaction, "✅ Link submetido! Aguarda aprovação do staff.", ephemeral=True)

# =========================
# CAMPAIGN PRIVATE WORKSPACE
# =========================
async def ensure_campaign_role(guild: discord.Guild, campaign_id: int, slug: str, existing_role_id: Optional[int]) -> discord.Role:
    role = None
    if existing_role_id:
        role = guild.get_role(int(existing_role_id))
    if role:
        return role

    role_name = f"VZ • {slug}"
    role = discord.utils.get(guild.roles, name=role_name)
    if role is None:
        role = await guild.create_role(name=role_name, reason="Viralizzaa campaign access role")

    set_campaign_role_id(campaign_id, role.id)
    return role

async def ensure_campaign_workspace_private(
    guild: discord.Guild,
    camp_id: int,
    name: str,
    slug: str,
    platforms: str,
    content_types: str,
    audio_url: str,
    rate: int,
    budget_total: int,
    max_user_kz: int,
    max_posts_total: int,
    category_id: Optional[int],
    campaign_role: discord.Role
) -> int:
    existing = guild.get_channel(int(category_id)) if category_id else None
    if existing:
        return int(existing.id)

    bot_member = await get_bot_member_safe(guild)
    admin_member = guild.get_member(ADMIN_USER_ID) or await fetch_member_safe(guild, ADMIN_USER_ID)

    overwrites_cat = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        campaign_role: discord.PermissionOverwrite(view_channel=True, read_message_history=True),
    }
    if bot_member:
        overwrites_cat[bot_member] = discord.PermissionOverwrite(
            view_channel=True, read_message_history=True, send_messages=True,
            manage_channels=True, manage_messages=True
        )
    if admin_member:
        overwrites_cat[admin_member] = discord.PermissionOverwrite(
            view_channel=True, read_message_history=True, send_messages=True, manage_messages=True
        )

    category = await guild.create_category(f"🎯 {name}", overwrites=overwrites_cat)

    overw_ro = overwrites_cat.copy()
    overw_ro[campaign_role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True, send_messages=False)

    overw_submit = overwrites_cat.copy()
    overw_submit[campaign_role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True, send_messages=True)

    details_ch = await guild.create_text_channel("1-detalhes-da-campanha", category=category, overwrites=overw_ro)
    req_ch = await guild.create_text_channel("2-requisitos", category=category, overwrites=overw_ro)
    submit_ch = await guild.create_text_channel("3-submeter-links", category=category, overwrites=overw_submit)
    lb_ch = await guild.create_text_channel("4-leaderboard", category=category, overwrites=overw_ro)

    cobj = {
        "name": name,
        "platforms": platforms,
        "content_types": content_types,
        "audio_url": audio_url,
        "rate_kz_per_1k": rate,
        "budget_total_kz": budget_total,
        "max_payout_user_kz": max_user_kz,
        "max_posts_total": max_posts_total,
    }
    await details_ch.send(details_channel_text(cobj))
    await req_ch.send(requirements_text(cobj))

    submit_panel = await submit_ch.send("📤 **Submete os teus links aqui**\n\nUsa os botões 👇", view=submit_view(camp_id))
    lb_msg = await lb_ch.send("🏆 **LEADERBOARD**\n(aguarda atualizações automáticas)")

    set_campaign_workspace_ids(
        campaign_id=camp_id,
        category_id=category.id,
        details_id=details_ch.id,
        req_id=req_ch.id,
        submit_id=submit_ch.id,
        submit_panel_id=submit_panel.id,
        lb_id=lb_ch.id,
        lb_msg_id=lb_msg.id
    )

    return int(category.id)

# =========================
# COMMANDS
# =========================
@bot.command()
async def ligar(ctx):
    if ctx.guild and ctx.guild.id != SERVER_ID:
        return
    await ctx.send("**Ligar conta e verificar**", view=MainView())

@bot.command()
async def ibanpanel(ctx):
    if ctx.guild and ctx.guild.id != SERVER_ID:
        return
    await ctx.send("**Painel IBAN (apenas verificados)**", view=IbanButtons())

@bot.command()
async def suporte(ctx):
    if ctx.guild and ctx.guild.id != SERVER_ID:
        return
    if ctx.channel.id != SUPORTE_CHANNEL_ID:
        return await ctx.send(f"Usa o suporte aqui: <#{SUPORTE_CHANNEL_ID}>")
    await ctx.send("🆘 **SUPORTE**\nEscolhe uma opção:", view=SupportView())

@commands.has_permissions(administrator=True)
@bot.command()
async def campanha(ctx):
    """Publica o post/painel em #campanhas. Categoria só aparece quando alguém aderir."""
    if ctx.guild and ctx.guild.id != SERVER_ID:
        return

    conn = db_conn()
    cur = conn.cursor()
    now = _now()
    c = TREEZY_TEST_CAMPAIGN

    cur.execute("""
    INSERT OR IGNORE INTO campaigns
    (name, slug, platforms, content_types, audio_url, rate_kz_per_1k,
     budget_total_kz, max_payout_user_kz, max_posts_total,
     campaigns_channel_id, created_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        c["name"], c["slug"], c["platforms"], c["content_types"], c["audio_url"],
        c["rate_kz_per_1k"], c["budget_total_kz"], c["max_payout_user_kz"], c["max_posts_total"],
        CAMPANHAS_CHANNEL_ID, now
    ))
    conn.commit()

    row = get_campaign_by_slug(conn, c["slug"])
    if not row:
        conn.close()
        return await ctx.send("❌ Erro ao criar campanha no DB.")

    post_msg_id = row[13]
    conn.close()

    guild = ctx.guild
    ch = guild.get_channel(CAMPANHAS_CHANNEL_ID) if guild else None
    target = ch or ctx.channel

    needs_new_post = True
    if ch and post_msg_id:
        try:
            await ch.fetch_message(int(post_msg_id))
            needs_new_post = False
        except:
            needs_new_post = True

    if needs_new_post:
        msg = await target.send(campaign_post_text(c), view=JoinCampaignView())
        set_campaign_post_message_id(c["slug"], msg.id)

    await ctx.send("✅ Campanha publicada em #campanhas. (A categoria só aparece quando alguém aderir)")

# =========================
# REATTACH PANELS
# =========================
async def reattach_pending_verification_views():
    guild = bot.get_guild(SERVER_ID)
    if not guild:
        return
    ch = guild.get_channel(VERIFICACOES_CHANNEL_ID)
    if not ch:
        return

    for (user_id, social, username, code, vch_id, msg_id) in list_pending_verifications():
        try:
            if vch_id and msg_id:
                vch = guild.get_channel(int(vch_id)) or ch
                msg = await vch.fetch_message(int(msg_id))
                await msg.edit(view=verify_approval_view(int(user_id)))
        except:
            pass

async def reattach_submit_panels():
    guild = bot.get_guild(SERVER_ID)
    if not guild:
        return

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT id, submit_channel_id, submit_panel_message_id
    FROM campaigns
    WHERE submit_channel_id IS NOT NULL AND submit_panel_message_id IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()

    for cid, submit_ch_id, panel_msg_id in rows:
        try:
            submit_ch = guild.get_channel(int(submit_ch_id))
            if not submit_ch:
                continue
            msg = await submit_ch.fetch_message(int(panel_msg_id))
            await msg.edit(view=submit_view(int(cid)))
        except:
            pass

# =========================
# LEADERBOARD
# =========================
async def update_leaderboard_for_campaign(campaign_id: int):
    guild = bot.get_guild(SERVER_ID)
    if not guild:
        return

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT leaderboard_channel_id, leaderboard_message_id, name, spent_kz, budget_total_kz, status
    FROM campaigns WHERE id=?
    """, (campaign_id,))
    camp = cur.fetchone()
    if not camp:
        conn.close()
        return

    lb_ch_id, lb_msg_id, name, spent, budget, status = camp
    spent = int(spent or 0)
    budget = int(budget or 0)

    # Top por dinheiro pago (desc) e views pagas (desc)
    cur.execute("""
    SELECT user_id, paid_kz, total_views_paid
    FROM campaign_users
    WHERE campaign_id=?
    ORDER BY paid_kz DESC, total_views_paid DESC
    LIMIT 10
    """, (campaign_id,))
    top_paid = cur.fetchall()

    # Views atuais por user (soma de views_current dos approved/frozen)
    cur.execute("""
    SELECT user_id, COALESCE(SUM(views_current),0) as views_sum
    FROM submissions
    WHERE campaign_id=? AND status IN ('approved','frozen')
    GROUP BY user_id
    """, (campaign_id,))
    views_map = {int(uid): int(vsum) for (uid, vsum) in cur.fetchall()}

    conn.close()

    if not lb_ch_id or not lb_msg_id:
        return

    ch = guild.get_channel(int(lb_ch_id))
    if not ch:
        return

    ratio = compute_spent_ratio(spent, budget)
    remaining = max(0, budget - spent)

    lines = [
        f"🏆 **LEADERBOARD — {name}**",
        f"💰 **Gasto:** {spent:,}/{budget:,} Kz (**{fmt_percent(ratio)}**) | ⏳ **Restante:** {remaining:,} Kz | 📌 **Estado:** {status}",
        ""
    ]

    if not top_paid:
        lines.append("Ainda sem pagamentos/atualizações.")
    else:
        for i, (uid, paid_kz, views_paid) in enumerate(top_paid, 1):
            views_now = views_map.get(int(uid), 0)
            lines.append(f"**{i}.** <@{uid}> — **{int(paid_kz):,} Kz** | views pagas: **{int(views_paid):,}** | views atuais: **{views_now:,}**")

    try:
        msg = await ch.fetch_message(int(lb_msg_id))
        await msg.edit(content="\n".join(lines))
    except:
        pass

# =========================
# APIFY: obter views
# =========================
async def apify_run(actor: str, payload: dict) -> Optional[dict]:
    if not APIFY_TOKEN:
        return None

    run_url = f"https://api.apify.com/v2/acts/{actor}/runs?token={APIFY_TOKEN}"
    try:
        session = await get_http_session()

        async with session.post(run_url, json=payload) as r:
            data = await r.json()
            run = data.get("data", {}) or {}
            run_id = run.get("id")
            dataset_id = run.get("defaultDatasetId")
            if not run_id or not dataset_id:
                return None

        status = None
        for _ in range(25):
            async with session.get(f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}") as rr:
                rd = await rr.json()
                status = (rd.get("data", {}) or {}).get("status")
                if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                    break
            await asyncio.sleep(2)

        if status != "SUCCEEDED":
            return None

        async with session.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&clean=true&limit=1"
        ) as ri:
            items = await ri.json()

        if not items or not isinstance(items, list):
            return None

        return items[0]
    except Exception as e:
        print("⚠️ APIFY erro:", e)
        traceback.print_exc()
        return None

def extract_views_from_item(item: dict) -> Optional[int]:
    if not item or not isinstance(item, dict):
        return None
    candidates = [
        "playCount", "plays", "views", "viewCount", "videoViewCount", "video_view_count",
        "videoPlayCount", "video_play_count"
    ]
    for k in candidates:
        v = item.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)

    stats = item.get("stats") or {}
    if isinstance(stats, dict):
        for k in ["playCount", "viewCount", "views", "videoViewCount"]:
            v = stats.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
    return None

async def apify_get_views_for_url(url: str) -> Optional[int]:
    platform = detect_platform(url)
    if platform == "tiktok":
        item = await apify_run(APIFY_ACTOR_TIKTOK, {"startUrls": [{"url": url}], "maxItems": 1})
        return extract_views_from_item(item) if item else None

    if platform == "instagram":
        payload = {"directUrls": [url], "resultsType": "posts", "resultsLimit": 1}
        item = await apify_run(APIFY_ACTOR_INSTAGRAM, payload)
        return extract_views_from_item(item) if item else None

    return None

# =========================
# VIEWS LOOP
# =========================
@tasks.loop(minutes=VIEWS_REFRESH_MINUTES)
async def refresh_views_loop():
    if not APIFY_TOKEN:
        return

    try:
        conn = db_conn()
        cur = conn.cursor()
        # conta views/pagamentos mesmo se campanha estiver "closing"
        cur.execute("""
        SELECT s.id, s.campaign_id, s.user_id, s.post_url, s.platform, s.views_current, s.paid_views,
               c.rate_kz_per_1k, c.budget_total_kz, c.spent_kz, c.max_payout_user_kz, c.status
        FROM submissions s
        JOIN campaigns c ON c.id = s.campaign_id
        WHERE s.status='approved' AND c.status IN ('active','closing')
        """)
        rows = cur.fetchall()
        conn.close()

        touched = set()

        for (sub_id, camp_id, user_id, url, platform, views_current, paid_views,
             rate, budget_total, spent_kz, max_user_kz, camp_status) in rows:

            views = await apify_get_views_for_url(url)
            if views is None:
                continue

            conn2 = db_conn()
            cur2 = conn2.cursor()

            # atualiza views_current
            cur2.execute("UPDATE submissions SET views_current=? WHERE id=?", (int(views), int(sub_id)))

            # calcula views pagáveis por blocos de 1000
            payable_total = (int(views) // 1000) * 1000
            to_pay_views = payable_total - int(paid_views)
            if to_pay_views < 1000:
                conn2.commit()
                conn2.close()
                continue

            to_pay_kz = (to_pay_views // 1000) * int(rate)

            # limite por user
            cur2.execute("SELECT COALESCE(paid_kz,0) FROM campaign_users WHERE campaign_id=? AND user_id=?",
                         (int(camp_id), int(user_id)))
            rowu = cur2.fetchone()
            already_paid_kz = int(rowu[0]) if rowu else 0
            remaining_user_kz = max(0, int(max_user_kz) - already_paid_kz)
            if remaining_user_kz <= 0:
                conn2.commit()
                conn2.close()
                continue
            if to_pay_kz > remaining_user_kz:
                max_blocks = remaining_user_kz // int(rate)
                to_pay_views = max_blocks * 1000
                to_pay_kz = max_blocks * int(rate)

            # budget restante
            remaining_budget = max(0, int(budget_total) - int(spent_kz))
            if remaining_budget <= 0:
                # termina campanha
                cur2.execute("UPDATE campaigns SET status='ended' WHERE id=?", (int(camp_id),))
                conn2.commit()
                conn2.close()
                touched.add(int(camp_id))
                continue

            if to_pay_kz > remaining_budget:
                max_blocks = remaining_budget // int(rate)
                to_pay_views = max_blocks * 1000
                to_pay_kz = max_blocks * int(rate)

            if to_pay_kz <= 0:
                conn2.commit()
                conn2.close()
                continue

            # aplica pagamento acumulado
            cur2.execute("""
            INSERT INTO campaign_users (campaign_id, user_id, paid_kz, total_views_paid)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(campaign_id, user_id) DO UPDATE SET
                paid_kz = paid_kz + excluded.paid_kz,
                total_views_paid = total_views_paid + excluded.total_views_paid
            """, (int(camp_id), int(user_id), int(to_pay_kz), int(to_pay_views)))

            cur2.execute("UPDATE submissions SET paid_views = paid_views + ? WHERE id=?",
                         (int(to_pay_views), int(sub_id)))

            cur2.execute("UPDATE campaigns SET spent_kz = spent_kz + ? WHERE id=?",
                         (int(to_pay_kz), int(camp_id)))

            # se cruzou 95%, passa para closing (não aceita novos)
            new_spent = int(spent_kz) + int(to_pay_kz)
            if campaign_is_closed_for_new_submissions(new_spent, int(budget_total)):
                cur2.execute("UPDATE campaigns SET status='closing' WHERE id=? AND status='active'", (int(camp_id),))

            # se esgotou, termina
            if new_spent >= int(budget_total):
                cur2.execute("UPDATE campaigns SET status='ended' WHERE id=?", (int(camp_id),))

            conn2.commit()
            conn2.close()
            touched.add(int(camp_id))

        for cid in touched:
            await update_leaderboard_for_campaign(cid)

    except Exception as e:
        print("⚠️ refresh_views_loop erro:", e)
        traceback.print_exc()

@refresh_views_loop.before_loop
async def before_refresh_views():
    await bot.wait_until_ready()

# =========================
# INTERACTIONS ROUTER
# =========================
@bot.event
async def on_interaction(interaction: discord.Interaction):
    try:
        if interaction.type != discord.InteractionType.component:
            return

        cid = (interaction.data or {}).get("custom_id")
        if not cid:
            return

        # -------- CONNECT --------
        if cid == "vz:connect":
            view = discord.ui.View(timeout=120)
            select = discord.ui.Select(
                placeholder="Escolhe a rede social",
                options=[
                    discord.SelectOption(label="TikTok", emoji="🎵", value="TikTok"),
                    discord.SelectOption(label="Instagram", emoji="📸", value="Instagram"),
                    discord.SelectOption(label="YouTube", emoji="📺", value="YouTube"),
                ],
            )

            async def _cb(i: discord.Interaction):
                social = (i.data["values"][0] if i.data and "values" in i.data else "TikTok")
                code = generate_verification_code()
                await i.response.send_modal(UsernameModal(social=social, code=code))

            select.callback = _cb
            view.add_item(select)
            return await safe_reply(interaction, "Escolhe a rede social:", ephemeral=True, view=view)

        if cid == "vz:view_account":
            await safe_defer(interaction, ephemeral=True)
            row = get_verification_request(interaction.user.id)
            if not row:
                return await safe_reply(interaction, "❌ Nenhum pedido encontrado.", ephemeral=True)
            _, social, username, code, status, _, _ = row
            msg = (
                ("✅ **Conta verificada**\n" if status == "verified" else "⏳ **Conta ainda não verificada**\n")
                + f"📱 Rede: {social}\n"
                + f"🏷️ Username: {username}\n"
                + f"🔑 Código: `{code}`\n"
                + ("" if status == "verified" else f"📌 Status: **{status.upper()}**")
            )
            return await safe_reply(interaction, msg, ephemeral=True)

        # -------- SUPORTE --------
        if cid == "vz:support:campaign":
            return await interaction.response.send_modal(SupportCampaignModal())
        if cid == "vz:support:question":
            return await interaction.response.send_modal(SupportQuestionModal())

        # -------- VERIFY APPROVAL --------
        if cid.startswith("vz:verify:"):
            await safe_defer(interaction, ephemeral=True)
            _, _, action, user_id = cid.split(":", 3)
            user_id = int(user_id)

            if interaction.user.id != ADMIN_USER_ID:
                return await safe_reply(interaction, "⛔ Só o admin pode aprovar/rejeitar.", ephemeral=True)

            row = get_verification_request(user_id)
            if not row:
                return await safe_reply(interaction, "⚠️ Pedido não existe no DB.", ephemeral=True)
            _, social, username, code, status, _, _ = row
            if status != "pending":
                return await safe_reply(interaction, f"⚠️ Pedido já está como **{status}**.", ephemeral=True)

            guild = bot.get_guild(SERVER_ID)
            member = await fetch_member_safe(guild, user_id) if guild else None
            if not guild or not member:
                return await safe_reply(interaction, "⚠️ Não consegui buscar guild/membro.", ephemeral=True)

            if action == "approve":
                role = guild.get_role(VERIFICADO_ROLE_ID)
                if not role:
                    return await safe_reply(interaction, "⚠️ Cargo 'Verificado' não encontrado.", ephemeral=True)
                try:
                    await member.add_roles(role, reason="Verificação aprovada")
                except discord.Forbidden:
                    return await safe_reply(interaction, "⛔ Bot sem permissões para dar cargo.", ephemeral=True)

                set_verification_status(user_id, "verified")

                # ✅ DM imediata com painel IBAN (botões)
                await notify_user_with_view(
                    member,
                    "✅ **Verificação aprovada!**\n\n"
                    "👉 **Agora adiciona o teu IBAN aqui mesmo (carrega no botão):**",
                    view=IbanButtons(),
                    fallback_channel_id=LIGAR_CONTA_E_VERIFICAR_CHANNEL_ID
                )

                try:
                    await interaction.message.edit(view=None)
                except:
                    pass
                return await safe_reply(interaction, "✅ Aprovado e cargo atribuído.", ephemeral=True)

            if action == "reject":
                set_verification_status(user_id, "rejected")
                try:
                    await interaction.message.edit(view=None)
                except:
                    pass
                return await safe_reply(interaction, "❌ Rejeitado.", ephemeral=True)

        # -------- IBAN --------
        if cid == "vz:iban:add":
            guild = interaction.guild or bot.get_guild(SERVER_ID)
            member = await fetch_member_safe(guild, interaction.user.id) if guild else None
            if not guild or not member:
                return await safe_reply(interaction, "⚠️ Servidor não encontrado.", ephemeral=True)
            if not is_verified(member):
                return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para adicionar IBAN.", ephemeral=True)
            return await interaction.response.send_modal(IbanModal())

        if cid == "vz:iban:view":
            await safe_defer(interaction, ephemeral=True)
            guild = interaction.guild or bot.get_guild(SERVER_ID)
            member = await fetch_member_safe(guild, interaction.user.id) if guild else None
            if not guild or not member:
                return await safe_reply(interaction, "⚠️ Servidor não encontrado.", ephemeral=True)
            if not is_verified(member):
                return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para ver IBAN.", ephemeral=True)
            row = get_iban(interaction.user.id)
            if not row:
                return await safe_reply(interaction, "Ainda não tens IBAN guardado.", ephemeral=True)
            iban, updated_at = row
            return await safe_reply(interaction, f"✅ Teu IBAN: **{iban}**\n🕒 Atualizado: {updated_at}", ephemeral=True)

        # -------- JOIN CAMPAIGN (cria role + categoria privada) --------
        if cid == "vz:camp:join":
            await safe_defer(interaction, ephemeral=True)
            guild = interaction.guild or bot.get_guild(SERVER_ID)
            member = await fetch_member_safe(guild, interaction.user.id) if guild else None
            if not guild or not member:
                return await safe_reply(interaction, "⚠️ Servidor não encontrado.", ephemeral=True)
            if not is_verified(member):
                return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para aderir.", ephemeral=True)

            post_id = interaction.message.id

            conn = db_conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT id, name, slug, platforms, content_types, audio_url, rate_kz_per_1k, budget_total_kz, spent_kz, "
                "max_payout_user_kz, max_posts_total, status, category_id, campaign_role_id "
                "FROM campaigns WHERE post_message_id=?",
                (post_id,)
            )
            row = cur.fetchone()
            conn.close()

            if not row:
                return await safe_reply(interaction, "❌ Campanha não encontrada no DB. Admin: `!campanha`.", ephemeral=True)

            camp_id, name, slug, platforms_str, content_types, audio_url, rate, budget_total, spent_kz, max_user_kz, max_posts_total, status, category_id, role_id = row

            if status == "ended":
                return await safe_reply(interaction, "⚠️ Esta campanha já terminou.", ephemeral=True)

            # 1) garantir role da campanha
            campaign_role = await ensure_campaign_role(guild, int(camp_id), str(slug), int(role_id) if role_id else None)

            # 2) impedir "aderir várias vezes"
            if campaign_role in member.roles:
                # já aderiu: só direciona
                if category_id and guild.get_channel(int(category_id)):
                    return await safe_reply(interaction, f"✅ Já aderiste. Categoria: <#{int(category_id)}>", ephemeral=True)
                # se a categoria não existe, tenta criar
            else:
                try:
                    await member.add_roles(campaign_role, reason="Aderiu à campanha")
                except discord.Forbidden:
                    return await safe_reply(interaction, "⛔ Bot sem permissões para atribuir o role da campanha.", ephemeral=True)

            # 3) criar workspace privado (se ainda não existe)
            try:
                cat_id = await ensure_campaign_workspace_private(
                    guild=guild,
                    camp_id=int(camp_id),
                    name=str(name),
                    slug=str(slug),
                    platforms=str(platforms_str),
                    content_types=str(content_types),
                    audio_url=str(audio_url or ""),
                    rate=int(rate),
                    budget_total=int(budget_total),
                    max_user_kz=int(max_user_kz),
                    max_posts_total=int(max_posts_total),
                    category_id=int(category_id) if category_id else None,
                    campaign_role=campaign_role
                )
            except Exception:
                traceback.print_exc()
                return await safe_reply(interaction, "⚠️ Aderiste, mas falhei a criar a categoria/canais (vê logs/permissões).", ephemeral=True)

            return await safe_reply(interaction, f"✅ Aderiste! Agora tens acesso à categoria: <#{cat_id}>", ephemeral=True)

        # -------- SUBMIT --------
        if cid.startswith("vz:submit:"):
            _, _, action, camp_id = cid.split(":", 3)
            camp_id = int(camp_id)

            if action == "open":
                return await interaction.response.send_modal(SubmitLinkModal(camp_id))

            if action == "remove":
                return await interaction.response.send_modal(RemoveLinkModal(camp_id))

            if action == "stats":
                await safe_defer(interaction, ephemeral=True)
                conn = db_conn()
                cur = conn.cursor()

                # campanha
                cur.execute("SELECT name, budget_total_kz, spent_kz, max_payout_user_kz, status FROM campaigns WHERE id=?", (camp_id,))
                camp = cur.fetchone()
                if not camp:
                    conn.close()
                    return await safe_reply(interaction, "❌ Campanha não encontrada.", ephemeral=True)
                camp_name, bt, sk, mx, st = camp
                bt = int(bt or 0); sk = int(sk or 0); mx = int(mx or 0)
                ratio = compute_spent_ratio(sk, bt)
                remaining = max(0, bt - sk)

                # lista submissões do user
                cur.execute("""
                SELECT post_url, platform, status, views_current, paid_views
                FROM submissions
                WHERE campaign_id=? AND user_id=? AND status IN ('pending','approved','frozen')
                ORDER BY created_at DESC
                """, (camp_id, interaction.user.id))
                subs = cur.fetchall()

                cur.execute("""
                SELECT COALESCE(paid_kz,0), COALESCE(total_views_paid,0)
                FROM campaign_users
                WHERE campaign_id=? AND user_id=?
                """, (camp_id, interaction.user.id))
                rowp = cur.fetchone() or (0, 0)
                paid_kz = int(rowp[0] or 0)
                views_paid_total = int(rowp[1] or 0)

                conn.close()

                # build texto
                lines = []
                lines.append(f"📊 **As tuas stats — {camp_name}**")
                lines.append(f"📌 Estado: **{st}**")
                lines.append(f"💰 Campanha: **{sk:,}/{bt:,} Kz** (**{fmt_percent(ratio)}**) | Restante: **{remaining:,} Kz**")
                lines.append("")
                lines.append(f"👤 **Tu:**")
                lines.append(f"• Dinheiro (pago): **{paid_kz:,} Kz** (máx {mx:,} Kz)")
                lines.append(f"• Views pagas (total): **{views_paid_total:,}**")
                lines.append("")
                lines.append(f"🎬 **Vídeos submetidos:** **{len(subs)}**")

                if subs:
                    show = subs[:8]
                    for (url, plat, stt, vcur, vpaid) in show:
                        lines.append(f"• [{plat.upper()}] **{stt}** | views: {int(vcur):,} | pagas: {int(vpaid):,}")
                    if len(subs) > 8:
                        lines.append(f"... +{len(subs)-8} outros")
                else:
                    lines.append("Ainda não tens vídeos nesta campanha.")

                # aviso de fecho 95%
                if campaign_is_closed_for_new_submissions(sk, bt) and st != "ended":
                    lines.append("")
                    lines.append("⛔ **Novas submissões fechadas (campanha ≥ 95%)**. Estamos só a contar views dos vídeos já submetidos.")

                return await safe_reply(interaction, "\n".join(lines), ephemeral=True)

        # -------- APPROVAL SUBMISSION --------
        if cid.startswith("vz:sub:"):
            await safe_defer(interaction, ephemeral=True)
            if interaction.user.id != ADMIN_USER_ID:
                return await safe_reply(interaction, "⛔ Só o admin pode aprovar/rejeitar.", ephemeral=True)

            _, _, action, sub_id = cid.split(":", 3)
            sub_id = int(sub_id)

            conn = db_conn()
            cur = conn.cursor()
            cur.execute("SELECT id, campaign_id, user_id, post_url, platform, status FROM submissions WHERE id=?",
                        (sub_id,))
            srow = cur.fetchone()
            if not srow:
                conn.close()
                return await safe_reply(interaction, "❌ Submission não encontrada.", ephemeral=True)

            _, camp_id, user_id, url, platform, st = srow
            if st != "pending":
                conn.close()
                return await safe_reply(interaction, f"⚠️ Já está como **{st}**.", ephemeral=True)

            # confirmar campanha ainda permite (se acabou, não aprova)
            cur.execute("SELECT name, budget_total_kz, spent_kz, status FROM campaigns WHERE id=?", (camp_id,))
            camp = cur.fetchone()
            if not camp:
                conn.close()
                return await safe_reply(interaction, "❌ Campanha não encontrada.", ephemeral=True)

            camp_name, bt, sk, camp_status = camp
            bt = int(bt or 0); sk = int(sk or 0)

            if camp_status == "ended" or sk >= bt:
                conn.close()
                return await safe_reply(interaction, "⛔ Campanha sem budget / terminada. Não vou aprovar novos links.", ephemeral=True)

            if action == "approve":
                cur.execute("UPDATE submissions SET status='approved', approved_at=? WHERE id=?", (_now(), sub_id))
                conn.commit()
                conn.close()

                # DM ao user aprovado
                guild = bot.get_guild(SERVER_ID)
                member = await fetch_member_safe(guild, int(user_id)) if guild else None
                if member:
                    await notify_user(
                        member,
                        f"✅ **O teu link foi aprovado!**\n"
                        f"🎯 Campanha: **{camp_name}**\n"
                        f"🔗 {url}\n\n"
                        "👉 Vai ao canal da campanha e carrega em **📊 Ver estatísticas** para veres views/dinheiro.",
                        fallback_channel_id=CHAT_CHANNEL_ID
                    )

                try:
                    await interaction.message.edit(view=None)
                except:
                    pass

                # update leaderboard
                try:
                    await update_leaderboard_for_campaign(int(camp_id))
                except:
                    pass

                return await safe_reply(interaction, "✅ Link aprovado.", ephemeral=True)

            if action == "reject":
                cur.execute("UPDATE submissions SET status='rejected' WHERE id=?", (sub_id,))
                conn.commit()
                conn.close()
                try:
                    await interaction.message.edit(view=None)
                except:
                    pass
                return await safe_reply(interaction, "❌ Link rejeitado.", ephemeral=True)

    except Exception as e:
        print("⚠️ on_interaction erro:", e)
        traceback.print_exc()
        try:
            if interaction and not interaction.response.is_done():
                await interaction.response.send_message("⚠️ Erro interno. Vê os logs no Render.", ephemeral=True)
        except:
            pass

# =========================
# READY
# =========================
@bot.event
async def on_ready():
    init_db()

    if not getattr(bot, "_views_added", False):
        bot.add_view(MainView())
        bot.add_view(IbanButtons())
        bot.add_view(JoinCampaignView())
        bot.add_view(SupportView())
        bot._views_added = True

    try:
        await reattach_pending_verification_views()
        await reattach_submit_panels()
    except Exception as e:
        print("⚠️ Erro ao reanexar views:", e)

    if not refresh_views_loop.is_running():
        refresh_views_loop.start()

    print(f"✅ Bot ligado como {bot.user}!")

# =========================
# WEB (opcional)
# =========================
app = Flask(__name__)

@app.get("/")
def home():
    return "Viralizza Bot is running!"

def run_web():
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)

def keep_alive():
    t = threading.Thread(target=run_web, daemon=True)
    t.start()

# =========================
# GRACEFUL SHUTDOWN (Render)
# =========================
async def _graceful_shutdown():
    try:
        await close_http_session()
    except Exception:
        pass
    try:
        await bot.close()
    except Exception:
        pass

def _handle_sigterm(*_):
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(_graceful_shutdown())
    except Exception:
        pass

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# =========================
# RUN
# =========================
keep_alive()
bot.run(BOT_TOKEN)
