import os
import re
import time
import sqlite3
import threading
import asyncio
import secrets
import string
from typing import Optional, Tuple, Any, List

import aiohttp
import discord
from discord.ext import commands, tasks
from flask import Flask

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

# DB
DB_PATH = os.getenv("DB_PATH", "/var/data/database.sqlite3")

# APIFY (views automáticas)
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "clockworks/tiktok-scraper").strip()  # podes mudar no Render
VIEWS_REFRESH_MINUTES = int(os.getenv("VIEWS_REFRESH_MINUTES", "10"))

print("DISCORD VERSION:", getattr(discord, "__version__", "unknown"))
print("DB_PATH:", DB_PATH)

# =========================
# BOT / INTENTS
# =========================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

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
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def generate_verification_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "VZ-" + "".join(secrets.choice(alphabet) for _ in range(7))

def tiktok_extract_video_id(url: str):
    m = re.search(r"/video/(\d+)", url)
    if m:
        return m.group(1)
    return None

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

async def _safe_ephemeral(interaction: discord.Interaction, content: str):
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=True)
        else:
            await interaction.response.send_message(content, ephemeral=True)
    except Exception as e:
        print("⚠️ _safe_ephemeral falhou:", e)

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

# =========================
# DB INIT
# =========================
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
    CREATE TABLE IF NOT EXISTS support_tickets (
        thread_id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,

        tiktok_url TEXT NOT NULL,
        tiktok_video_id TEXT,

        status TEXT NOT NULL DEFAULT 'pending',

        views_current INTEGER NOT NULL DEFAULT 0,
        paid_views INTEGER NOT NULL DEFAULT 0,

        created_at INTEGER NOT NULL,
        approved_at INTEGER,

        UNIQUE(campaign_id, tiktok_url)
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

# ===== SUPORTE HELPERS =====
def set_ticket(thread_id: int, user_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO support_tickets(thread_id, user_id, status) VALUES (?, ?, 'open')",
        (thread_id, user_id)
    )
    conn.commit()
    conn.close()

def get_open_thread_for_user(user_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT thread_id FROM support_tickets
    WHERE user_id=? AND status='open'
    ORDER BY created_at DESC LIMIT 1
    """, (user_id,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None

def get_user_for_thread(thread_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM support_tickets WHERE thread_id=? AND status='open'", (thread_id,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None

def close_ticket(thread_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE support_tickets SET status='closed' WHERE thread_id=?", (thread_id,))
    conn.commit()
    conn.close()

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

# =========================
# CAMPANHAS (BASE)
# =========================
TREEZY_TEST_CAMPAIGN = {
    "name": "Treezy Flacko – Kwarran",
    "slug": "treezy-flacko-kwarran",
    "platforms": "TikTok",
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
        f"**Detalhes da campanha:**\n"
        f"• **Plataformas autorizadas:** {c['platforms']}\n"
        f"• **Tipo de vídeo:** {c['content_types'].replace(',', ', ')}\n"
        f"• **Taxa de pagamento:** {c['rate_kz_per_1k']} Kz / 1000 views\n\n"
        f"👇 Clica no botão para aderir"
    )

def details_channel_text(c):
    return (
        f"📊 **Plataformas:** {c['platforms']}\n\n"
        f"🎥 **Tipo:** {c['content_types'].replace(',', ', ')}\n\n"
        f"💸 **Taxa:** {c['rate_kz_per_1k']} Kz / 1000 visualizações\n\n"
        f"💰 **Budget:** {c['budget_total_kz']:,} Kz\n"
        f"🧾 **Pagamento máximo por pessoa:** {c['max_payout_user_kz']:,} Kz\n"
        f"📦 **Nº máximo de posts (campanha):** {c['max_posts_total']}\n"
    )

def requirements_text(c):
    return (
        "📌 **REGRAS:**\n"
        "• Mínimo: **2.000 views** (somativas)\n"
        f"• Conteúdo obrigatório: {c['content_types'].replace(',', ', ')}\n\n"
        "🎵 **Áudio obrigatório:**\n"
        f"{c.get('audio_url','')}\n"
    )

def get_campaign_by_slug(conn, slug: str):
    cur = conn.cursor()
    cur.execute("""
    SELECT id, name, slug, platforms, content_types, audio_url,
           rate_kz_per_1k, budget_total_kz, spent_kz,
           max_payout_user_kz, max_posts_total, status,
           campaigns_channel_id, post_message_id,
           category_id, details_channel_id, requirements_channel_id,
           submit_channel_id, submit_panel_message_id,
           leaderboard_channel_id, leaderboard_message_id
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
           leaderboard_channel_id, leaderboard_message_id
    FROM campaigns WHERE id=?
    """, (campaign_id,))
    return cur.fetchone()

# =========================
# UI VIEWS (com custom_id únicos por prefix)
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
    v.add_item(discord.ui.Button(label="📥 Submeter vídeo", style=discord.ButtonStyle.primary, custom_id=f"vz:submit:open:{campaign_id}"))
    v.add_item(discord.ui.Button(label="📊 Ver estatísticas", style=discord.ButtonStyle.secondary, custom_id=f"vz:submit:stats:{campaign_id}"))
    return v

def verify_approval_view(user_id: int) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(discord.ui.Button(label="✅ Aprovar", style=discord.ButtonStyle.green, custom_id=f"vz:verify:approve:{user_id}"))
    v.add_item(discord.ui.Button(label="❌ Rejeitar", style=discord.ButtonStyle.red, custom_id=f"vz:verify:reject:{user_id}"))
    return v

def submission_approval_view(submission_id: int) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(discord.ui.Button(label="✅ Aprovar vídeo", style=discord.ButtonStyle.green, custom_id=f"vz:sub:approve:{submission_id}"))
    v.add_item(discord.ui.Button(label="❌ Rejeitar vídeo", style=discord.ButtonStyle.red, custom_id=f"vz:sub:reject:{submission_id}"))
    return v

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

        await _safe_ephemeral(
            interaction,
            "✅ Pedido enviado!\n\n"
            f"📱 Rede: {self.social}\n"
            f"👤 Username: {username}\n"
            f"🔑 Código: {self.code}\n\n"
            "🔒 Coloca este código na tua BIO para confirmar.\n"
            "⏳ Depois disso, aguarda aprovação do staff."
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
            return await _safe_ephemeral(interaction, "⚠️ Servidor não encontrado.")
        member = await fetch_member_safe(guild, interaction.user.id)
        if not member or not is_verified(member):
            return await _safe_ephemeral(interaction, "⛔ Tens de estar **Verificado** para guardar IBAN.")
        set_iban(interaction.user.id, str(self.iban.value).strip())
        await _safe_ephemeral(interaction, "✅ IBAN guardado com sucesso.")

class SubmitVideoModal(discord.ui.Modal):
    def __init__(self, campaign_id: int):
        super().__init__(title="Submeter vídeo TikTok")
        self.campaign_id = int(campaign_id)
        self.url = discord.ui.TextInput(
            label="Link do teu vídeo TikTok",
            placeholder="https://www.tiktok.com/@.../video/...",
            required=True,
            max_length=300
        )
        self.add_item(self.url)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild or bot.get_guild(SERVER_ID)
        if not guild:
            return await _safe_ephemeral(interaction, "⚠️ Servidor não encontrado.")

        member = await fetch_member_safe(guild, interaction.user.id)
        if not member or not is_verified(member):
            return await _safe_ephemeral(interaction, "⛔ Tens de estar **Verificado** para submeter vídeos.")

        conn = db_conn()
        row = get_campaign_by_id(conn, self.campaign_id)
        if not row:
            conn.close()
            return await _safe_ephemeral(interaction, "❌ Campanha não encontrada.")
        if row[11] != "active":
            conn.close()
            return await _safe_ephemeral(interaction, "⚠️ Esta campanha já terminou.")

        url = str(self.url.value).strip()
        if not url.startswith("http://") and not url.startswith("https://"):
            conn.close()
            return await _safe_ephemeral(interaction, "❌ Link inválido. Envia um link completo com **https://**")

        vid = tiktok_extract_video_id(url)
        now = _now()

        cur = conn.cursor()
        try:
            cur.execute("""
            INSERT INTO submissions (campaign_id, user_id, tiktok_url, tiktok_video_id, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
            """, (self.campaign_id, interaction.user.id, url, vid, now))
            submission_id = int(cur.lastrowid)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return await _safe_ephemeral(interaction, "⚠️ Este vídeo já foi submetido nesta campanha.")

        # manda para aprovações (staff)
        appr = guild.get_channel(VERIFICACOES_CHANNEL_ID)
        if appr:
            await appr.send(
                f"📥 **Novo vídeo submetido**\n"
                f"🆔 Submission: `{submission_id}`\n"
                f"🎯 Campanha ID: `{self.campaign_id}`\n"
                f"👤 User: {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"🔗 {url}\n"
                f"📌 Status: **PENDENTE**",
                view=submission_approval_view(submission_id)
            )

        conn.close()
        await _safe_ephemeral(interaction, "✅ Vídeo submetido! Aguarda aprovação do staff.")

# =========================
# CAMPANHAS: comandos
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

@commands.has_permissions(administrator=True)
@bot.command()
async def campaign_test(ctx):
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

    ch = ctx.guild.get_channel(CAMPANHAS_CHANNEL_ID)
    if not ch:
        return await ctx.send("❌ Canal de campanhas não encontrado.")

    if not post_msg_id:
        msg = await ch.send(campaign_post_text(c), view=JoinCampaignView())
        conn2 = db_conn()
        cur2 = conn2.cursor()
        cur2.execute("UPDATE campaigns SET post_message_id=? WHERE slug=?", (msg.id, c["slug"]))
        conn2.commit()
        conn2.close()

    await ctx.send("✅ Campanha teste publicada em #campanhas.")

@commands.has_permissions(administrator=True)
@bot.command(name="campanha")
async def campanha(ctx):
    await ctx.invoke(bot.get_command("campaign_test"))

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

    cur.execute("""
    SELECT user_id, paid_kz, total_views_paid
    FROM campaign_users
    WHERE campaign_id=?
    ORDER BY paid_kz DESC
    LIMIT 10
    """, (campaign_id,))
    top = cur.fetchall()
    conn.close()

    if not lb_ch_id or not lb_msg_id:
        return

    ch = guild.get_channel(int(lb_ch_id))
    if not ch:
        return

    lines = [f"🏆 **LEADERBOARD — {name}**",
             f"💰 **Gasto:** {spent:,}/{budget:,} Kz | 📌 **Estado:** {status}",
             ""]
    if not top:
        lines.append("Ainda sem pagamentos/atualizações.")
    else:
        for i, (uid, paid_kz, views_paid) in enumerate(top, 1):
            lines.append(f"**{i}.** <@{uid}> — **{paid_kz:,} Kz** | views pagas: **{views_paid:,}**")

    content = "\n".join(lines)

    try:
        msg = await ch.fetch_message(int(lb_msg_id))
        await msg.edit(content=content)
    except:
        pass

# =========================
# APIFY: obter views
# =========================
async def apify_get_views_for_url(url: str) -> Optional[int]:
    if not APIFY_TOKEN:
        return None

    run_url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/runs?token={APIFY_TOKEN}"
    # input comum do clockworks/tiktok-scraper
    payload = {
        "startUrls": [{"url": url}],
        "maxItems": 1
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(run_url, json=payload) as r:
                data = await r.json()
                run = data.get("data", {})
                run_id = run.get("id")
                dataset_id = run.get("defaultDatasetId")
                if not run_id or not dataset_id:
                    return None

            # esperar fim (poll)
            for _ in range(20):
                async with session.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
                ) as rr:
                    rd = await rr.json()
                    status = (rd.get("data", {}) or {}).get("status")
                    if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                        break
                await asyncio.sleep(3)

            if status != "SUCCEEDED":
                return None

            async with session.get(
                f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&clean=true&limit=1"
            ) as ri:
                items = await ri.json()

            if not items or not isinstance(items, list):
                return None

            item = items[0]
            # campos comuns possíveis
            for key in ("playCount", "play_count", "stats.playCount", "stats.play_count"):
                # suporte simples
                if key in item and isinstance(item[key], int):
                    return item[key]
            # tenta alternativas
            stats = item.get("stats") or {}
            if isinstance(stats, dict) and isinstance(stats.get("playCount"), int):
                return stats["playCount"]
            return None

    except Exception as e:
        print("⚠️ APIFY erro:", e)
        return None

# =========================
# VIEWS LOOP (pagamento por blocos de 1000)
# =========================
@tasks.loop(minutes=VIEWS_REFRESH_MINUTES)
async def refresh_views_loop():
    try:
        conn = db_conn()
        cur = conn.cursor()
        cur.execute("""
        SELECT s.id, s.campaign_id, s.user_id, s.tiktok_url, s.views_current, s.paid_views,
               c.rate_kz_per_1k, c.budget_total_kz, c.spent_kz, c.max_payout_user_kz, c.status
        FROM submissions s
        JOIN campaigns c ON c.id = s.campaign_id
        WHERE s.status='approved' AND c.status='active'
        """)
        rows = cur.fetchall()
        conn.close()

        touched_campaigns = set()

        for (sub_id, camp_id, user_id, url, views_current, paid_views,
             rate, budget_total, spent_kz, max_user_kz, camp_status) in rows:

            views = await apify_get_views_for_url(url)
            if views is None:
                continue

            # atualiza views_current sempre
            conn2 = db_conn()
            cur2 = conn2.cursor()
            cur2.execute("UPDATE submissions SET views_current=? WHERE id=?", (int(views), int(sub_id)))

            # pagar só em blocos de 1000
            payable_total = (int(views) // 1000) * 1000
            to_pay_views = payable_total - int(paid_views)
            if to_pay_views < 1000:
                conn2.commit()
                conn2.close()
                continue

            # calcular pagamento e caps
            to_pay_kz = (to_pay_views // 1000) * int(rate)

            # cap por user
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
                # reduz views a pagar para caber
                max_blocks = remaining_user_kz // int(rate)
                to_pay_views = max_blocks * 1000
                to_pay_kz = max_blocks * int(rate)

            # cap por budget
            remaining_budget = max(0, int(budget_total) - int(spent_kz))
            if remaining_budget <= 0:
                # fecha campanha
                cur2.execute("UPDATE campaigns SET status='ended' WHERE id=?", (int(camp_id),))
                conn2.commit()
                conn2.close()
                touched_campaigns.add(int(camp_id))
                continue
            if to_pay_kz > remaining_budget:
                max_blocks = remaining_budget // int(rate)
                to_pay_views = max_blocks * 1000
                to_pay_kz = max_blocks * int(rate)

            if to_pay_kz <= 0:
                conn2.commit()
                conn2.close()
                continue

            # aplica pagamento
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

            conn2.commit()
            conn2.close()
            touched_campaigns.add(int(camp_id))

        # atualizar leaderboards tocados
        for cid in touched_campaigns:
            await update_leaderboard_for_campaign(cid)

    except Exception as e:
        print("⚠️ refresh_views_loop erro:", e)

@refresh_views_loop.before_loop
async def before_refresh_views():
    await bot.wait_until_ready()

# =========================
# ROUTER DE INTERAÇÕES (botões)
# =========================
@bot.event
async def on_interaction(interaction: discord.Interaction):
    try:
        if interaction.type != discord.InteractionType.component:
            return await bot.process_application_commands(interaction)

        data = interaction.data or {}
        cid = data.get("custom_id")
        if not cid:
            return

        # ---------- VERIFICAÇÃO ----------
        if cid == "vz:connect":
            v = discord.ui.View(timeout=120)
            select = discord.ui.Select(
                placeholder="Escolhe a rede social",
                options=[
                    discord.SelectOption(label="TikTok", emoji="🎵"),
                    discord.SelectOption(label="YouTube", emoji="📺"),
                    discord.SelectOption(label="Instagram", emoji="📸"),
                ],
                custom_id="vz:social_select"
            )
            async def _sel_callback(i: discord.Interaction):
                social = (i.data["values"][0] if i.data and "values" in i.data else "TikTok")
                code = generate_verification_code()
                await i.response.send_modal(UsernameModal(social=social, code=code))
            select.callback = _sel_callback
            v.add_item(select)
            return await _safe_ephemeral(interaction, "Escolhe a rede social:"), await interaction.followup.send(" ", view=v, ephemeral=True)

        if cid == "vz:view_account":
            row = get_verification_request(interaction.user.id)
            if not row:
                return await _safe_ephemeral(interaction, "❌ Nenhum pedido encontrado.")
            _, social, username, code, status, _, _ = row
            if status != "verified":
                msg = (
                    "⏳ **Conta ainda não verificada**\n"
                    f"📱 Rede: {social}\n"
                    f"🏷️ Username: {username}\n"
                    f"🔑 Código: `{code}`\n"
                    f"📌 Status: **{status.upper()}**"
                )
            else:
                msg = (
                    "✅ **Conta verificada**\n"
                    f"📱 Rede: {social}\n"
                    f"🏷️ Username: {username}\n"
                    f"🔑 Código: `{code}`"
                )
            return await _safe_ephemeral(interaction, msg)

        if cid.startswith("vz:verify:"):
            _, _, action, user_id = cid.split(":", 3)
            user_id = int(user_id)

            if interaction.user.id != ADMIN_USER_ID:
                return await _safe_ephemeral(interaction, "⛔ Só o admin pode aprovar/rejeitar.")

            row = get_verification_request(user_id)
            if not row:
                return await _safe_ephemeral(interaction, "⚠️ Este pedido já não existe no DB.")
            _, social, username, code, status, _, _ = row
            if status != "pending":
                return await _safe_ephemeral(interaction, f"⚠️ Pedido já está como **{status}**.")

            guild = bot.get_guild(SERVER_ID)
            if not guild:
                return await _safe_ephemeral(interaction, "⚠️ Guild não encontrada.")
            member = await fetch_member_safe(guild, user_id)
            if not member:
                return await _safe_ephemeral(interaction, "⚠️ Não consegui buscar o membro.")

            if action == "approve":
                role = guild.get_role(VERIFICADO_ROLE_ID)
                if not role:
                    return await _safe_ephemeral(interaction, "⚠️ Cargo 'Verificado' não encontrado.")
                try:
                    await member.add_roles(role, reason="Verificação aprovada")
                except discord.Forbidden:
                    return await _safe_ephemeral(interaction, "⛔ Bot sem permissões para dar cargo (Manage Roles + role acima).")

                set_verification_status(user_id, "verified")
                await notify_user(
                    member,
                    "✅ **Verificação aprovada!**\n"
                    f"📱 Rede: {social}\n"
                    f"🏷️ Username: {username}\n\n"
                    "👉 Agora adiciona o teu IBAN:\n"
                    f"• Vai ao canal <#{LIGAR_CONTA_E_VERIFICAR_CHANNEL_ID}> e usa **!ibanpanel**.",
                    fallback_channel_id=LIGAR_CONTA_E_VERIFICAR_CHANNEL_ID
                )
                try:
                    await interaction.message.edit(content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **APROVADO ✅**"), view=None)
                except:
                    pass
                return await _safe_ephemeral(interaction, "✅ Aprovado e cargo atribuído.")

            if action == "reject":
                set_verification_status(user_id, "rejected")
                await notify_user(
                    member,
                    "❌ **Verificação rejeitada.**\n"
                    f"📱 Rede: {social}\n"
                    f"🏷️ Username: {username}\n\n"
                    "✅ Confere se colocaste o **código na bio** e tenta outra vez.",
                    fallback_channel_id=LIGAR_CONTA_E_VERIFICAR_CHANNEL_ID
                )
                try:
                    await interaction.message.edit(content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **REJEITADO ❌**"), view=None)
                except:
                    pass
                return await _safe_ephemeral(interaction, "❌ Rejeitado.")

        # ---------- IBAN ----------
        if cid == "vz:iban:add":
            guild = interaction.guild or bot.get_guild(SERVER_ID)
            if not guild:
                return await _safe_ephemeral(interaction, "⚠️ Servidor não encontrado.")
            member = await fetch_member_safe(guild, interaction.user.id)
            if not member or not is_verified(member):
                return await _safe_ephemeral(interaction, "⛔ Tens de estar **Verificado** para adicionar IBAN.")
            return await interaction.response.send_modal(IbanModal())

        if cid == "vz:iban:view":
            guild = interaction.guild or bot.get_guild(SERVER_ID)
            if not guild:
                return await _safe_ephemeral(interaction, "⚠️ Servidor não encontrado.")
            member = await fetch_member_safe(guild, interaction.user.id)
            if not member or not is_verified(member):
                return await _safe_ephemeral(interaction, "⛔ Tens de estar **Verificado** para ver IBAN.")
            row = get_iban(interaction.user.id)
            if not row:
                return await _safe_ephemeral(interaction, "Ainda não tens IBAN guardado.")
            iban, updated_at = row
            return await _safe_ephemeral(interaction, f"✅ Teu IBAN: **{iban}**\n🕒 Atualizado: {updated_at}")

        # ---------- CAMPANHA JOIN ----------
        if cid == "vz:camp:join":
            guild = interaction.guild or bot.get_guild(SERVER_ID)
            if not guild:
                return await _safe_ephemeral(interaction, "⚠️ Servidor não encontrado.")
            member = await fetch_member_safe(guild, interaction.user.id)
            if not member or not is_verified(member):
                return await _safe_ephemeral(interaction, "⛔ Tens de estar **Verificado** para aderir.")

            post_id = interaction.message.id
            conn = db_conn()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, name, platforms, content_types, audio_url,
                       rate_kz_per_1k, budget_total_kz, spent_kz,
                       max_payout_user_kz, max_posts_total, status,
                       category_id, details_channel_id, requirements_channel_id,
                       submit_channel_id, submit_panel_message_id,
                       leaderboard_channel_id, leaderboard_message_id
                FROM campaigns
                WHERE post_message_id=?
            """, (post_id,))
            row = cur.fetchone()

            if not row:
                conn.close()
                return await _safe_ephemeral(interaction, "❌ Campanha não encontrada no DB. Admin: republica com `!campanha`.")

            (cid2, name, platforms, content_types, audio_url,
             rate, budget_total, spent_kz,
             max_user_kz, max_posts_total, status,
             category_id, details_id, req_id,
             submit_id, submit_panel_msg_id,
             lb_id, lb_msg_id) = row

            if status != "active":
                conn.close()
                return await _safe_ephemeral(interaction, "⚠️ Esta campanha já terminou.")

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                guild.get_role(VERIFICADO_ROLE_ID): discord.PermissionOverwrite(
                    view_channel=True, read_message_history=True, send_messages=False
                ),
                guild.me: discord.PermissionOverwrite(
                    view_channel=True, read_message_history=True, send_messages=True,
                    manage_channels=True, manage_messages=True
                )
            }
            admin_member = guild.get_member(ADMIN_USER_ID)
            if admin_member:
                overwrites[admin_member] = discord.PermissionOverwrite(
                    view_channel=True, read_message_history=True, send_messages=True, manage_messages=True
                )

            if not category_id:
                try:
                    category = await guild.create_category(f"🎯 {name}", overwrites=overwrites)
                    details_ch = await guild.create_text_channel("1-detalhes-da-campanha", category=category, overwrites=overwrites)
                    req_ch = await guild.create_text_channel("2-requisitos", category=category, overwrites=overwrites)
                    submit_ch = await guild.create_text_channel("3-submeter-videos", category=category, overwrites=overwrites)
                    lb_ch = await guild.create_text_channel("4-leaderboard", category=category, overwrites=overwrites)
                except discord.Forbidden:
                    conn.close()
                    return await _safe_ephemeral(interaction, "⛔ Bot sem permissão (Manage Channels).")

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

                submit_panel = await submit_ch.send(
                    "📤 **Submete os teus vídeos aqui**\n\nUsa os botões abaixo 👇",
                    view=submit_view(campaign_id=cid2)
                )
                lb_msg = await lb_ch.send("🏆 **LEADERBOARD**\n(aguarda atualizações automáticas)")

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
                """, (category.id, details_ch.id, req_ch.id, submit_ch.id, submit_panel.id, lb_ch.id, lb_msg.id, cid2))
                conn.commit()

            conn.close()
            return await _safe_ephemeral(interaction, "✅ Aderiste à campanha! Vai à categoria da campanha para submeter.")

        # ---------- SUBMIT ----------
        if cid.startswith("vz:submit:"):
            _, _, action, camp_id = cid.split(":", 3)
            camp_id = int(camp_id)

            if action == "open":
                return await interaction.response.send_modal(SubmitVideoModal(camp_id))

            if action == "stats":
                conn = db_conn()
                cur = conn.cursor()
                cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(views_current),0), COALESCE(SUM(paid_views),0)
                FROM submissions
                WHERE campaign_id=? AND user_id=? AND status IN ('approved','frozen')
                """, (camp_id, interaction.user.id))
                posts, views, paid_views = cur.fetchone()

                cur.execute("""
                SELECT COALESCE(paid_kz,0) FROM campaign_users
                WHERE campaign_id=? AND user_id=?
                """, (camp_id, interaction.user.id))
                row = cur.fetchone()
                paid_kz = row[0] if row else 0

                cur.execute("""
                SELECT budget_total_kz, spent_kz, max_payout_user_kz, status
                FROM campaigns WHERE id=?
                """, (camp_id,))
                r2 = cur.fetchone()
                conn.close()
                if not r2:
                    return await _safe_ephemeral(interaction, "❌ Campanha não encontrada.")
                bt, sk, mx, st = r2
                return await _safe_ephemeral(
                    interaction,
                    f"📊 **As tuas stats (campanha {camp_id})**\n"
                    f"• Posts aprovados: **{posts}**\n"
                    f"• Views atuais somadas: **{views:,}**\n"
                    f"• Views já pagas: **{paid_views:,}**\n"
                    f"• Ganho estimado: **{paid_kz:,} Kz** (máx {mx:,} Kz)\n\n"
                    f"💰 Campanha: **{sk:,}/{bt:,} Kz**\n"
                    f"📌 Estado: **{st}**"
                )

        # ---------- APPROVAL SUBMISSION ----------
        if cid.startswith("vz:sub:"):
            if interaction.user.id != ADMIN_USER_ID:
                return await _safe_ephemeral(interaction, "⛔ Só o admin pode aprovar/rejeitar.")

            _, _, action, sub_id = cid.split(":", 3)
            sub_id = int(sub_id)

            conn = db_conn()
            cur = conn.cursor()
            cur.execute("""
            SELECT id, campaign_id, user_id, tiktok_url, status
            FROM submissions WHERE id=?
            """, (sub_id,))
            srow = cur.fetchone()
            if not srow:
                conn.close()
                return await _safe_ephemeral(interaction, "❌ Submission não encontrada.")

            _, camp_id, user_id, url, st = srow
            if st != "pending":
                conn.close()
                return await _safe_ephemeral(interaction, f"⚠️ Já está como **{st}**.")

            if action == "approve":
                cur.execute("UPDATE submissions SET status='approved', approved_at=? WHERE id=?", (_now(), sub_id))
                conn.commit()
                conn.close()
                try:
                    await interaction.message.edit(content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **APROVADO ✅**"), view=None)
                except:
                    pass
                return await _safe_ephemeral(interaction, "✅ Vídeo aprovado.")

            if action == "reject":
                cur.execute("UPDATE submissions SET status='rejected' WHERE id=?", (sub_id,))
                conn.commit()
                conn.close()
                try:
                    await interaction.message.edit(content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **REJEITADO ❌**"), view=None)
                except:
                    pass
                return await _safe_ephemeral(interaction, "❌ Vídeo rejeitado.")

    except Exception as e:
        print("⚠️ on_interaction erro:", e)

# =========================
# READY
# =========================
@bot.event
async def on_ready():
    init_db()

    # views persistentes (as base)
    if not getattr(bot, "_views_added", False):
        bot.add_view(MainView())
        bot.add_view(IbanButtons())
        bot.add_view(JoinCampaignView())
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

# (no Render worker não precisas, mas não atrapalha)
# keep_alive()

# =========================
# RUN
# =========================
raw = os.getenv("DISCORD_TOKEN")
if not raw:
    raise RuntimeError("DISCORD_TOKEN está vazio/None no Render Environment.")
TOKEN = raw.strip().strip('"').strip("'")
bot.run(TOKEN)
