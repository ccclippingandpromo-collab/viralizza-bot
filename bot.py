import os
import re
import time
import sqlite3
import threading
import asyncio
import secrets
import string
from typing import Optional

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
# DICA: Em Render, mete DB_PATH para um disco persistente (ex: /var/data/database.sqlite3)
DB_PATH = os.getenv("DB_PATH", "database.sqlite3")

# ====== TikTok Views Provider (Apify) ======
APIFY_TOKEN = os.getenv("APIFY_TOKEN")  # obrigatório para tracking
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "clockworks/tiktok-scraper")

# Onde cai aprovação/rejeição de vídeos
CAMPANHAS_APROVACAO_CHANNEL_ID = VERIFICACOES_CHANNEL_ID

print("DISCORD VERSION:", getattr(discord, "__version__", "unknown"))
print("DISCORD FILE:", getattr(discord, "__file__", "unknown"))
print("DB_PATH:", DB_PATH)
print("APIFY_TOKEN set?:", bool(APIFY_TOKEN))

# =========================
# BOT / INTENTS
# =========================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.messages = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# HELPERS
# =========================
def _now() -> int:
    return int(time.time())


def db_conn():
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
    except:
        pass


def _get_interaction(a, b):
    if isinstance(a, discord.Interaction):
        return a
    if isinstance(b, discord.Interaction):
        return b
    return None


def _safe_button_pair(a, b):
    interaction = _get_interaction(a, b)
    button = None
    if isinstance(a, discord.ui.Button):
        button = a
    elif isinstance(b, discord.ui.Button):
        button = b
    return interaction, button


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


def delete_iban(user_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM ibans WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()


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


def delete_verification_request(user_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM verification_requests WHERE user_id=?", (user_id,))
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
# CAMPANHAS
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
    est_views = int(c["budget_total_kz"] / c["rate_kz_per_1k"] * 1000)
    return (
        f"📊 **Plataformas:** {c['platforms']}\n\n"
        f"🎥 **Tipo:** {c['content_types'].replace(',', ', ')}\n\n"
        f"💸 **Taxa:** {c['rate_kz_per_1k']} Kz / 1000 visualizações\n\n"
        f"💰 **Budget:** {c['budget_total_kz']:,} Kz (≈ {est_views:,} views)\n"
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


def can_approve(conn, campaign_id: int, user_id: int):
    cur = conn.cursor()
    cur.execute("""
    SELECT budget_total_kz, spent_kz, max_posts_total, max_payout_user_kz, rate_kz_per_1k, status
    FROM campaigns WHERE id=?
    """, (campaign_id,))
    row = cur.fetchone()
    if not row:
        return False, "Campanha não encontrada."

    budget_total, spent_kz, max_posts_total, max_user_kz, rate, status = row

    if status != "active" or spent_kz >= budget_total:
        return False, "Campanha já terminou (budget esgotado)."

    cur.execute("SELECT COUNT(*) FROM submissions WHERE campaign_id=? AND status='approved'", (campaign_id,))
    approved_count = cur.fetchone()[0]
    if approved_count >= max_posts_total:
        return False, f"Limite atingido: {max_posts_total} vídeos já aprovados nesta campanha."

    cur.execute("SELECT paid_kz FROM campaign_users WHERE campaign_id=? AND user_id=?", (campaign_id, user_id))
    r2 = cur.fetchone()
    user_paid = r2[0] if r2 else 0
    if user_paid >= max_user_kz:
        return False, "Já atingiste o pagamento máximo nesta campanha."

    return True, "OK"


def update_one_submission_payment(conn, submission_id: int):
    cur = conn.cursor()
    cur.execute("""
    SELECT
        s.id, s.campaign_id, s.user_id, s.views_current, s.paid_views,
        c.budget_total_kz, c.spent_kz, c.rate_kz_per_1k, c.max_payout_user_kz, c.status,
        COALESCE(u.paid_kz,0)
    FROM submissions s
    JOIN campaigns c ON c.id = s.campaign_id
    LEFT JOIN campaign_users u ON u.campaign_id=s.campaign_id AND u.user_id=s.user_id
    WHERE s.id=? AND s.status='approved'
    """, (submission_id,))
    row = cur.fetchone()
    if not row:
        return

    (sid, cid, uid, views_current, paid_views,
     budget_total, spent_kz, rate, max_user_kz, status,
     user_paid_kz) = row

    if status != "active":
        return

    remaining_campaign = budget_total - spent_kz
    if remaining_campaign <= 0:
        cur.execute("UPDATE campaigns SET status='closed' WHERE id=?", (cid,))
        conn.commit()
        return

    remaining_user = max_user_kz - user_paid_kz
    if remaining_user <= 0:
        cur.execute("UPDATE submissions SET status='frozen' WHERE id=?", (sid,))
        conn.commit()
        return

    new_views = max(0, int(views_current) - int(paid_views))
    blocks = new_views // 1000
    if blocks <= 0:
        return

    cap_kz = min(remaining_campaign, remaining_user)
    max_blocks_by_money = cap_kz // rate
    blocks_payable = min(blocks, max_blocks_by_money)
    if blocks_payable <= 0:
        return

    pay_kz = blocks_payable * rate
    pay_views = blocks_payable * 1000

    cur.execute("UPDATE submissions SET paid_views = paid_views + ? WHERE id=?", (pay_views, sid))

    cur.execute("""
    INSERT INTO campaign_users (campaign_id, user_id, paid_kz, total_views_paid)
    VALUES (?,?,0,0)
    ON CONFLICT(campaign_id, user_id) DO NOTHING
    """, (cid, uid))

    cur.execute("""
    UPDATE campaign_users
    SET paid_kz = paid_kz + ?, total_views_paid = total_views_paid + ?
    WHERE campaign_id=? AND user_id=?
    """, (pay_kz, pay_views, cid, uid))

    cur.execute("UPDATE campaigns SET spent_kz = spent_kz + ? WHERE id=?", (pay_kz, cid))

    cur.execute("SELECT budget_total_kz, spent_kz FROM campaigns WHERE id=?", (cid,))
    bt, sk = cur.fetchone()
    if sk >= bt:
        cur.execute("UPDATE campaigns SET status='closed' WHERE id=?", (cid,))

    conn.commit()


async def update_leaderboard_message(guild: discord.Guild, campaign_id: int):
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT name, budget_total_kz, spent_kz, status,
           leaderboard_channel_id, leaderboard_message_id
    FROM campaigns WHERE id=?
    """, (campaign_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return

    name, bt, sk, status, lb_ch_id, lb_msg_id = row
    if not lb_ch_id or not lb_msg_id:
        conn.close()
        return

    cur.execute("""
    SELECT user_id, COALESCE(SUM(views_current),0) as v
    FROM submissions
    WHERE campaign_id=? AND status IN ('approved','frozen')
    GROUP BY user_id
    ORDER BY v DESC
    LIMIT 10
    """, (campaign_id,))
    top = cur.fetchall()
    conn.close()

    pct = 0 if bt == 0 else int((sk / bt) * 100)
    remaining = max(0, bt - sk)

    lines = [f"🏆 **LEADERBOARD — {name}**\n"]
    if top:
        for i, (uid, v) in enumerate(top, start=1):
            lines.append(f"{i}. <@{uid}> — **{int(v):,}** views")
    else:
        lines.append("*(ainda sem vídeos aprovados / sem views atualizadas)*")

    lines.append("\n📊 **Progresso da campanha:**")
    lines.append(f"**{pct}%** | **{sk:,}/{bt:,} Kz**")
    lines.append(f"💰 **Budget restante:** **{remaining:,} Kz**")

    if status != "active":
        lines.append("\n🔒 **Campanha encerrada.**")

    text = "\n".join(lines)

    lb_ch = guild.get_channel(int(lb_ch_id))
    if not lb_ch:
        return
    try:
        msg = await lb_ch.fetch_message(int(lb_msg_id))
        await msg.edit(content=text)
    except:
        pass


# =========================
# TikTok views via Apify
# =========================
async def fetch_tiktok_views_apify(session: aiohttp.ClientSession, url: str):
    if not APIFY_TOKEN:
        return None

    run_url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/runs?token={APIFY_TOKEN}"
    payload = {
        "startUrls": [{"url": url}],
        "resultsPerPage": 1,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSlideshowImages": False,
    }

    async with session.post(run_url, json=payload, timeout=60) as r:
        if r.status >= 300:
            return None
        data = await r.json()
        run_id = data.get("data", {}).get("id")
        if not run_id:
            return None

    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    st = None
    for _ in range(12):
        async with session.get(status_url, timeout=30) as r:
            if r.status >= 300:
                return None
            st = await r.json()
            status = st.get("data", {}).get("status")
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
        await asyncio.sleep(5)

    if not st:
        return None

    dataset_id = st.get("data", {}).get("defaultDatasetId")
    if not dataset_id:
        return None

    items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true&limit=1&token={APIFY_TOKEN}"
    async with session.get(items_url, timeout=60) as r:
        if r.status >= 300:
            return None
        items = await r.json()

    if not items:
        return None

    item = items[0]
    if isinstance(item, dict):
        if isinstance(item.get("playCount"), (int, float)):
            return int(item["playCount"])
        stats = item.get("stats") or item.get("statistics")
        if isinstance(stats, dict) and isinstance(stats.get("playCount"), (int, float)):
            return int(stats["playCount"])
        if isinstance(stats, dict) and isinstance(stats.get("play_count"), (int, float)):
            return int(stats["play_count"])

    return None


# =========================
# CAMPANHAS: UI (SUBMIT)
# =========================
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
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return await _safe_ephemeral(interaction, "⚠️ Este vídeo já foi submetido nesta campanha.")

        appr = guild.get_channel(CAMPANHAS_APROVACAO_CHANNEL_ID)
        if appr:
            view = VideoApprovalView(
                campaign_id=self.campaign_id,
                submitter_id=interaction.user.id,
                tiktok_url=url
            )
            await appr.send(
                f"📥 **Novo vídeo submetido**\n"
                f"🎯 Campanha ID: `{self.campaign_id}`\n"
                f"👤 User: {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"🔗 {url}\n"
                f"📌 Status: **PENDENTE**",
                view=view
            )

        conn.close()
        await _safe_ephemeral(interaction, "✅ Vídeo submetido! Aguarda aprovação do staff.")


class SubmitView(discord.ui.View):
    """
    BOTÕES COM custom_id ÚNICO POR CAMPANHA (evita falhas).
    """
    def __init__(self, campaign_id: int):
        super().__init__(timeout=None)
        self.campaign_id = int(campaign_id)

        self.add_item(discord.ui.Button(
            label="📥 Submeter vídeo",
            style=discord.ButtonStyle.primary,
            custom_id=f"camp_submit_video_{self.campaign_id}"
        ))
        self.add_item(discord.ui.Button(
            label="📊 Ver estatísticas",
            style=discord.ButtonStyle.secondary,
            custom_id=f"camp_view_stats_{self.campaign_id}"
        ))


@bot.event
async def on_interaction(interaction: discord.Interaction):
    """
    Router para os botões dinâmicos do SubmitView.
    """
    if interaction.type != discord.InteractionType.component:
        return

    custom_id = None
    try:
        custom_id = interaction.data.get("custom_id")
    except:
        custom_id = None

    if not custom_id:
        return

    if custom_id.startswith("camp_submit_video_"):
        try:
            cid = int(custom_id.split("_")[-1])
        except:
            return
        await interaction.response.send_modal(SubmitVideoModal(cid))
        return

    if custom_id.startswith("camp_view_stats_"):
        try:
            cid = int(custom_id.split("_")[-1])
        except:
            return

        conn = db_conn()
        cur = conn.cursor()

        cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(views_current),0), COALESCE(SUM(paid_views),0)
        FROM submissions
        WHERE campaign_id=? AND user_id=? AND status IN ('approved','frozen')
        """, (cid, interaction.user.id))
        posts, views, paid_views = cur.fetchone()

        cur.execute("""
        SELECT COALESCE(paid_kz,0) FROM campaign_users
        WHERE campaign_id=? AND user_id=?
        """, (cid, interaction.user.id))
        row = cur.fetchone()
        paid_kz = row[0] if row else 0

        cur.execute("""
        SELECT budget_total_kz, spent_kz, max_payout_user_kz, status
        FROM campaigns WHERE id=?
        """, (cid,))
        r2 = cur.fetchone()
        conn.close()

        if not r2:
            return await _safe_ephemeral(interaction, "❌ Campanha não encontrada.")

        bt, sk, mx, st = r2

        await _safe_ephemeral(
            interaction,
            f"📊 **As tuas stats (campanha {cid})**\n"
            f"• Posts aprovados: **{posts}**\n"
            f"• Views atuais somadas: **{views:,}**\n"
            f"• Views já pagas: **{paid_views:,}**\n"
            f"• Ganho estimado: **{paid_kz:,} Kz** (máx {mx:,} Kz)\n\n"
            f"💰 Campanha: **{sk:,}/{bt:,} Kz**\n"
            f"📌 Estado: **{st}**"
        )
        return


class VideoApprovalView(discord.ui.View):
    def __init__(self, campaign_id: int, submitter_id: int, tiktok_url: str):
        super().__init__(timeout=None)
        self.campaign_id = int(campaign_id)
        self.submitter_id = int(submitter_id)
        self.tiktok_url = str(tiktok_url)

    async def _only_admin(self, interaction: discord.Interaction):
        if interaction.user.id != ADMIN_USER_ID:
            await _safe_ephemeral(interaction, "⛔ Só o admin pode aprovar/rejeitar.")
            return False
        return True

    @discord.ui.button(label="✅ Aprovar vídeo", style=discord.ButtonStyle.green, custom_id="camp_approve_video")
    async def approve(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return
        if not await self._only_admin(interaction):
            return

        conn = db_conn()
        ok, msg = can_approve(conn, self.campaign_id, self.submitter_id)
        if not ok:
            conn.close()
            for child in self.children:
                child.disabled = True
            try:
                await interaction.message.edit(
                    content=interaction.message.content.replace(
                        "📌 Status: **PENDENTE**",
                        f"📌 Status: **RECUSADO**\nMotivo: {msg}"
                    ),
                    view=self
                )
            except:
                pass
            return await _safe_ephemeral(interaction, f"❌ Não aprovado: {msg}")

        cur = conn.cursor()
        now = _now()
        cur.execute("""
        UPDATE submissions
        SET status='approved', approved_at=?
        WHERE campaign_id=? AND user_id=? AND tiktok_url=? AND status='pending'
        """, (now, self.campaign_id, self.submitter_id, self.tiktok_url))
        conn.commit()
        conn.close()

        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(
                content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **APROVADO ✅**"),
                view=self
            )
        except:
            pass

        await _safe_ephemeral(interaction, "✅ Vídeo aprovado e entrou no tracking automático.")

    @discord.ui.button(label="❌ Rejeitar vídeo", style=discord.ButtonStyle.red, custom_id="camp_reject_video")
    async def reject(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return
        if not await self._only_admin(interaction):
            return

        conn = db_conn()
        cur = conn.cursor()
        cur.execute("""
        UPDATE submissions
        SET status='rejected'
        WHERE campaign_id=? AND user_id=? AND tiktok_url=? AND status='pending'
        """, (self.campaign_id, self.submitter_id, self.tiktok_url))
        conn.commit()
        conn.close()

        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(
                content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **REJEITADO ❌**"),
                view=self
            )
        except:
            pass

        await _safe_ephemeral(interaction, "❌ Vídeo rejeitado.")


# =========================
# CAMPANHAS: JOIN
# =========================
class JoinCampaignView(discord.ui.View):
    """
    Encontra campanha pelo post_message_id
    """
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🔥 Aderir à Campanha", style=discord.ButtonStyle.success, custom_id="campaign_join_btn")
    async def join(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return

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
            return await _safe_ephemeral(
                interaction,
                "❌ Campanha não encontrada na base de dados.\n"
                "➡️ Admin: republica a campanha com `!campanha`."
            )

        (cid, name, platforms, content_types, audio_url,
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
                view_channel=True,
                read_message_history=True,
                send_messages=False
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True
            )
        }
        admin_member = guild.get_member(ADMIN_USER_ID)
        if admin_member:
            overwrites[admin_member] = discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                manage_messages=True
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
                return await _safe_ephemeral(
                    interaction,
                    "⛔ Falta permissão ao bot para criar categoria/canais.\n"
                    "✅ Dá ao bot: **Manage Channels**."
                )

            c = {
                "name": name,
                "platforms": platforms,
                "content_types": content_types,
                "audio_url": audio_url,
                "rate_kz_per_1k": rate,
                "budget_total_kz": budget_total,
                "max_payout_user_kz": max_user_kz,
                "max_posts_total": max_posts_total,
            }

            await details_ch.send(details_channel_text(c))
            await req_ch.send(requirements_text(c))

            submit_panel = await submit_ch.send(
                "📤 **Submete os teus vídeos aqui**\n\nUsa os botões abaixo 👇",
                view=SubmitView(campaign_id=cid)
            )
            lb_msg = await lb_ch.send("🏆 **LEADERBOARD**\n*(à espera de dados...)*")

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
            """, (category.id, details_ch.id, req_ch.id, submit_ch.id, submit_panel.id, lb_ch.id, lb_msg.id, cid))
            conn.commit()

        conn.close()
        await _safe_ephemeral(interaction, "✅ Aderiste à campanha! Vai à categoria da campanha para submeter.")


# =========================
# Tracking loop + Manual refresh
# =========================
async def run_tracking_once(guild: discord.Guild, only_campaign_id: Optional[int] = None) -> str:
    if not APIFY_TOKEN:
        return "⚠️ APIFY_TOKEN não definido — não dá para contar views."

    conn = db_conn()
    cur = conn.cursor()

    if only_campaign_id:
        cur.execute("SELECT id FROM campaigns WHERE status='active' AND id=?", (int(only_campaign_id),))
    else:
        cur.execute("SELECT id FROM campaigns WHERE status='active'")

    active_campaigns = {int(r[0]) for r in cur.fetchall()}
    if not active_campaigns:
        conn.close()
        return "✅ Done. Sem campanhas ativas."

    if only_campaign_id:
        cur.execute("""
        SELECT id, campaign_id, tiktok_url
        FROM submissions
        WHERE status='approved' AND campaign_id=?
        """, (int(only_campaign_id),))
    else:
        cur.execute("""
        SELECT id, campaign_id, tiktok_url
        FROM submissions
        WHERE status='approved'
        """)

    subs = cur.fetchall()
    conn.close()

    if not subs:
        for camp_id in active_campaigns:
            await update_leaderboard_message(guild, int(camp_id))
        return "✅ Done. Sem submissões aprovadas para atualizar."

    updated = 0
    async with aiohttp.ClientSession() as session:
        for sub_id, camp_id, url in subs:
            camp_id = int(camp_id)
            if camp_id not in active_campaigns:
                continue

            views = await fetch_tiktok_views_apify(session, url)
            if views is None:
                continue

            conn2 = db_conn()
            cur2 = conn2.cursor()
            cur2.execute("UPDATE submissions SET views_current=? WHERE id=?", (int(views), int(sub_id)))
            conn2.commit()
            update_one_submission_payment(conn2, int(sub_id))
            conn2.close()
            updated += 1

    for camp_id in list(active_campaigns):
        await update_leaderboard_message(guild, int(camp_id))

    return f"✅ Done. Atualizei views em **{updated}** submissões."


@tasks.loop(minutes=15)
async def track_campaign_views_loop():
    guild = bot.get_guild(SERVER_ID)
    if not guild:
        return
    msg = await run_tracking_once(guild)
    print("[TRACK LOOP]", msg)


@commands.has_permissions(administrator=True)
@bot.command()
async def refreshviews(ctx, campaign_id: int = None):
    if ctx.guild and ctx.guild.id != SERVER_ID:
        return
    await ctx.send("⏳ A atualizar views/leaderboard...")
    guild = ctx.guild or bot.get_guild(SERVER_ID)
    res = await run_tracking_once(guild, only_campaign_id=campaign_id)
    await ctx.send(res)


# =========================
# SUPORTE (mantido igual ao teu)
# =========================
async def criar_ticket(interaction: discord.Interaction, tipo: str, conteudo: str):
    staff_channel = interaction.client.get_channel(SUPORTE_STAFF_CHANNEL_ID)
    if not staff_channel:
        await _safe_ephemeral(interaction, "❌ Canal de suporte do staff não encontrado.")
        return

    msg = await staff_channel.send(
        f"🎫 **Novo Ticket**\n"
        f"👤 User: {interaction.user.mention} (`{interaction.user.id}`)\n"
        f"🧾 Tipo: **{tipo}**\n\n"
        f"📩 **Mensagem:**\n{conteudo}\n\n"
        f"🟢 Staff: respondam no **thread** abaixo para a resposta voltar ao user."
    )

    try:
        thread = await msg.create_thread(
            name=f"ticket-{interaction.user.name}-{interaction.user.id}",
            auto_archive_duration=1440
        )
    except discord.Forbidden:
        await _safe_ephemeral(
            interaction,
            "❌ O bot não tem permissão para criar threads no canal suporte-staff.\n"
            "Dá ao bot: **Create Public Threads / Create Private Threads / Send Messages / Manage Threads**."
        )
        return

    set_ticket(thread.id, interaction.user.id)

    try:
        await interaction.user.send(
            "✅ **Ticket aberto com o staff!**\n\n"
            "Responde **aqui por DM** e eu vou encaminhar ao staff.\n"
            "Quando o staff responder, vais receber aqui também.\n\n"
            "⚠️ Se não receberes DMs: abre as DMs do servidor."
        )
    except:
        await thread.send("⚠️ Não consegui enviar DM ao user (DMs fechadas).")

    await _safe_ephemeral(interaction, "✅ Pedido enviado ao staff! Verifica as tuas DMs para continuar.")
    await thread.send("🟢 Ticket aberto. Staff respondam aqui.")


class CampanhaModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Problema sobre campanha")
        self.campanha = discord.ui.TextInput(label="Nome da campanha", required=True, max_length=80)
        self.problema = discord.ui.TextInput(label="Qual é o problema?", style=discord.TextStyle.paragraph, required=True, max_length=1000)
        self.add_item(self.campanha)
        self.add_item(self.problema)

    async def on_submit(self, interaction: discord.Interaction):
        texto = f"📢 Campanha: {self.campanha.value}\n⚠️ Problema: {self.problema.value}"
        await criar_ticket(interaction, "Problema com campanha", texto)


class DuvidaModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Dúvidas")
        self.duvida = discord.ui.TextInput(label="Escreve a tua dúvida", style=discord.TextStyle.paragraph, required=True, max_length=1000)
        self.add_item(self.duvida)

    async def on_submit(self, interaction: discord.Interaction):
        await criar_ticket(interaction, "Dúvida", self.duvida.value)


class SuporteView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📢 Problema sobre campanha", style=discord.ButtonStyle.danger, custom_id="support_btn_campaign")
    async def btn_campaign(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return
        await interaction.response.send_modal(CampanhaModal())

    @discord.ui.button(label="❓ Dúvidas", style=discord.ButtonStyle.primary, custom_id="support_btn_question")
    async def btn_question(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return
        await interaction.response.send_modal(DuvidaModal())


# =========================
# VERIFICAÇÃO + IBAN (mantido igual ao teu)
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

        await interaction.response.send_message(
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

        view = ApprovalView(target_user_id=user_id)
        msg = await channel.send(
            f"🆕 **Novo pedido de verificação**\n"
            f"👤 User: {interaction.user.mention} (`{user_id}`)\n"
            f"📱 Rede: **{self.social}**\n"
            f"🏷️ Username: **{username}**\n"
            f"🔑 Código: `{self.code}`\n"
            f"📌 Status: **PENDENTE**",
            view=view
        )
        set_verification_message(user_id=user_id, channel_id=channel.id, message_id=msg.id)


class SocialSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="TikTok", emoji="🎵"),
            discord.SelectOption(label="YouTube", emoji="📺"),
            discord.SelectOption(label="Instagram", emoji="📸"),
        ]
        super().__init__(placeholder="Escolhe a rede social", min_values=1, max_values=1, options=options, custom_id="social_select")

    async def callback(self, interaction: discord.Interaction):
        social = interaction.data["values"][0]
        code = generate_verification_code()
        await interaction.response.send_modal(UsernameModal(social=social, code=code))


class ConnectButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Conectar rede social", style=discord.ButtonStyle.green, custom_id="btn_connect_social")

    async def callback(self, interaction: discord.Interaction):
        v = discord.ui.View(timeout=120)
        v.add_item(SocialSelect())
        await interaction.response.send_message("Escolhe a rede social:", view=v, ephemeral=True)


class ViewAccountsButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Ver minha conta", style=discord.ButtonStyle.blurple, custom_id="btn_view_account")

    async def callback(self, interaction: discord.Interaction):
        row = get_verification_request(interaction.user.id)
        if not row:
            return await interaction.response.send_message("❌ Nenhum pedido encontrado.", ephemeral=True)

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
        await interaction.response.send_message(msg, ephemeral=True)


class MainView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ConnectButton())
        self.add_item(ViewAccountsButton())


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


class IbanButtons(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Adicionar / Atualizar IBAN", style=discord.ButtonStyle.primary, custom_id="iban_add")
    async def add_iban(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return
        guild = interaction.guild or bot.get_guild(SERVER_ID)
        if not guild:
            return await _safe_ephemeral(interaction, "⚠️ Servidor não encontrado.")
        member = await fetch_member_safe(guild, interaction.user.id)
        if not member or not is_verified(member):
            return await _safe_ephemeral(interaction, "⛔ Tens de estar **Verificado** para adicionar IBAN.")
        await interaction.response.send_modal(IbanModal())

    @discord.ui.button(label="Ver meu IBAN", style=discord.ButtonStyle.secondary, custom_id="iban_view")
    async def view_iban(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return
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
        await _safe_ephemeral(interaction, f"✅ Teu IBAN: **{iban}**\n🕒 Atualizado: {updated_at}")


class ApprovalView(discord.ui.View):
    def __init__(self, target_user_id: int):
        super().__init__(timeout=None)
        self.target_user_id = int(target_user_id)

    async def _only_admin(self, interaction: discord.Interaction):
        if interaction.user.id != ADMIN_USER_ID:
            await _safe_ephemeral(interaction, "⛔ Só o admin pode aprovar/rejeitar.")
            return False
        return True

    @discord.ui.button(label="✅ Aprovar", style=discord.ButtonStyle.green, custom_id="approve_btn")
    async def approve(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return
        if not await self._only_admin(interaction):
            return

        row = get_verification_request(self.target_user_id)
        if not row:
            return await _safe_ephemeral(interaction, "⚠️ Este pedido já não existe no DB.")

        _, social, username, code, status, _, _ = row
        if status != "pending":
            return await _safe_ephemeral(interaction, f"⚠️ Pedido já está como **{status}**.")

        guild = bot.get_guild(SERVER_ID)
        if not guild:
            return await _safe_ephemeral(interaction, "⚠️ Guild não encontrada.")

        member = await fetch_member_safe(guild, self.target_user_id)
        if not member:
            return await _safe_ephemeral(interaction, "⚠️ Não consegui buscar o membro.")

        role = guild.get_role(VERIFICADO_ROLE_ID)
        if not role:
            return await _safe_ephemeral(interaction, "⚠️ Cargo 'Verificado' não encontrado.")

        try:
            await member.add_roles(role, reason="Verificação aprovada")
        except discord.Forbidden:
            return await _safe_ephemeral(
                interaction,
                "⛔ Sem permissões para dar cargo.\n"
                "1) Dá ao bot **Manage Roles**.\n"
                "2) Cargo do bot acima de **Verificado**."
            )

        set_verification_status(self.target_user_id, "verified")

        await notify_user(
            member,
            "✅ **Verificação aprovada!**\n"
            f"📱 Rede: {social}\n"
            f"🏷️ Username: {username}\n\n"
            "👉 Agora adiciona o teu IBAN:\n"
            f"• Vai ao canal <#{LIGAR_CONTA_E_VERIFICAR_CHANNEL_ID}> e usa **!ibanpanel**.",
            fallback_channel_id=LIGAR_CONTA_E_VERIFICAR_CHANNEL_ID
        )

        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(
                content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **APROVADO ✅**"),
                view=self
            )
        except:
            pass

        await _safe_ephemeral(interaction, "✅ Aprovado e cargo atribuído.")

    @discord.ui.button(label="❌ Rejeitar", style=discord.ButtonStyle.red, custom_id="reject_btn")
    async def reject(self, a, b):
        interaction, _ = _safe_button_pair(a, b)
        if not interaction:
            return
        if not await self._only_admin(interaction):
            return

        row = get_verification_request(self.target_user_id)
        if not row:
            return await _safe_ephemeral(interaction, "⚠️ Este pedido já não existe no DB.")

        _, social, username, code, status, _, _ = row
        set_verification_status(self.target_user_id, "rejected")

        guild = bot.get_guild(SERVER_ID)
        member = await fetch_member_safe(guild, self.target_user_id) if guild else None

        if member:
            await notify_user(
                member,
                "❌ **Verificação rejeitada.**\n"
                f"📱 Rede: {social}\n"
                f"🏷️ Username: {username}\n\n"
                "✅ Confere se colocaste o **código na bio** e tenta outra vez.",
                fallback_channel_id=LIGAR_CONTA_E_VERIFICAR_CHANNEL_ID
            )

        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(
                content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **REJEITADO ❌**"),
                view=self
            )
        except:
            pass

        await _safe_ephemeral(interaction, "❌ Rejeitado.")


# =========================
# COMANDOS
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
# READY: reattach views
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
                await msg.edit(view=ApprovalView(target_user_id=int(user_id)))
                continue
        except:
            pass

        try:
            view = ApprovalView(target_user_id=int(user_id))
            m = await ch.send(
                f"🆕 **Novo pedido de verificação**\n"
                f"👤 User: <@{user_id}> (`{user_id}`)\n"
                f"📱 Rede: **{social}**\n"
                f"🏷️ Username: **{username}**\n"
                f"🔑 Código: `{code}`\n"
                f"📌 Status: **PENDENTE**",
                view=view
            )
            set_verification_message(int(user_id), ch.id, m.id)
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
            await msg.edit(view=SubmitView(int(cid)))
        except:
            pass


@bot.event
async def on_ready():
    init_db()

    if not getattr(bot, "_views_added", False):
        bot.add_view(MainView())
        bot.add_view(IbanButtons())
        bot.add_view(SuporteView())
        bot.add_view(JoinCampaignView())
        bot._views_added = True

    try:
        await reattach_pending_verification_views()
        await reattach_submit_panels()
    except Exception as e:
        print("⚠️ Erro ao reanexar views:", e)

    if APIFY_TOKEN and (not track_campaign_views_loop.is_running()):
        track_campaign_views_loop.start()
        print("✅ APIFY_TOKEN OK — tracking de views ativo (loop 15 min).")
    else:
        print("⚠️ APIFY_TOKEN não definido — tracking de views NÃO vai atualizar (usa !refreshviews após definir o token).")

    print(f"✅ Bot ligado como {bot.user}!")


# =========================
# WEB (keep alive)
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
# RUN
# =========================
TOKEN = (os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN") or "").strip()
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN não encontrado. Define a variável DISCORD_TOKEN no Render/Railway.")

# DEBUG SEGURO DO TOKEN (NÃO IMPRIME O TOKEN)
print("TOKEN_LEN:", len(TOKEN))
print("TOKEN_DOTS:", TOKEN.count("."))
print("TOKEN_HAS_SPACE:", any(c.isspace() for c in TOKEN))
print("TOKEN_STARTS_WITH_QUOTES:", TOKEN[:1] in ["'", '"'])
print("TOKEN_ENDS_WITH_QUOTES:", TOKEN[-1:] in ["'", '"'])

# Se não tiver 2 pontos, não é bot token válido (ou veio cortado/errado)
if TOKEN.count(".") != 2:
    raise RuntimeError
        "Token parece mal-formatado (não tem 2 pontos). "
        "Vai ao Developer Portal -> Bot -> Reset Token -> Copy e cola no Render."

    )

keep_alive()
bot.run(TOKEN)
