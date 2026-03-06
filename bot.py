# bot.py — Viralizzaa
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
from typing import Optional, List, Dict, Any, Tuple, Set

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

# DB
DB_PATH = os.getenv("DB_PATH", "/var/data/database.sqlite3").strip()

# =========================
# APIFY
# =========================
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()
APIFY_ACTOR_TIKTOK = os.getenv("APIFY_ACTOR_TIKTOK", "clockworks~tiktok-scraper").strip()
APIFY_ACTOR_INSTAGRAM = os.getenv("APIFY_ACTOR_INSTAGRAM", "apify~instagram-scraper").strip()
VIEWS_REFRESH_MINUTES = int((os.getenv("VIEWS_REFRESH_MINUTES", "10").strip() or "10"))

APIFY_USE_PROXY = (os.getenv("APIFY_USE_PROXY", "false").strip().lower() in ("1", "true", "yes", "y", "on"))
APIFY_PROXY_COUNTRY = os.getenv("APIFY_PROXY_COUNTRY", "").strip().upper()
APIFY_PROXY_GROUPS_RAW = os.getenv("APIFY_PROXY_GROUPS", "").strip()
APIFY_PROXY_GROUPS = [g.strip().upper() for g in APIFY_PROXY_GROUPS_RAW.split(",") if g.strip()]

CAMPAIGN_SUBMISSION_LOCK_PCT = float((os.getenv("CAMPAIGN_SUBMISSION_LOCK_PCT", "0.95").strip() or "0.95"))

MAX_APPROVED_PER_USER = int(os.getenv("MAX_APPROVED_PER_USER", "10").strip() or "10")
PAYMENTS_NOTICE = "✅ A campanha terminou. **Aguarda o pagamento** — será enviado em **3–7 dias úteis**."

print("DISCORD VERSION:", getattr(discord, "__version__", "unknown"))
print("DB_PATH:", DB_PATH)
print("APIFY_TOKEN set:", bool(APIFY_TOKEN))
print("APIFY_ACTOR_TIKTOK:", APIFY_ACTOR_TIKTOK)
print("APIFY_ACTOR_INSTAGRAM:", APIFY_ACTOR_INSTAGRAM)
print("VIEWS_REFRESH_MINUTES:", VIEWS_REFRESH_MINUTES)
print("APIFY_USE_PROXY:", APIFY_USE_PROXY)
print("APIFY_PROXY_COUNTRY:", APIFY_PROXY_COUNTRY)
print("APIFY_PROXY_GROUPS:", APIFY_PROXY_GROUPS)
print("CAMPAIGN_SUBMISSION_LOCK_PCT:", CAMPAIGN_SUBMISSION_LOCK_PCT)
print("MAX_APPROVED_PER_USER:", MAX_APPROVED_PER_USER)

# =========================
# BOT / INTENTS
# =========================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# COMMAND ERROR HANDLER
# =========================
@bot.event
async def on_command_error(ctx: commands.Context, error: Exception):
    if isinstance(error, commands.CheckFailure):
        return await ctx.send("⛔ Sem permissão para usar este comando (staff-only).")

    if isinstance(error, commands.CommandNotFound):
        return

    try:
        await ctx.send(f"⚠️ Erro no comando: `{type(error).__name__}`")
    except:
        pass
    print("⚠️ on_command_error:", repr(error))
    traceback.print_exc()

@bot.command()
async def whoami(ctx):
    if not ctx.guild or not isinstance(ctx.author, discord.Member):
        return await ctx.send(f"User ID: {ctx.author.id}")
    p = ctx.author.guild_permissions
    await ctx.send(
        f"👤 {ctx.author} | id={ctx.author.id}\n"
        f"admin={p.administrator} | is_staff_member={is_staff_member(ctx.author)}\n"
        f"ADMIN_USER_ID no código = {ADMIN_USER_ID}"
    )

# =========================
# HTTP SESSION
# =========================
HTTP_SESSION: Optional[aiohttp.ClientSession] = None

async def get_http_session() -> aiohttp.ClientSession:
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        timeout = aiohttp.ClientTimeout(total=90)
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
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def generate_verification_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "VZ-" + "".join(secrets.choice(alphabet) for _ in range(7))

def is_verified(member: discord.Member) -> bool:
    role = member.guild.get_role(VERIFICADO_ROLE_ID)
    return bool(role) and (role in member.roles)

def is_staff_member(member: discord.Member) -> bool:
    if member.id == ADMIN_USER_ID:
        return True
    perms = getattr(member, "guild_permissions", None)
    return bool(perms and perms.administrator)

def is_staff_ctx(ctx: commands.Context) -> bool:
    if not ctx.guild or not isinstance(ctx.author, discord.Member):
        return False
    return is_staff_member(ctx.author)

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

async def notify_user(
    member: discord.Member,
    content: str,
    fallback_channel_id: Optional[int] = None,
    view: Optional[discord.ui.View] = None
):
    try:
        await member.send(content, view=view)
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

async def safe_send_modal(interaction: discord.Interaction, modal: discord.ui.Modal, fallback_text: str = "⚠️ Tenta novamente."):
    try:
        if interaction.response.is_done():
            await interaction.followup.send(fallback_text, ephemeral=True)
            return
        await interaction.response.send_modal(modal)
    except Exception as e:
        print("⚠️ safe_send_modal falhou:", e)
        try:
            await safe_reply(interaction, fallback_text, ephemeral=True)
        except:
            pass

def detect_platform(url: str) -> str:
    u = (url or "").lower()
    if "tiktok.com" in u:
        return "tiktok"
    if "instagram.com" in u:
        return "instagram"
    if "youtube.com" in u or "youtu.be" in u:
        return "youtube"
    return "unknown"

def social_pretty_name(social: str) -> str:
    s = (social or "").lower()
    if s == "tiktok":
        return "TikTok"
    if s == "instagram":
        return "Instagram"
    if s == "youtube":
        return "YouTube"
    return social

def parse_campaign_platforms(platforms: str) -> List[str]:
    p = (platforms or "").lower()
    allowed = []
    if "tiktok" in p:
        allowed.append("tiktok")
    if "instagram" in p:
        allowed.append("instagram")
    if "youtube" in p:
        allowed.append("youtube")
    return allowed

def pct(a: int, b: int) -> float:
    if b <= 0:
        return 0.0
    return float(a) / float(b)

def parse_human_number(v: str) -> Optional[int]:
    if not v:
        return None
    s = str(v).strip().upper().replace(",", "")
    m = re.match(r"^(\d+(\.\d+)?)([KM])?$", s)
    if not m:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits.isdigit() else None
    num = float(m.group(1))
    suf = m.group(3)
    if suf == "K":
        num *= 1000
    elif suf == "M":
        num *= 1_000_000
    return int(num)

def normalize_tiktok_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    if "tiktok.com" not in u.lower():
        return u
    u = u.split("?")[0]
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]
    if not u.startswith("https://"):
        u = "https://" + u.lstrip("/")
    return u

def normalize_apify_actor_id(actor: str) -> str:
    a = (actor or "").strip()
    if not a:
        return a
    if "~" in a:
        return a
    if "/" in a:
        return a.replace("/", "~", 1)
    return a

def build_proxy_configuration() -> Optional[dict]:
    if not APIFY_USE_PROXY:
        return None
    cfg: Dict[str, Any] = {"useApifyProxy": True}
    if APIFY_PROXY_GROUPS:
        cfg["apifyProxyGroups"] = APIFY_PROXY_GROUPS
    if APIFY_PROXY_COUNTRY:
        cfg["apifyProxyCountry"] = APIFY_PROXY_COUNTRY
    return cfg

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
    CREATE TABLE IF NOT EXISTS linked_accounts (
        user_id INTEGER NOT NULL,
        social TEXT NOT NULL,
        username TEXT NOT NULL,
        linked_at INTEGER NOT NULL,
        PRIMARY KEY (user_id, social)
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

    if not _column_exists(conn, "campaigns", "campaign_role_id"):
        try:
            cur.execute("ALTER TABLE campaigns ADD COLUMN campaign_role_id INTEGER")
        except Exception as e:
            print("⚠️ MIGRATION campaigns.campaign_role_id:", e)

    if not _column_exists(conn, "campaigns", "ended_notified"):
        try:
            cur.execute("ALTER TABLE campaigns ADD COLUMN ended_notified INTEGER NOT NULL DEFAULT 0")
        except Exception as e:
            print("⚠️ MIGRATION campaigns.ended_notified:", e)

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
        maxed_notified INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (campaign_id, user_id)
    )
    """)

    if not _column_exists(conn, "campaign_users", "maxed_notified"):
        try:
            cur.execute("ALTER TABLE campaign_users ADD COLUMN maxed_notified INTEGER NOT NULL DEFAULT 0")
        except Exception as e:
            print("⚠️ MIGRATION campaign_users.maxed_notified:", e)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS campaign_members (
        campaign_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        joined_at INTEGER NOT NULL,
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
    cur.execute("DELETE FROM ibans WHERE user_id=?", (int(user_id),))
    conn.commit()
    conn.close()

# ===== LINKED ACCOUNTS =====
def get_linked_account(user_id: int, social: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT username, linked_at
        FROM linked_accounts
        WHERE user_id=? AND social=?
    """, (int(user_id), str(social).lower()))
    row = cur.fetchone()
    conn.close()
    return row

def list_linked_accounts(user_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT social, username, linked_at
        FROM linked_accounts
        WHERE user_id=?
        ORDER BY social ASC
    """, (int(user_id),))
    rows = cur.fetchall()
    conn.close()
    return rows

def add_linked_account(user_id: int, social: str, username: str) -> bool:
    """
    Adiciona só se NÃO existir conta dessa rede.
    Retorna True se adicionou, False se já existia.
    """
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO linked_accounts (user_id, social, username, linked_at)
            VALUES (?, ?, ?, ?)
        """, (int(user_id), str(social).lower(), str(username).strip(), _now()))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False

def delete_linked_account(user_id: int, social: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM linked_accounts
        WHERE user_id=? AND social=?
    """, (int(user_id), str(social).lower()))
    conn.commit()
    conn.close()

# ===== VERIFICAÇÃO =====
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
           campaign_role_id,
           ended_notified
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

def add_campaign_member(campaign_id: int, user_id: int) -> bool:
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO campaign_members (campaign_id, user_id, joined_at) VALUES (?,?,?)",
                    (campaign_id, user_id, _now()))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False

def is_campaign_member(campaign_id: int, user_id: int) -> bool:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM campaign_members WHERE campaign_id=? AND user_id=?", (campaign_id, user_id))
    row = cur.fetchone()
    conn.close()
    return bool(row)

def set_maxed_notified(campaign_id: int, user_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE campaign_users SET maxed_notified=1 WHERE campaign_id=? AND user_id=?",
                (campaign_id, user_id))
    conn.commit()
    conn.close()

def get_user_paid_in_campaign(campaign_id: int, user_id: int) -> Tuple[int, int]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(paid_kz,0), COALESCE(maxed_notified,0) FROM campaign_users WHERE campaign_id=? AND user_id=?",
                (int(campaign_id), int(user_id)))
    row = cur.fetchone() or (0, 0)
    conn.close()
    return int(row[0] or 0), int(row[1] or 0)

def reset_user_in_campaign(campaign_id: int, user_id: int, refund_budget: bool = True):
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("SELECT COALESCE(paid_kz,0) FROM campaign_users WHERE campaign_id=? AND user_id=?",
                (int(campaign_id), int(user_id)))
    user_paid_kz = int((cur.fetchone() or [0])[0] or 0)

    cur.execute("DELETE FROM submissions WHERE campaign_id=? AND user_id=?", (int(campaign_id), int(user_id)))
    cur.execute("DELETE FROM campaign_users WHERE campaign_id=? AND user_id=?", (int(campaign_id), int(user_id)))
    cur.execute("DELETE FROM campaign_members WHERE campaign_id=? AND user_id=?", (int(campaign_id), int(user_id)))

    if refund_budget and user_paid_kz > 0:
        cur.execute("SELECT spent_kz, budget_total_kz, status FROM campaigns WHERE id=?", (int(campaign_id),))
        row = cur.fetchone() or (0, 0, "active")
        spent_kz = int(row[0] or 0)
        budget_total = int(row[1] or 0)
        status = str(row[2] or "active")

        new_spent = max(0, spent_kz - user_paid_kz)
        cur.execute("UPDATE campaigns SET spent_kz=? WHERE id=?", (int(new_spent), int(campaign_id)))

        if status == "ended" and new_spent < budget_total:
            cur.execute("UPDATE campaigns SET status='active', ended_notified=0 WHERE id=?", (int(campaign_id),))

    conn.commit()
    conn.close()

def get_user_submission_counts(campaign_id: int, user_id: int) -> Tuple[int, int, int]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            SUM(CASE WHEN status='approved' THEN 1 ELSE 0 END) as approved,
            SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END)  as pending,
            COUNT(*) as total
        FROM submissions
        WHERE campaign_id=? AND user_id=? AND status IN ('pending','approved')
    """, (int(campaign_id), int(user_id)))
    row = cur.fetchone() or (0, 0, 0)
    conn.close()
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)

async def notify_campaign_finished(campaign_id: int, winner_user_id: Optional[int], reason: str):
    guild = bot.get_guild(SERVER_ID)
    if not guild:
        return

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT name FROM campaigns WHERE id=?", (int(campaign_id),))
    camp = cur.fetchone()
    camp_name = str(camp[0]) if camp else f"Campanha {campaign_id}"

    cur.execute("SELECT user_id FROM campaign_members WHERE campaign_id=?", (int(campaign_id),))
    members = [int(r[0]) for r in cur.fetchall()]
    conn.close()

    if reason == "budget" and winner_user_id:
        m = await fetch_member_safe(guild, int(winner_user_id))
        if m:
            await notify_user(
                m,
                f"🏁 **Parabéns!** O teu progresso fez a campanha **{camp_name}** atingir o budget.\n\n{PAYMENTS_NOTICE}",
                fallback_channel_id=CHAT_CHANNEL_ID
            )

    for uid in members:
        m = await fetch_member_safe(guild, int(uid))
        if m:
            await notify_user(
                m,
                f"🏁 A campanha **{camp_name}** terminou ({'budget atingido' if reason=='budget' else 'finalizada pelo staff'}).\n\n{PAYMENTS_NOTICE}",
                fallback_channel_id=CHAT_CHANNEL_ID
            )

def mark_campaign_ended_notified(campaign_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE campaigns SET ended_notified=1 WHERE id=?", (int(campaign_id),))
    conn.commit()
    conn.close()

def get_campaign_basic(campaign_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, slug, platforms, content_types, audio_url,
               rate_kz_per_1k, budget_total_kz, spent_kz,
               max_payout_user_kz, max_posts_total, status,
               category_id, submit_channel_id, leaderboard_channel_id, leaderboard_message_id,
               campaign_role_id, ended_notified
        FROM campaigns WHERE id=?
    """, (int(campaign_id),))
    row = cur.fetchone()
    conn.close()
    return row

async def remove_campaign_role_from_member(guild: discord.Guild, campaign_id: int, member: discord.Member):
    row = get_campaign_basic(int(campaign_id))
    if not row:
        return
    role_id = row[16]
    if not role_id:
        return
    role = guild.get_role(int(role_id))
    if role and role in member.roles:
        try:
            await member.remove_roles(role, reason="Removed from campaign")
        except:
            pass

async def purge_ghosts_for_campaign(guild: discord.Guild, campaign_id: int, refund_budget: bool = True) -> Tuple[int, int]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM campaign_members WHERE campaign_id=?", (int(campaign_id),))
    member_ids = [int(r[0]) for r in cur.fetchall()]
    conn.close()

    ghost_removed = 0
    for uid in member_ids:
        m = await fetch_member_safe(guild, uid)
        if m is None:
            reset_user_in_campaign(int(campaign_id), int(uid), refund_budget=refund_budget)
            ghost_removed += 1

    conn2 = db_conn()
    c2 = conn2.cursor()

    c2.execute("""
        DELETE FROM submissions
        WHERE campaign_id=?
          AND user_id NOT IN (SELECT user_id FROM campaign_members WHERE campaign_id=?)
    """, (int(campaign_id), int(campaign_id)))
    orph_sub = c2.rowcount if c2.rowcount is not None else 0

    c2.execute("""
        DELETE FROM campaign_users
        WHERE campaign_id=?
          AND user_id NOT IN (SELECT user_id FROM campaign_members WHERE campaign_id=?)
    """, (int(campaign_id), int(campaign_id)))
    orph_users = c2.rowcount if c2.rowcount is not None else 0

    conn2.commit()
    conn2.close()

    return ghost_removed, int(orph_sub) + int(orph_users)

def reset_campaign_all(campaign_id: int, reset_spent: bool = True):
    conn = db_conn()
    cur = conn.cursor()

    if reset_spent:
        cur.execute("""
            UPDATE campaigns
            SET spent_kz=0,
                status='active',
                ended_notified=0
            WHERE id=?
        """, (int(campaign_id),))

    cur.execute("DELETE FROM submissions WHERE campaign_id=?", (int(campaign_id),))
    cur.execute("DELETE FROM campaign_users WHERE campaign_id=?", (int(campaign_id),))
    cur.execute("DELETE FROM campaign_members WHERE campaign_id=?", (int(campaign_id),))

    conn.commit()
    conn.close()

def find_campaign_id_for_channel(channel: discord.abc.GuildChannel) -> Optional[int]:
    try:
        ch_id = int(channel.id)
    except:
        return None
    cat_id = None
    try:
        if hasattr(channel, "category_id") and channel.category_id:
            cat_id = int(channel.category_id)
    except:
        cat_id = None

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id FROM campaigns
        WHERE submit_channel_id=?
           OR leaderboard_channel_id=?
           OR details_channel_id=?
           OR requirements_channel_id=?
           OR category_id=?
        LIMIT 1
    """, (ch_id, ch_id, ch_id, ch_id, cat_id if cat_id else -1))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None

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
# UI VIEWS
# =========================
class MainView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="Conectar rede social", style=discord.ButtonStyle.green, custom_id="vz:connect"))
        self.add_item(discord.ui.Button(label="Ver minha conta", style=discord.ButtonStyle.blurple, custom_id="vz:view_account"))

class IbanButtons(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="🏦 Adicionar/Alterar IBAN (Angola)", style=discord.ButtonStyle.primary, custom_id="vz:iban:add"))
        self.add_item(discord.ui.Button(label="👁️ Ver meu IBAN", style=discord.ButtonStyle.secondary, custom_id="vz:iban:view"))
        self.add_item(discord.ui.Button(label="🗑️ Apagar meu IBAN", style=discord.ButtonStyle.danger, custom_id="vz:iban:delete"))

class LinkedAccountsManageView(discord.ui.View):
    """
    Mostra só botões das contas que realmente existem.
    """
    def __init__(self, linked_rows: List[Tuple[str, str, int]]):
        super().__init__(timeout=300)
        socials_present = {str(s).lower(): str(u) for s, u, _ts in linked_rows}

        if "tiktok" in socials_present:
            self.add_item(discord.ui.Button(
                label=f"🗑️ Remover TikTok ({socials_present['tiktok']})",
                style=discord.ButtonStyle.danger,
                custom_id="vz:unlink:tiktok"
            ))
        if "instagram" in socials_present:
            self.add_item(discord.ui.Button(
                label=f"🗑️ Remover Instagram ({socials_present['instagram']})",
                style=discord.ButtonStyle.danger,
                custom_id="vz:unlink:instagram"
            ))
        if "youtube" in socials_present:
            self.add_item(discord.ui.Button(
                label=f"🗑️ Remover YouTube ({socials_present['youtube']})",
                style=discord.ButtonStyle.danger,
                custom_id="vz:unlink:youtube"
            ))

class JoinCampaignView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="🔥 Aderir à Campanha", style=discord.ButtonStyle.success, custom_id="vz:camp:join"))

def submit_view(campaign_id: int) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(discord.ui.Button(label="📥 Submeter link", style=discord.ButtonStyle.primary, custom_id=f"vz:submit:open:{campaign_id}"))
    v.add_item(discord.ui.Button(label="📊 Estatísticas", style=discord.ButtonStyle.secondary, custom_id=f"vz:submit:stats:{campaign_id}"))
    v.add_item(discord.ui.Button(label="🗑️ Retirar vídeo", style=discord.ButtonStyle.danger, custom_id=f"vz:submit:remove:{campaign_id}"))
    v.add_item(discord.ui.Button(label="🚪 Sair da campanha (reset)", style=discord.ButtonStyle.secondary, custom_id=f"vz:camp:leave:{campaign_id}"))
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

class ChooseSocialView(discord.ui.View):
    def __init__(self, code: str):
        super().__init__(timeout=120)
        self.add_item(discord.ui.Button(label="TikTok", style=discord.ButtonStyle.primary, custom_id=f"vz:connect:tiktok:{code}"))
        self.add_item(discord.ui.Button(label="Instagram", style=discord.ButtonStyle.primary, custom_id=f"vz:connect:instagram:{code}"))
        self.add_item(discord.ui.Button(label="YouTube", style=discord.ButtonStyle.primary, custom_id=f"vz:connect:youtube:{code}"))

class SupportView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="📌 Problema com campanha", style=discord.ButtonStyle.danger, custom_id="vz:support:campaign"))
        self.add_item(discord.ui.Button(label="💬 Dúvidas", style=discord.ButtonStyle.primary, custom_id="vz:support:question"))

# =========================
# SUPORTE
# =========================
class CloseTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="✅ Fechar ticket", style=discord.ButtonStyle.danger, custom_id="vz:ticket:close"))

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
        await safe_reply(interaction, "✅ Recebido. Vou abrir um ticket privado contigo e o staff.", ephemeral=True)

        guild = interaction.guild or bot.get_guild(SERVER_ID)
        if not guild:
            return

        support_ch = guild.get_channel(SUPORTE_CHANNEL_ID)
        staff_ch = guild.get_channel(SUPORTE_STAFF_CHANNEL_ID)
        admin_member = guild.get_member(ADMIN_USER_ID) or await fetch_member_safe(guild, ADMIN_USER_ID)

        if staff_ch:
            try:
                await staff_ch.send(
                    "🆘 **Novo ticket (Problema com campanha)**\n"
                    f"👤 {interaction.user.mention} (`{interaction.user.id}`)\n"
                    f"🎯 Campanha: **{self.campaign_name.value}**\n"
                    f"📝 Resumo:\n{self.problem.value[:700]}"
                )
            except:
                pass

        if not support_ch:
            return

        try:
            thread = await support_ch.create_thread(
                name=f"🆘 Campanha • {interaction.user.name}",
                type=discord.ChannelType.private_thread,
                auto_archive_duration=1440
            )
            await thread.add_user(interaction.user)
            if admin_member:
                await thread.add_user(admin_member)

            await thread.send(
                "🆘 **Ticket — Problema com Campanha**\n"
                f"👤 {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"🎯 Campanha: **{self.campaign_name.value}**\n"
                f"📝 Problema:\n{self.problem.value}\n\n"
                "✅ O staff vai responder aqui.",
                view=CloseTicketView()
            )
        except Exception as e:
            print("⚠️ erro a criar thread privada:", e)

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
        if not guild:
            return

        staff_ch = guild.get_channel(SUPORTE_STAFF_CHANNEL_ID)
        if staff_ch:
            try:
                await staff_ch.send(
                    "❓ **Nova dúvida**\n"
                    f"👤 {interaction.user.mention} (`{interaction.user.id}`)\n"
                    f"📝 Mensagem:\n{self.question.value}"
                )
            except:
                pass

        support_ch = guild.get_channel(SUPORTE_CHANNEL_ID)
        admin_member = guild.get_member(ADMIN_USER_ID) or await fetch_member_safe(guild, ADMIN_USER_ID)
        if not support_ch:
            return

        try:
            thread = await support_ch.create_thread(
                name=f"❓ Dúvida • {interaction.user.name}",
                type=discord.ChannelType.private_thread,
                auto_archive_duration=1440
            )
            await thread.add_user(interaction.user)
            if admin_member:
                await thread.add_user(admin_member)

            await thread.send(
                "❓ **Ticket — Dúvida**\n"
                f"👤 {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"📝 Dúvida:\n{self.question.value}\n\n"
                "✅ O staff vai responder aqui.",
                view=CloseTicketView()
            )
        except Exception as e:
            print("⚠️ erro a criar thread (dúvidas):", e)

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
        social = str(self.social).lower()
        username = str(self.username.value).strip()

        existing = get_linked_account(user_id, social)
        if existing:
            return await safe_reply(
                interaction,
                f"⚠️ Já associou uma conta **{social_pretty_name(social)}**: **{existing[0]}**.\n"
                "Se quiser trocar, elimine a outra e só depois adicione.",
                ephemeral=True
            )

        upsert_verification_request(user_id=user_id, social=social, username=username, code=self.code, status="pending")

        await safe_reply(
            interaction,
            "✅ Pedido enviado!\n\n"
            f"📱 Rede: {social_pretty_name(social)}\n"
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
            f"📱 Rede: **{social_pretty_name(social)}**\n"
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
        set_iban(interaction.user.id, str(self.iban.value).strip())
        await safe_reply(interaction, "✅ IBAN guardado com sucesso.", ephemeral=True)

class RejectSubmissionReasonModal(discord.ui.Modal):
    def __init__(self, submission_id: int, campaign_id: int, user_id: int, post_url: str, camp_name: str):
        super().__init__(title="Motivo da rejeição")
        self.submission_id = int(submission_id)
        self.campaign_id = int(campaign_id)
        self.user_id = int(user_id)
        self.post_url = str(post_url)
        self.camp_name = str(camp_name)

        self.reason = discord.ui.TextInput(
            label="Porque estás a rejeitar este vídeo?",
            placeholder="Ex: vídeo fora do tema, link errado, qualidade fraca, não segue os requisitos...",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=800
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild or bot.get_guild(SERVER_ID)
        if not guild:
            return await safe_reply(interaction, "⚠️ Servidor não encontrado.", ephemeral=True)

        staff = await fetch_member_safe(guild, interaction.user.id)
        if not staff or not is_staff_member(staff):
            return await safe_reply(interaction, "⛔ Sem permissão.", ephemeral=True)

        conn = db_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, campaign_id, user_id, post_url, status, platform
            FROM submissions
            WHERE id=?
        """, (int(self.submission_id),))
        row = cur.fetchone()

        if not row:
            conn.close()
            return await safe_reply(interaction, "❌ Submission não encontrada.", ephemeral=True)

        sid, camp_id, user_id, post_url, status, platform = row
        sid = int(sid)
        camp_id = int(camp_id)
        user_id = int(user_id)
        post_url = str(post_url)
        platform = str(platform)

        if status == "rejected":
            conn.close()
            return await safe_reply(interaction, "⚠️ Esta submission já foi rejeitada.", ephemeral=True)

        cur.execute("UPDATE submissions SET status='rejected' WHERE id=?", (sid,))
        conn.commit()
        conn.close()

        target_member = await fetch_member_safe(guild, user_id)
        reason_txt = str(self.reason.value).strip()
        linked = get_linked_account(user_id, platform)
        linked_txt = linked[0] if linked else "Não encontrada"

        if target_member:
            await notify_user(
                target_member,
                "❌ O teu vídeo foi **rejeitado**.\n"
                f"🎯 Campanha: **{self.camp_name}**\n"
                f"📱 Conta {social_pretty_name(platform)}: **{linked_txt}**\n"
                f"📝 Motivo: {reason_txt}\n"
                f"🔗 {post_url}",
                fallback_channel_id=CHAT_CHANNEL_ID
            )

        try:
            if interaction.message:
                await interaction.message.edit(view=None)
        except:
            pass

        await update_leaderboard_for_campaign(camp_id)
        await safe_reply(interaction, "✅ Vídeo rejeitado e motivo enviado ao utilizador.", ephemeral=True)

class SubmitLinkModal(discord.ui.Modal):
    def __init__(self, campaign_id: int):
        super().__init__(title="Submeter link (TikTok/Instagram)")
        self.campaign_id = int(campaign_id)
        self.url = discord.ui.TextInput(
            label="Link do teu post (TikTok/Instagram)",
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

        if not is_campaign_member(self.campaign_id, interaction.user.id):
            return await safe_reply(interaction, "⛔ Primeiro tens de **aderir** à campanha no post (botão 🔥).", ephemeral=True)

        conn = db_conn()
        row = get_campaign_by_id(conn, self.campaign_id)
        if not row:
            conn.close()
            return await safe_reply(interaction, "❌ Campanha não encontrada.", ephemeral=True)

        camp_id         = int(row[0])
        name            = str(row[1])
        platforms       = str(row[3])
        budget_total    = int(row[7])
        spent_kz        = int(row[8])
        max_user_kz     = int(row[9])
        max_posts_total = int(row[10])
        status          = str(row[11])
        conn.close()

        paid_kz, maxed_notified = get_user_paid_in_campaign(int(camp_id), int(interaction.user.id))
        if paid_kz >= max_user_kz:
            if maxed_notified == 0:
                set_maxed_notified(int(camp_id), int(interaction.user.id))
            return await safe_reply(
                interaction,
                f"⛔ Já atingiste o teu limite nesta campanha (**{max_user_kz:,} Kz**). Não podes submeter mais vídeos.",
                ephemeral=True
            )

        if status != "active":
            return await safe_reply(interaction, "⚠️ Esta campanha já terminou.", ephemeral=True)

        if pct(int(spent_kz), int(budget_total)) >= CAMPAIGN_SUBMISSION_LOCK_PCT:
            return await safe_reply(interaction, "⚠️ Campanha está **quase cheia (95%)**. Submissões fechadas.", ephemeral=True)

        url = str(self.url.value).strip()
        if not url.startswith("http://") and not url.startswith("https://"):
            return await safe_reply(interaction, "❌ Link inválido. Envia um link completo com **https://**", ephemeral=True)

        platform = detect_platform(url)
        if platform == "tiktok":
            url = normalize_tiktok_url(url)

        allowed = parse_campaign_platforms(platforms)
        if platform not in allowed:
            return await safe_reply(interaction, f"❌ Esta campanha só aceita: **{', '.join([p.upper() for p in allowed])}**.", ephemeral=True)

        linked = get_linked_account(int(interaction.user.id), platform)
        if not linked:
            return await safe_reply(
                interaction,
                f"⛔ Antes de submeter um vídeo de **{social_pretty_name(platform)}**, tens de ligar uma conta dessa rede.",
                ephemeral=True
            )

        linked_username = str(linked[0])

        conn2 = db_conn()
        cur2 = conn2.cursor()

        cur2.execute("""
        SELECT COUNT(*)
        FROM submissions
        WHERE campaign_id=? AND status IN ('pending','approved')
        """, (int(camp_id),))
        total_active_posts = int((cur2.fetchone() or [0])[0])
        if total_active_posts >= int(max_posts_total):
            conn2.close()
            return await safe_reply(interaction, f"⚠️ Esta campanha já atingiu o máximo de posts (**{max_posts_total}**).", ephemeral=True)

        now = _now()
        try:
            cur2.execute("""
            INSERT INTO submissions (campaign_id, user_id, post_url, platform, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
            """, (int(camp_id), interaction.user.id, url, platform, now))
            submission_id = int(cur2.lastrowid)
            conn2.commit()
        except sqlite3.IntegrityError:
            conn2.close()
            return await safe_reply(interaction, "⚠️ Este link já foi submetido nesta campanha.", ephemeral=True)

        conn2.close()

        appr = guild.get_channel(VERIFICACOES_CHANNEL_ID)
        if appr:
            await appr.send(
                f"📥 **Novo link submetido**\n"
                f"🆔 Submission: `{submission_id}`\n"
                f"🎯 Campanha: **{name}** (`{camp_id}`)\n"
                f"👤 User: {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"📱 Conta associada ({social_pretty_name(platform)}): **{linked_username}**\n"
                f"🌐 Plataforma: **{platform.upper()}**\n"
                f"🔗 {url}\n"
                f"📌 Status: **PENDENTE**",
                view=submission_approval_view(submission_id)
            )

        approved, _, _ = get_user_submission_counts(int(camp_id), int(interaction.user.id))
        extra = ""
        if approved >= MAX_APPROVED_PER_USER:
            extra = f"\n\n⚠️ Nota: já tens **{MAX_APPROVED_PER_USER} aprovados**. Este link pode não ser aprovado."

        await safe_reply(interaction, "✅ Link submetido! Aguarda aprovação do staff." + extra, ephemeral=True)

class RemoveLinkModal(discord.ui.Modal):
    def __init__(self, campaign_id: int):
        super().__init__(title="Retirar vídeo da campanha")
        self.campaign_id = int(campaign_id)
        self.url = discord.ui.TextInput(
            label="Link do vídeo que queres retirar",
            placeholder="Cola aqui o mesmo link que submeteste",
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

        if not is_campaign_member(self.campaign_id, interaction.user.id):
            return await safe_reply(interaction, "⛔ Primeiro tens de **aderir** à campanha.", ephemeral=True)

        url = str(self.url.value).strip()
        if "tiktok.com" in url.lower():
            url = normalize_tiktok_url(url)

        conn = db_conn()
        cur = conn.cursor()
        cur.execute("""
        SELECT id, status
        FROM submissions
        WHERE campaign_id=? AND user_id=? AND post_url=?
        """, (self.campaign_id, interaction.user.id, url))
        row = cur.fetchone()
        if not row:
            conn.close()
            return await safe_reply(interaction, "❌ Não encontrei esse link nas tuas submissões desta campanha.", ephemeral=True)

        sub_id, st = row
        if st == "removed":
            conn.close()
            return await safe_reply(interaction, "⚠️ Esse vídeo já foi retirado.", ephemeral=True)

        cur.execute("UPDATE submissions SET status='removed' WHERE id=?", (int(sub_id),))
        conn.commit()
        conn.close()

        await safe_reply(interaction, "✅ Vídeo retirado. Ele deixa de ser contado/atualizado.", ephemeral=True)

# =========================
# CAMPANHA WORKSPACE
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
    lb_ch = await guild.create_text_channel("4-tabela-de-classificacao", category=category, overwrites=overw_ro)

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
    lb_msg = await lb_ch.send("🏆 **Tabela de classificação**\n(aguarda atualizações automáticas)")

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

def staff_only():
    async def predicate(ctx: commands.Context):
        return is_staff_ctx(ctx)
    return commands.check(predicate)

@staff_only()
@bot.command()
async def campanha(ctx):
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
     campaigns_channel_id, created_at, ended_notified)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,0)
    """, (
        c["name"], c["slug"], c["platforms"], c["content_types"], c["audio_url"],
        int(c["rate_kz_per_1k"]), int(c["budget_total_kz"]),
        int(c["max_payout_user_kz"]), int(c["max_posts_total"]),
        int(CAMPANHAS_CHANNEL_ID), int(now)
    ))
    conn.commit()

    cur.execute("SELECT id, post_message_id FROM campaigns WHERE slug=?", (c["slug"],))
    row = cur.fetchone()
    conn.close()

    if not row:
        return await ctx.send("❌ Falha ao criar/encontrar campanha na DB.")

    camp_id = int(row[0])
    post_msg_id = row[1]

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

    await ctx.send(f"✅ Campanha publicada em #campanhas. ID da campanha: **{camp_id}**")

@staff_only()
@bot.command()
async def relancar(ctx):
    await campanha(ctx)

@staff_only()
@bot.command()
async def listcampaigns(ctx):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, status, spent_kz, budget_total_kz FROM campaigns ORDER BY id DESC LIMIT 25")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return await ctx.send("Não há campanhas na DB.")

    lines = ["📌 **Campanhas (ID | Nome | Status | Gasto/Budget)**"]
    for cid, name, status, spent, budget in rows:
        lines.append(f"**{cid}** | {name} | {status} | {int(spent):,}/{int(budget):,} Kz")
    await ctx.send("\n".join(lines))

@staff_only()
@bot.command()
async def campaignid(ctx):
    if not ctx.guild:
        return
    cid = find_campaign_id_for_channel(ctx.channel)
    if not cid:
        return await ctx.send("⚠️ Não consegui identificar a campanha por este canal. Usa `!listcampaigns`.")
    await ctx.send(f"✅ O ID desta campanha é: **{cid}**")

@staff_only()
@bot.command()
async def refreshnow(ctx):
    await ctx.send("⏳ A correr refresh agora…")
    await refresh_views_once()
    await ctx.send("✅ Refresh concluído (vê se o leaderboard/stats mudou).")

@staff_only()
@bot.command()
async def endcampaign(ctx, campaign_id: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT status, ended_notified FROM campaigns WHERE id=?", (int(campaign_id),))
    row = cur.fetchone()
    if not row:
        conn.close()
        return await ctx.send("❌ Campanha não encontrada.")
    ended_notified = int(row[1] or 0)

    cur.execute("UPDATE campaigns SET status='ended' WHERE id=?", (int(campaign_id),))
    conn.commit()
    conn.close()

    if ended_notified == 0:
        mark_campaign_ended_notified(int(campaign_id))
        await notify_campaign_finished(int(campaign_id), winner_user_id=None, reason="manual")

    await update_leaderboard_for_campaign(int(campaign_id))
    await ctx.send(f"✅ Campanha {campaign_id} terminada manualmente e notificações enviadas.")

@staff_only()
@bot.command()
async def closecampaign(ctx, campaign_id: int):
    await endcampaign(ctx, campaign_id)

@staff_only()
@bot.command()
async def purgeghosts(ctx, campaign_id: int):
    guild = ctx.guild or bot.get_guild(SERVER_ID)
    if not guild:
        return await ctx.send("⚠️ Guild não encontrada.")
    ghost_removed, orphans = await purge_ghosts_for_campaign(guild, int(campaign_id), refund_budget=True)
    await update_leaderboard_for_campaign(int(campaign_id))
    await ctx.send(f"🧹 purgeghosts concluído na campanha {campaign_id}: ghosts removidos={ghost_removed} | órfãos limpos={orphans}")

@staff_only()
@bot.command()
async def restartcampaign(ctx, campaign_id: int):
    guild = ctx.guild or bot.get_guild(SERVER_ID)
    if not guild:
        return await ctx.send("⚠️ Guild não encontrada.")

    row = get_campaign_basic(int(campaign_id))
    if not row:
        return await ctx.send("❌ Campanha não encontrada.")

    role_id = row[16]
    role = guild.get_role(int(role_id)) if role_id else None

    await ctx.send(f"⚠️ A reiniciar campanha {campaign_id}… (vai apagar tudo)")

    removed_roles = 0
    if role:
        try:
            for member in list(role.members):
                try:
                    await member.remove_roles(role, reason="restartcampaign (reset total)")
                    removed_roles += 1
                except:
                    pass
        except:
            pass

    reset_campaign_all(int(campaign_id), reset_spent=True)
    await update_leaderboard_for_campaign(int(campaign_id))

    await ctx.send(
        f"✅ Campanha {campaign_id} reiniciada.\n"
        f"• Roles removidos: {removed_roles}\n"
        f"• DB limpo: submissions/campaign_users/campaign_members apagados\n"
        f"• spent_kz=0 | status=active | ended_notified=0"
    )

@staff_only()
@bot.command()
async def pureghosts(ctx, user_id: int):
    guild = ctx.guild or bot.get_guild(SERVER_ID)
    if not guild:
        return await ctx.send("⚠️ Guild não encontrada.")

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT campaign_id FROM (
            SELECT campaign_id FROM campaign_members WHERE user_id=?
            UNION
            SELECT campaign_id FROM submissions WHERE user_id=?
            UNION
            SELECT campaign_id FROM campaign_users WHERE user_id=?
        )
    """, (int(user_id), int(user_id), int(user_id)))
    cids = [int(r[0]) for r in cur.fetchall()]
    conn.close()

    if not cids:
        return await ctx.send(f"✅ Nada para limpar. user_id `{user_id}` não aparece em nenhuma campanha.")

    touched: Set[int] = set()
    for cid in cids:
        reset_user_in_campaign(int(cid), int(user_id), refund_budget=True)
        mem = await fetch_member_safe(guild, int(user_id))
        if mem:
            await remove_campaign_role_from_member(guild, int(cid), mem)
        touched.add(int(cid))

    for cid in touched:
        await update_leaderboard_for_campaign(int(cid))

    await ctx.send(f"🧼 pureghosts concluído: user_id `{user_id}` removido de {len(touched)} campanha(s).")

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
    SELECT leaderboard_channel_id, leaderboard_message_id, name, spent_kz, budget_total_kz, status, rate_kz_per_1k
    FROM campaigns WHERE id=?
    """, (campaign_id,))
    camp = cur.fetchone()
    if not camp:
        conn.close()
        return

    lb_ch_id, lb_msg_id, name, spent, budget, status, rate = camp

    cur.execute("""
    SELECT s.user_id,
           COALESCE(SUM(s.views_current),0) AS views_current_sum,
           COALESCE(cu.paid_kz,0) AS paid_kz,
           COALESCE(cu.total_views_paid,0) AS views_paid
    FROM submissions s
    JOIN campaign_members cm
      ON cm.campaign_id = s.campaign_id AND cm.user_id = s.user_id
    LEFT JOIN campaign_users cu
      ON cu.campaign_id = s.campaign_id AND cu.user_id = s.user_id
    WHERE s.campaign_id=? AND s.status='approved'
    GROUP BY s.user_id
    ORDER BY paid_kz DESC, views_current_sum DESC
    LIMIT 20
    """, (campaign_id,))
    raw_top = cur.fetchall()
    conn.close()

    if not lb_ch_id:
        return
    ch = guild.get_channel(int(lb_ch_id))
    if not ch:
        return

    top = []
    for (uid, vcur, paid_kz, vpaid) in raw_top:
        m = await fetch_member_safe(guild, int(uid))
        if m is None:
            continue
        top.append((uid, vcur, paid_kz, vpaid))
        if len(top) >= 10:
            break

    progress = pct(int(spent), int(budget)) * 100.0
    lines = [
        f"🏆 **Tabela de classificação — {name}**",
        f"💰 **Gasto:** {int(spent):,}/{int(budget):,} Kz (**{progress:.1f}%**) | 📌 **Estado:** {status}",
        f"💵 Taxa: **{int(rate)} Kz / 1.000 views**",
        ""
    ]

    if not top:
        lines.append("Ainda sem vídeos aprovados / atualizações.")
    else:
        for i, (uid, vcur, paid_kz, vpaid) in enumerate(top, 1):
            lines.append(
                f"**{i}.** <@{uid}> — **{int(paid_kz):,} Kz** | views atuais: **{int(vcur):,}** | views pagas: **{int(vpaid):,}**"
            )

    try:
        if lb_msg_id:
            msg = await ch.fetch_message(int(lb_msg_id))
            await msg.edit(content="\n".join(lines))
            return
    except:
        pass

    try:
        new_msg = await ch.send("\n".join(lines))
        connx = db_conn()
        cx = connx.cursor()
        cx.execute("UPDATE campaigns SET leaderboard_message_id=? WHERE id=?", (int(new_msg.id), int(campaign_id)))
        connx.commit()
        connx.close()
    except:
        pass

# =========================
# APIFY
# =========================
async def apify_run(actor: str, payload: dict) -> Optional[dict]:
    if not APIFY_TOKEN:
        return None

    actor_id = normalize_apify_actor_id(actor)
    run_url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={APIFY_TOKEN}"

    proxy_cfg = build_proxy_configuration()
    if proxy_cfg:
        payload = dict(payload or {})
        payload["proxyConfiguration"] = proxy_cfg

    try:
        session = await get_http_session()

        async with session.post(run_url, json=payload) as r:
            txt = await r.text()
            if r.status >= 400:
                print(f"⚠️ APIFY POST status={r.status} actor={actor_id} body={txt[:1200]}")
                return None
            data = await r.json()

            run = (data.get("data") or {})
            run_id = run.get("id")
            dataset_id = run.get("defaultDatasetId")
            if not run_id or not dataset_id:
                print("⚠️ APIFY: run sem id/dataset:", data)
                return None

        status = None
        last_run_info = None
        for _ in range(35):
            async with session.get(f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}") as rr:
                rr_txt = await rr.text()
                if rr.status >= 400:
                    print(f"⚠️ APIFY RUN status={rr.status} body={rr_txt[:1200]}")
                    return None
                rd = await rr.json()
                last_run_info = (rd.get("data") or {})
                status = last_run_info.get("status")
                if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                    break
            await asyncio.sleep(2)

        if status != "SUCCEEDED":
            print("⚠️ APIFY: run status:", status, "error:", (last_run_info or {}).get("errorMessage"))
            return None

        async with session.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&clean=true&limit=5"
        ) as ri:
            ri_txt = await ri.text()
            if ri.status >= 400:
                print(f"⚠️ APIFY DATASET status={ri.status} body={ri_txt[:1200]}")
                return None
            items = await ri.json()

        if not items or not isinstance(items, list):
            print("⚠️ APIFY: dataset sem items. payload usado:", payload)
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
        "videoPlayCount", "video_play_count", "playCountText"
    ]

    for k in candidates:
        v = item.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            hv = parse_human_number(v)
            if hv is not None:
                return hv

    stats = item.get("stats") or {}
    if isinstance(stats, dict):
        for k in ["playCount", "viewCount", "views", "videoViewCount", "plays"]:
            v = stats.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                hv = parse_human_number(v)
                if hv is not None:
                    return hv

    vmeta = item.get("videoMeta") or {}
    if isinstance(vmeta, dict):
        for k in ["playCount", "viewCount", "views"]:
            v = vmeta.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                hv = parse_human_number(v)
                if hv is not None:
                    return hv

    return None

async def apify_get_views_for_url(url: str) -> Optional[int]:
    platform = detect_platform(url)

    if platform == "tiktok":
        clean = normalize_tiktok_url(url)
        payloads = [
            {"postURLs": [clean], "resultsPerPage": 1, "scrapeRelatedVideos": False},
            {"startUrls": [{"url": clean}], "maxItems": 1},
            {"directUrls": [clean], "resultsPerPage": 1},
            {"videoUrls": [clean]},
        ]
        for p in payloads:
            item = await apify_run(APIFY_ACTOR_TIKTOK, p)
            v = extract_views_from_item(item) if item else None
            if isinstance(v, int) and v >= 0:
                return v
        return None

    if platform == "instagram":
        payloads = [
            {"directUrls": [url], "resultsType": "posts", "resultsLimit": 1},
            {"startUrls": [{"url": url}], "resultsLimit": 1},
        ]
        for p in payloads:
            item = await apify_run(APIFY_ACTOR_INSTAGRAM, p)
            v = extract_views_from_item(item) if item else None
            if isinstance(v, int) and v >= 0:
                return v
        return None

    return None

# =========================
# VIEWS REFRESH
# =========================
async def refresh_views_once() -> None:
    if not APIFY_TOKEN:
        return

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT s.id, s.campaign_id, s.user_id, s.post_url, s.paid_views,
           c.rate_kz_per_1k, c.budget_total_kz, c.spent_kz, c.max_payout_user_kz, c.status, COALESCE(c.ended_notified,0)
    FROM submissions s
    JOIN campaigns c ON c.id = s.campaign_id
    WHERE s.status='approved' AND c.status='active'
    """)
    rows = cur.fetchall()
    conn.close()

    touched_campaigns = set()

    for (sub_id, camp_id, user_id, url, paid_views,
         rate, budget_total, spent_kz, max_user_kz, _camp_status, ended_notified) in rows:

        remaining_budget_now = max(0, int(budget_total) - int(spent_kz))
        if remaining_budget_now <= 0:
            connx = db_conn()
            cx = connx.cursor()
            cx.execute("UPDATE campaigns SET status='ended' WHERE id=?", (int(camp_id),))
            connx.commit()
            connx.close()

            touched_campaigns.add(int(camp_id))
            if int(ended_notified) == 0:
                mark_campaign_ended_notified(int(camp_id))
                await notify_campaign_finished(int(camp_id), winner_user_id=None, reason="budget")
            continue

        views = await apify_get_views_for_url(url)
        if views is None:
            print(f"⚠️ Views None (Apify) url={url}")
            continue

        conn2 = db_conn()
        cur2 = conn2.cursor()

        cur2.execute("UPDATE submissions SET views_current=? WHERE id=?", (int(views), int(sub_id)))

        payable_total = (int(views) // 1000) * 1000
        to_pay_views = payable_total - int(paid_views)

        if to_pay_views < 1000 or int(rate) <= 0:
            conn2.commit()
            conn2.close()
            touched_campaigns.add(int(camp_id))
            continue

        to_pay_kz = (to_pay_views // 1000) * int(rate)

        cur2.execute("SELECT COALESCE(paid_kz,0), COALESCE(maxed_notified,0) FROM campaign_users WHERE campaign_id=? AND user_id=?",
                     (int(camp_id), int(user_id)))
        rowu = cur2.fetchone()
        already_paid_kz = int(rowu[0]) if rowu else 0
        maxed_notified_u = int(rowu[1]) if rowu else 0

        remaining_user_kz = max(0, int(max_user_kz) - already_paid_kz)
        if remaining_user_kz <= 0:
            conn2.commit()
            conn2.close()
            touched_campaigns.add(int(camp_id))

            if maxed_notified_u == 0:
                guild = bot.get_guild(SERVER_ID)
                if guild:
                    mem = await fetch_member_safe(guild, int(user_id))
                    if mem:
                        await notify_user(
                            mem,
                            f"✅ Atingiste o teu limite nesta campanha (**{int(max_user_kz):,} Kz**). "
                            "A partir de agora **não podes submeter mais vídeos** para esta campanha.",
                            fallback_channel_id=CHAT_CHANNEL_ID
                        )
                set_maxed_notified(int(camp_id), int(user_id))
            continue

        if to_pay_kz > remaining_user_kz:
            max_blocks = remaining_user_kz // int(rate)
            to_pay_views = max_blocks * 1000
            to_pay_kz = max_blocks * int(rate)

        remaining_budget = max(0, int(budget_total) - int(spent_kz))
        if to_pay_kz > remaining_budget:
            max_blocks = remaining_budget // int(rate)
            to_pay_views = max_blocks * 1000
            to_pay_kz = max_blocks * int(rate)

        if to_pay_kz <= 0:
            conn2.commit()
            conn2.close()
            touched_campaigns.add(int(camp_id))
            continue

        cur2.execute("""
        INSERT INTO campaign_users (campaign_id, user_id, paid_kz, total_views_paid, maxed_notified)
        VALUES (?, ?, ?, ?, 0)
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

        new_paid = already_paid_kz + int(to_pay_kz)
        if new_paid >= int(max_user_kz) and maxed_notified_u == 0:
            guild = bot.get_guild(SERVER_ID)
            if guild:
                mem = await fetch_member_safe(guild, int(user_id))
                if mem:
                    await notify_user(
                        mem,
                        f"✅ Atingiste o teu limite nesta campanha (**{int(max_user_kz):,} Kz**). "
                        "A partir de agora **não podes submeter mais vídeos** para esta campanha.",
                        fallback_channel_id=CHAT_CHANNEL_ID
                    )
            set_maxed_notified(int(camp_id), int(user_id))

        new_spent = int(spent_kz) + int(to_pay_kz)
        if new_spent >= int(budget_total):
            connz = db_conn()
            cz = connz.cursor()
            cz.execute("SELECT COALESCE(ended_notified,0) FROM campaigns WHERE id=?", (int(camp_id),))
            en = int((cz.fetchone() or [0])[0] or 0)
            cz.execute("UPDATE campaigns SET status='ended' WHERE id=?", (int(camp_id),))
            connz.commit()
            connz.close()

            if en == 0:
                mark_campaign_ended_notified(int(camp_id))
                await notify_campaign_finished(int(camp_id), winner_user_id=int(user_id), reason="budget")

    for cid in touched_campaigns:
        await update_leaderboard_for_campaign(int(cid))

@tasks.loop(minutes=VIEWS_REFRESH_MINUTES)
async def refresh_views_loop():
    try:
        await refresh_views_once()
    except Exception as e:
        print("⚠️ refresh_views_loop erro:", e)
        traceback.print_exc()

@refresh_views_loop.before_loop
async def before_refresh_views():
    await bot.wait_until_ready()

# =========================
# INTERACTIONS
# =========================
@bot.event
async def on_interaction(interaction: discord.Interaction):
    try:
        if interaction.type == discord.InteractionType.component:
            data = interaction.data or {}
            custom_id = (data.get("custom_id") or "").strip()
            if not custom_id:
                return

            guild = interaction.guild or bot.get_guild(SERVER_ID)
            if not guild:
                return

            # CONNECT
            if custom_id == "vz:connect":
                code = generate_verification_code()
                await safe_reply(
                    interaction,
                    "Escolhe a rede social para ligar.\n\n"
                    f"🔑 O teu código será: `{code}`",
                    ephemeral=True,
                    view=ChooseSocialView(code)
                )
                return

            if custom_id.startswith("vz:connect:"):
                parts = custom_id.split(":")
                if len(parts) >= 4:
                    social = parts[2]
                    code = parts[3]
                    await safe_send_modal(interaction, UsernameModal(social=social, code=code), fallback_text="⚠️ Tenta novamente ligar a conta.")
                else:
                    await safe_reply(interaction, "⚠️ Botão inválido.", ephemeral=True)
                return

            if custom_id == "vz:view_account":
                vr = get_verification_request(int(interaction.user.id))
                iban = get_iban(int(interaction.user.id))
                linked = list_linked_accounts(int(interaction.user.id))

                status = "NÃO LIGADO"
                social = "-"
                username = "-"
                if vr:
                    _, social, username, _code, st, _, _ = vr
                    status = str(st).upper()

                iban_txt = "NÃO DEFINIDO"
                if iban and iban[0]:
                    raw = str(iban[0])
                    iban_txt = raw[:6] + "…" + raw[-4:] if len(raw) > 12 else raw

                if linked:
                    linked_lines = []
                    for s, u, _ts in linked:
                        linked_lines.append(f"• **{social_pretty_name(str(s))}**: {u}")
                    linked_txt = "\n".join(linked_lines)
                    linked_view: Optional[discord.ui.View] = LinkedAccountsManageView(linked)
                else:
                    linked_txt = "Nenhuma conta associada."
                    linked_view = None

                await safe_reply(
                    interaction,
                    "👤 **A tua conta**\n"
                    f"📌 Estado do último pedido: **{status}**\n"
                    f"📱 Última rede do pedido: **{social_pretty_name(str(social)) if social != '-' else '-'}**\n"
                    f"🏷️ Último username do pedido: **{username}**\n"
                    f"🏦 IBAN: **{iban_txt}**\n\n"
                    f"🔗 **Contas associadas**\n{linked_txt}",
                    ephemeral=True,
                    view=linked_view
                )
                return

            # UNLINK ACCOUNTS
            if custom_id.startswith("vz:unlink:"):
                member = await fetch_member_safe(guild, interaction.user.id)
                if not member or not is_verified(member):
                    return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para gerir contas.", ephemeral=True)

                social = custom_id.split(":")[-1].lower()
                row = get_linked_account(int(interaction.user.id), social)
                if not row:
                    return await safe_reply(interaction, f"⚠️ Não tens nenhuma conta de **{social_pretty_name(social)}** associada.", ephemeral=True)

                delete_linked_account(int(interaction.user.id), social)
                return await safe_reply(interaction, f"✅ Conta de **{social_pretty_name(social)}** removida com sucesso.", ephemeral=True)

            # IBAN
            if custom_id == "vz:iban:add":
                member = await fetch_member_safe(guild, interaction.user.id)
                if not member or not is_verified(member):
                    return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para guardar IBAN.", ephemeral=True)
                await safe_send_modal(interaction, IbanModal(), fallback_text="⚠️ Tenta novamente abrir o painel de IBAN.")
                return

            if custom_id == "vz:iban:view":
                member = await fetch_member_safe(guild, interaction.user.id)
                if not member or not is_verified(member):
                    return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para ver IBAN.", ephemeral=True)
                row = get_iban(int(interaction.user.id))
                if not row:
                    return await safe_reply(interaction, "⚠️ Ainda não tens IBAN guardado.", ephemeral=True)
                raw = str(row[0])
                masked = raw[:6] + "…" + raw[-4:] if len(raw) > 12 else raw
                await safe_reply(interaction, f"🏦 O teu IBAN: **{masked}**", ephemeral=True)
                return

            if custom_id == "vz:iban:delete":
                member = await fetch_member_safe(guild, interaction.user.id)
                if not member or not is_verified(member):
                    return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para gerir IBAN.", ephemeral=True)

                row = get_iban(int(interaction.user.id))
                if not row:
                    return await safe_reply(interaction, "⚠️ Ainda não tens IBAN guardado.", ephemeral=True)

                delete_iban(int(interaction.user.id))
                return await safe_reply(interaction, "✅ IBAN apagado com sucesso.", ephemeral=True)

            # SUPPORT
            if custom_id == "vz:support:campaign":
                await safe_send_modal(interaction, SupportCampaignModal(), fallback_text="⚠️ Tenta novamente abrir **Problema com campanha**.")
                return
            if custom_id == "vz:support:question":
                await safe_send_modal(interaction, SupportQuestionModal(), fallback_text="⚠️ Tenta novamente abrir **Dúvidas**.")
                return

            # CLOSE TICKET
            if custom_id == "vz:ticket:close":
                member = await fetch_member_safe(guild, interaction.user.id)
                if not member or not is_staff_member(member):
                    return await safe_reply(interaction, "⛔ Só o staff pode fechar tickets.", ephemeral=True)

                try:
                    ch = interaction.channel
                    if isinstance(ch, discord.Thread):
                        await ch.send("✅ Ticket fechado pelo staff. Obrigado!")
                        await ch.edit(archived=True, locked=True)
                        return await safe_reply(interaction, "✅ Ticket fechado.", ephemeral=True)
                    return await safe_reply(interaction, "⚠️ Este botão só funciona dentro do ticket (thread).", ephemeral=True)
                except Exception as e:
                    print("⚠️ fechar ticket erro:", e)
                    return await safe_reply(interaction, "⚠️ Não consegui fechar o ticket agora.", ephemeral=True)

            # VERIFY APPROVAL
            if custom_id.startswith("vz:verify:approve:") or custom_id.startswith("vz:verify:reject:"):
                member = await fetch_member_safe(guild, interaction.user.id)
                if not member or not is_staff_member(member):
                    return await safe_reply(interaction, "⛔ Sem permissão.", ephemeral=True)

                is_approve = custom_id.startswith("vz:verify:approve:")
                user_id = int(custom_id.split(":")[-1])

                target_member = await fetch_member_safe(guild, user_id)
                if not target_member:
                    return await safe_reply(interaction, "⚠️ Utilizador não encontrado.", ephemeral=True)

                vr = get_verification_request(user_id)
                social = None
                username = None
                if vr:
                    _, social, username, _code, _st, _, _ = vr

                if is_approve:
                    role = guild.get_role(VERIFICADO_ROLE_ID)
                    if role:
                        try:
                            await target_member.add_roles(role, reason="Viralizzaa verification approved")
                        except:
                            pass

                    set_verification_status(user_id, "approved")

                    if social and username:
                        existing = get_linked_account(user_id, str(social).lower())
                        if not existing:
                            add_linked_account(user_id, str(social).lower(), str(username).strip())

                    await notify_user(
                        target_member,
                        "✅ A tua verificação foi **aprovada**!\n\n"
                        "🏦 Agora adiciona o teu **IBAN Angolano** (para receber pagamentos).",
                        fallback_channel_id=CHAT_CHANNEL_ID,
                        view=IbanButtons()
                    )
                    await safe_reply(interaction, "✅ Verificação aprovada.", ephemeral=True)
                else:
                    set_verification_status(user_id, "rejected")
                    await notify_user(target_member, "❌ A tua verificação foi **rejeitada**.", fallback_channel_id=CHAT_CHANNEL_ID)
                    await safe_reply(interaction, "✅ Verificação rejeitada.", ephemeral=True)

                try:
                    await interaction.message.edit(view=None)
                except:
                    pass
                return

            # JOIN CAMPAIGN
            if custom_id == "vz:camp:join":
                m = await fetch_member_safe(guild, interaction.user.id)
                if not m or not is_verified(m):
                    return await safe_reply(interaction, "⛔ Tens de estar **Verificado** para aderir a campanhas.", ephemeral=True)

                msg_id = getattr(interaction.message, "id", None)
                if not msg_id:
                    return await safe_reply(interaction, "⚠️ Não consegui identificar a campanha.", ephemeral=True)

                conn = db_conn()
                cur = conn.cursor()
                cur.execute("""
                    SELECT id, name, slug, platforms, content_types, audio_url,
                           rate_kz_per_1k, budget_total_kz, max_payout_user_kz, max_posts_total,
                           status, category_id, campaign_role_id
                    FROM campaigns
                    WHERE post_message_id=?
                    LIMIT 1
                """, (int(msg_id),))
                camp = cur.fetchone()
                conn.close()
                if not camp:
                    return await safe_reply(interaction, "❌ Campanha não encontrada para este post.", ephemeral=True)

                (camp_id, name, slug, platforms, content_types, audio_url,
                 rate, budget_total, max_user_kz, max_posts_total,
                 status, category_id, campaign_role_id) = camp

                if str(status) != "active":
                    return await safe_reply(interaction, "⚠️ Esta campanha já terminou.", ephemeral=True)

                campaign_role = await ensure_campaign_role(guild, int(camp_id), str(slug), campaign_role_id)
                await ensure_campaign_workspace_private(
                    guild=guild,
                    camp_id=int(camp_id),
                    name=str(name),
                    slug=str(slug),
                    platforms=str(platforms),
                    content_types=str(content_types),
                    audio_url=str(audio_url or ""),
                    rate=int(rate),
                    budget_total=int(budget_total),
                    max_user_kz=int(max_user_kz),
                    max_posts_total=int(max_posts_total),
                    category_id=category_id,
                    campaign_role=campaign_role
                )

                add_campaign_member(int(camp_id), int(interaction.user.id))

                try:
                    await m.add_roles(campaign_role, reason="Joined campaign")
                except:
                    pass

                await safe_reply(
                    interaction,
                    f"✅ Aderiste à campanha **{name}**!\n\n"
                    f"Vai ao canal de submissão da campanha para enviar links.",
                    ephemeral=True
                )
                return

            # LEAVE CAMPAIGN
            if custom_id.startswith("vz:camp:leave:"):
                camp_id = int(custom_id.split(":")[-1])

                if not is_campaign_member(int(camp_id), int(interaction.user.id)):
                    return await safe_reply(interaction, "⚠️ Tu não estás nesta campanha.", ephemeral=True)

                reset_user_in_campaign(int(camp_id), int(interaction.user.id), refund_budget=True)

                mem = await fetch_member_safe(guild, interaction.user.id)
                if mem:
                    await remove_campaign_role_from_member(guild, int(camp_id), mem)

                await update_leaderboard_for_campaign(int(camp_id))
                await safe_reply(interaction, "✅ Saíste da campanha e foi feito reset (vídeos/estatísticas removidos).", ephemeral=True)
                return

            # SUBMIT PANEL
            if custom_id.startswith("vz:submit:open:"):
                camp_id = int(custom_id.split(":")[-1])
                await safe_send_modal(interaction, SubmitLinkModal(camp_id), fallback_text="⚠️ Tenta novamente clicar em **Submeter link**.")
                return

            if custom_id.startswith("vz:submit:remove:"):
                camp_id = int(custom_id.split(":")[-1])
                await safe_send_modal(interaction, RemoveLinkModal(camp_id), fallback_text="⚠️ Tenta novamente clicar em **Retirar vídeo**.")
                return

            if custom_id.startswith("vz:submit:stats:"):
                camp_id = int(custom_id.split(":")[-1])

                if not is_campaign_member(int(camp_id), int(interaction.user.id)):
                    return await safe_reply(interaction, "⛔ Primeiro tens de **aderir** à campanha.", ephemeral=True)

                approved, pending, total = get_user_submission_counts(int(camp_id), int(interaction.user.id))

                conn = db_conn()
                cur = conn.cursor()
                cur.execute("SELECT COALESCE(paid_kz,0), COALESCE(total_views_paid,0) FROM campaign_users WHERE campaign_id=? AND user_id=?",
                            (int(camp_id), int(interaction.user.id)))
                row = cur.fetchone() or (0, 0)
                paid_kz, views_paid = int(row[0] or 0), int(row[1] or 0)

                cur.execute("""
                    SELECT COALESCE(SUM(views_current),0)
                    FROM submissions
                    WHERE campaign_id=? AND user_id=? AND status='approved'
                """, (int(camp_id), int(interaction.user.id)))
                views_current_sum = int((cur.fetchone() or [0])[0] or 0)
                conn.close()

                await safe_reply(
                    interaction,
                    "📊 **As tuas estatísticas nesta campanha**\n"
                    f"✅ Aprovados: **{approved}/{MAX_APPROVED_PER_USER}**\n"
                    f"⏳ Pendentes: **{pending}**\n"
                    f"📥 Total submetidos (ativos): **{total}**\n\n"
                    f"💰 Pago (estimado): **{paid_kz:,} Kz**\n"
                    f"👁️ Views pagas: **{views_paid:,}**\n"
                    f"👀 Views atuais (aprovados): **{views_current_sum:,}**",
                    ephemeral=True
                )
                return

            # SUBMISSION APPROVAL
            if custom_id.startswith("vz:sub:approve:") or custom_id.startswith("vz:sub:reject:"):
                staff = await fetch_member_safe(guild, interaction.user.id)
                if not staff or not is_staff_member(staff):
                    return await safe_reply(interaction, "⛔ Sem permissão.", ephemeral=True)

                is_approve = custom_id.startswith("vz:sub:approve:")
                submission_id = int(custom_id.split(":")[-1])

                conn = db_conn()
                cur = conn.cursor()
                cur.execute("""
                    SELECT s.id, s.campaign_id, s.user_id, s.post_url, s.status,
                           c.name, c.status, c.max_payout_user_kz, s.platform
                    FROM submissions s
                    JOIN campaigns c ON c.id = s.campaign_id
                    WHERE s.id=?
                """, (int(submission_id),))
                row = cur.fetchone()
                if not row:
                    conn.close()
                    return await safe_reply(interaction, "❌ Submission não encontrada.", ephemeral=True)

                sid, camp_id, user_id, post_url, _st, camp_name, camp_status, max_user_kz, platform = row
                camp_id = int(camp_id); user_id = int(user_id)
                post_url = str(post_url)
                max_user_kz = int(max_user_kz)
                platform = str(platform)

                target_member = await fetch_member_safe(guild, user_id)

                if not is_campaign_member(camp_id, user_id):
                    cur.execute("UPDATE submissions SET status='rejected' WHERE id=?", (int(sid),))
                    conn.commit()
                    conn.close()
                    if target_member:
                        await notify_user(
                            target_member,
                            "❌ O teu link não foi aprovado porque já não estás na campanha.\n"
                            f"🔗 {post_url}",
                            fallback_channel_id=CHAT_CHANNEL_ID
                        )
                    try:
                        await interaction.message.edit(view=None)
                    except:
                        pass
                    await safe_reply(interaction, "✅ Rejeitado (user já saiu da campanha).", ephemeral=True)
                    await update_leaderboard_for_campaign(int(camp_id))
                    return

                if str(camp_status) != "active" and is_approve:
                    cur.execute("UPDATE submissions SET status='rejected' WHERE id=?", (int(sid),))
                    conn.commit()
                    conn.close()
                    if target_member:
                        await notify_user(
                            target_member,
                            "❌ O teu link não foi aprovado porque a campanha já terminou.\n"
                            f"🔗 {post_url}",
                            fallback_channel_id=CHAT_CHANNEL_ID
                        )
                    try:
                        await interaction.message.edit(view=None)
                    except:
                        pass
                    await safe_reply(interaction, "✅ Não aprovado (campanha terminada).", ephemeral=True)
                    await update_leaderboard_for_campaign(int(camp_id))
                    return

                paid_kz, _mn = get_user_paid_in_campaign(int(camp_id), int(user_id))
                if paid_kz >= max_user_kz and is_approve:
                    cur.execute("UPDATE submissions SET status='rejected' WHERE id=?", (int(sid),))
                    conn.commit()
                    conn.close()
                    if target_member:
                        await notify_user(
                            target_member,
                            f"⛔ O teu link não foi aprovado porque já atingiste o teu limite (**{max_user_kz:,} Kz**) nesta campanha.\n"
                            f"🔗 {post_url}",
                            fallback_channel_id=CHAT_CHANNEL_ID
                        )
                    try:
                        await interaction.message.edit(view=None)
                    except:
                        pass
                    await safe_reply(interaction, "✅ Rejeitado (limite individual atingido).", ephemeral=True)
                    await update_leaderboard_for_campaign(int(camp_id))
                    return

                if is_approve:
                    approved_count, _, _ = get_user_submission_counts(camp_id, user_id)
                    if approved_count >= MAX_APPROVED_PER_USER:
                        cur.execute("UPDATE submissions SET status='rejected' WHERE id=?", (int(sid),))
                        conn.commit()
                        conn.close()
                        if target_member:
                            await notify_user(
                                target_member,
                                f"⛔ O teu link não foi aprovado porque já tens **{MAX_APPROVED_PER_USER} vídeos aprovados** nesta campanha.\n"
                                f"🔗 {post_url}",
                                fallback_channel_id=CHAT_CHANNEL_ID
                            )
                        try:
                            await interaction.message.edit(view=None)
                        except:
                            pass
                        await safe_reply(interaction, "✅ Rejeitado (limite de aprovados atingido).", ephemeral=True)
                        await update_leaderboard_for_campaign(int(camp_id))
                        return

                    cur.execute("UPDATE submissions SET status='approved', approved_at=? WHERE id=?", (_now(), int(sid)))
                    conn.commit()
                    conn.close()

                    linked = get_linked_account(user_id, platform)
                    linked_txt = linked[0] if linked else "Não encontrada"

                    if target_member:
                        await notify_user(
                            target_member,
                            "✅ O teu vídeo foi **aprovado**!\n"
                            f"📱 Conta {social_pretty_name(platform)}: **{linked_txt}**\n"
                            f"🔗 {post_url}",
                            fallback_channel_id=CHAT_CHANNEL_ID
                        )

                    await safe_reply(interaction, f"✅ Aprovado. (Campanha: {camp_name})", ephemeral=True)

                    try:
                        await interaction.message.edit(view=None)
                    except:
                        pass

                    await update_leaderboard_for_campaign(int(camp_id))
                    return

                else:
                    conn.close()
                    await safe_send_modal(
                        interaction,
                        RejectSubmissionReasonModal(
                            submission_id=int(sid),
                            campaign_id=int(camp_id),
                            user_id=int(user_id),
                            post_url=str(post_url),
                            camp_name=str(camp_name),
                        ),
                        fallback_text="⚠️ Não consegui abrir a caixa do motivo da rejeição."
                    )
                    return

        try:
            await bot.process_application_commands(interaction)  # type: ignore
        except Exception:
            pass

    except Exception as e:
        print("⚠️ on_interaction erro:", e)
        traceback.print_exc()
        try:
            await safe_reply(interaction, "⚠️ Ocorreu um erro ao processar a interação.", ephemeral=True)
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
        bot.add_view(CloseTicketView())
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
# WEB
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
# SHUTDOWN
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
