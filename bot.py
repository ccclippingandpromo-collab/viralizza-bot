import os

import sqlite3

import threading



import discord

from discord.ext import commands

from flask import Flask



print("DISCORD VERSION:", getattr(discord, "__version__", "unknown"))

print("DISCORD FILE:", getattr(discord, "__file__", "unknown"))



# =========================

# CONFIG (TEUS IDS)

# =========================

SERVER_ID = 1473469552917741678

VERIFICACOES_CHANNEL_ID = 1473886076476067850

VERIFICADO_ROLE_ID = 1473886534439538699

ADMIN_USER_ID = 1376499031890460714



SUPORTE_STAFF_CHANNEL_ID = 1474938549181874320



DB_PATH = "database.sqlite3"



# =========================

# BOT / INTENTS

# =========================

intents = discord.Intents.default()

intents.message_content = True

intents.members = True



bot = commands.Bot(command_prefix="!", intents=intents)



# =========================

# MEMÓRIA (pendentes/verificados)

# =========================

pending_accounts = {}   # user_id -> {"social":..., "username":..., "code":..., "status":"pending"}

verified_accounts = {}  # user_id -> {"social":..., "username":..., "code":..., "status":"verified"}



# =========================

# DB INIT (IBAN + SUPORTE)

# =========================

def init_db():

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()



    # IBAN

    cur.execute("""

        CREATE TABLE IF NOT EXISTS ibans (

            user_id INTEGER PRIMARY KEY,

            iban TEXT NOT NULL,

            updated_at TEXT NOT NULL

        )

    """)



    # SUPORTE

    cur.execute("""

        CREATE TABLE IF NOT EXISTS support_tickets (

            thread_id INTEGER PRIMARY KEY,

            user_id INTEGER NOT NULL,

            status TEXT NOT NULL DEFAULT 'open',

            created_at TEXT DEFAULT CURRENT_TIMESTAMP

        )

    """)



    conn.commit()

    conn.close()



# ===== IBAN HELPERS =====

def set_iban(user_id: int, iban: str):

    conn = sqlite3.connect(DB_PATH)

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

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute("SELECT iban, updated_at FROM ibans WHERE user_id=?", (user_id,))

    row = cur.fetchone()

    conn.close()

    return row



# ===== SUPORTE HELPERS =====

def set_ticket(thread_id: int, user_id: int):

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute(

        "INSERT OR REPLACE INTO support_tickets(thread_id, user_id, status) VALUES (?, ?, 'open')",

        (thread_id, user_id)

    )

    conn.commit()

    conn.close()



def get_open_thread_for_user(user_id: int):

    conn = sqlite3.connect(DB_PATH)

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

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute("SELECT user_id FROM support_tickets WHERE thread_id=? AND status='open'", (thread_id,))

    row = cur.fetchone()

    conn.close()

    return int(row[0]) if row else None



def close_ticket(thread_id: int):

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute("UPDATE support_tickets SET status='closed' WHERE thread_id=?", (thread_id,))

    conn.commit()

    conn.close()



# =========================

# UTILS

# =========================

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



# =========================

# SUPORTE: Criar ticket

# =========================

async def criar_ticket(interaction: discord.Interaction, tipo: str, conteudo: str):

    staff_channel = interaction.client.get_channel(SUPORTE_STAFF_CHANNEL_ID)

    if not staff_channel:

        await interaction.response.send_message("❌ Canal de suporte do staff não encontrado.", ephemeral=True)

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

        await interaction.response.send_message(

            "❌ O bot não tem permissão para criar threads no canal suporte-staff.",

            ephemeral=True

        )

        return



    set_ticket(thread.id, interaction.user.id)



    try:

        await interaction.user.send(

            "✅ **Ticket aberto com o staff!**\n\n"

            "A partir de agora, responde **aqui por DM** e eu vou encaminhar ao staff.\n"

            "Quando o staff responder, vais receber aqui também.\n\n"

            "⚠️ Se não receberes DMs: abre as DMs do servidor."

        )

    except discord.Forbidden:

        await thread.send("⚠️ Não consegui enviar DM ao user (DMs fechadas).")



    await interaction.response.send_message(

        "✅ Pedido enviado ao staff! Verifica as tuas DMs para continuar o suporte.",

        ephemeral=True

    )



    await thread.send("🟢 Ticket aberto. Tudo que o user escrever por DM vai cair aqui. Staff respondam aqui.")



# =========================

# SUPORTE: Modals + View

# =========================

class CampanhaModal(discord.ui.Modal):

    def __init__(self):

        super().__init__(title="Problema sobre campanha")



        self.campanha = discord.ui.TextInput(

            label="Nome da campanha",

            placeholder="Ex: Campanha AfroBeat",

            required=True,

            max_length=80

        )

        self.problema = discord.ui.TextInput(

            label="Qual é o problema?",

            style=discord.TextStyle.paragraph,

            required=True,

            max_length=1000

        )



        self.add_item(self.campanha)

        self.add_item(self.problema)



    async def on_submit(self, interaction: discord.Interaction):

        texto = f"📢 Campanha: {self.campanha.value}\n⚠️ Problema: {self.problema.value}"

        await criar_ticket(interaction, "Problema com campanha", texto)





class DuvidaModal(discord.ui.Modal):

    def __init__(self):

        super().__init__(title="Dúvidas")



        self.duvida = discord.ui.TextInput(

            label="Escreve a tua dúvida",

            style=discord.TextStyle.paragraph,

            required=True,

            max_length=1000

        )



        self.add_item(self.duvida)



    async def on_submit(self, interaction: discord.Interaction):

        await criar_ticket(interaction, "Dúvida", self.duvida.value)





class SuporteView(discord.ui.View):

    def __init__(self):

        super().__init__(timeout=None)



    @discord.ui.button(

        label="📢 Problema sobre campanha",

        style=discord.ButtonStyle.danger,

        custom_id="support_btn_campaign"

    )

    async def btn_campaign(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(CampanhaModal())



    @discord.ui.button(

        label="❓ Dúvidas",

        style=discord.ButtonStyle.primary,

        custom_id="support_btn_question"

    )

    async def btn_question(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(DuvidaModal())





@commands.has_permissions(administrator=True)

@bot.command()

async def painel_suporte(ctx):

    await ctx.send(

        "🆘 **SUPORTE VIRALIZZAA**\n\n"

        "Escolhe uma opção abaixo para falares com o staff:\n"

        "📢 Problema sobre campanha\n"

        "❓ Dúvidas gerais\n\n"

        "✅ As respostas do staff vão chegar por DM.",

        view=SuporteView()

    )



@commands.has_permissions(manage_messages=True)

@bot.command()

async def fechar_ticket(ctx):

    if not isinstance(ctx.channel, discord.Thread):

        await ctx.send("❌ Usa este comando dentro do thread do ticket.")

        return



    close_ticket(ctx.channel.id)

    await ctx.send("🔒 Ticket fechado.")

    await ctx.channel.edit(archived=True, locked=True)



# =========================

# VERIFICAÇÃO: Modal Username

# =========================

class UsernameModal(discord.ui.Modal):

    def __init__(self, social: str, code: str):

        super().__init__(title="Ligar Conta")

        self.social = social

        self.code = code



        self.username = discord.ui.TextInput(

            label="Coloca o teu username",

            placeholder="@teu_username",

            required=True,

            max_length=64

        )

        self.add_item(self.username)



    async def on_submit(self, interaction: discord.Interaction):

        user_id = interaction.user.id



        pending_accounts[user_id] = {

            "social": self.social,

            "username": str(self.username.value).strip(),

            "code": self.code,

            "status": "pending"

        }



        await interaction.response.send_message(

            "✅ Pedido enviado!\n\n"

            f"📱 Rede: {self.social}\n"

            f"👤 Username: {pending_accounts[user_id]['username']}\n"

            f"🔑 Código: {self.code}\n\n"

            "🔒 Isto serve para confirmar que a conta é realmente tua.\n\n"

            "⚠️ INSTRUÇÕES IMPORTANTES:\n"

            "1. Vai ao teu perfil do TikTok\n"

            "2. Coloca este código na tua BIO\n"

            "3. Guarda as alterações\n\n"

            "📌 Exemplo:\n"

            f"Bio: {self.code}\n\n"

            "⏳ Depois disso, aguarda a aprovação do staff.\n"

            "❗ Não removas o código até seres verificado.",

            ephemeral=True

        )



        guild = bot.get_guild(SERVER_ID)

        if not guild:

            return



        channel = guild.get_channel(VERIFICACOES_CHANNEL_ID)

        if not channel:

            return



        view = ApprovalView(target_user_id=user_id)

        await channel.send(

            f"🆕 **Novo pedido de verificação**\n"

            f"👤 User: {interaction.user.mention} (`{user_id}`)\n"

            f"📱 Rede: **{self.social}**\n"

            f"🏷️ Username: **{pending_accounts[user_id]['username']}**\n"

            f"🔑 Código: `{self.code}`\n"

            f"📌 Status: **PENDENTE**",

            view=view

        )



# =========================

# SELECT (TikTok / YouTube / Instagram)

# =========================

class SocialSelect(discord.ui.Select):

    def __init__(self):

        options = [

            discord.SelectOption(label="TikTok", emoji="🎵"),

            discord.SelectOption(label="YouTube", emoji="📺"),

            discord.SelectOption(label="Instagram", emoji="📸"),

        ]

        super().__init__(

            placeholder="Escolhe a rede social",

            min_values=1,

            max_values=1,

            options=options,

            custom_id="social_select"

        )



    async def callback(self, interaction: discord.Interaction):

        social = interaction.data["values"][0]

        code = f"VZ-{interaction.user.id}"

        await interaction.response.send_modal(UsernameModal(social=social, code=code))



# =========================

# MAIN VIEW (Painel de ligar)

# =========================

class ConnectButton(discord.ui.Button):

    def __init__(self):

        super().__init__(

            label="Conectar rede social",

            style=discord.ButtonStyle.green,

            custom_id="btn_connect_social"

        )



    async def callback(self, interaction: discord.Interaction):

        v = discord.ui.View(timeout=120)

        v.add_item(SocialSelect())

        await interaction.response.send_message("Escolhe a rede social:", view=v, ephemeral=True)



class ViewAccountsButton(discord.ui.Button):

    def __init__(self):

        super().__init__(

            label="Ver minha conta",

            style=discord.ButtonStyle.blurple,

            custom_id="btn_view_account"

        )



    async def callback(self, interaction: discord.Interaction):

        account = verified_accounts.get(interaction.user.id)

        if not account:

            msg = "❌ Nenhuma conta verificada ainda."

        else:

            msg = (

                "✅ **Conta verificada**\n"

                f"📱 Rede: {account['social']}\n"

                f"🏷️ Username: {account['username']}\n"

                f"🔑 Código: `{account['code']}`"

            )

        await interaction.response.send_message(msg, ephemeral=True)



class MainView(discord.ui.View):

    def __init__(self):

        super().__init__(timeout=None)

        self.add_item(ConnectButton())

        self.add_item(ViewAccountsButton())



# =========================

# IBAN: Modal + View

# =========================

class IbanModal(discord.ui.Modal):

    def __init__(self):

        super().__init__(title="Adicionar / Atualizar IBAN")

        self.iban = discord.ui.TextInput(

            label="Escreve o teu IBAN",

            placeholder="AO06 0000 0000 0000 0000 0000 0",

            required=True,

            max_length=64

        )

        self.add_item(self.iban)



    async def on_submit(self, interaction: discord.Interaction):

        guild = interaction.guild or bot.get_guild(SERVER_ID)

        if not guild:

            return await interaction.response.send_message("⚠️ Servidor não encontrado.", ephemeral=True)



        member = await fetch_member_safe(guild, interaction.user.id)

        if not member or not is_verified(member):

            return await interaction.response.send_message("⛔ Tens de estar **Verificado** para guardar IBAN.", ephemeral=True)



        set_iban(interaction.user.id, str(self.iban.value).strip())

        await interaction.response.send_message("✅ IBAN guardado com sucesso.", ephemeral=True)



class IbanButtons(discord.ui.View):

    def __init__(self):

        super().__init__(timeout=None)



    @discord.ui.button(label="Adicionar / Atualizar IBAN", style=discord.ButtonStyle.primary, custom_id="iban_add")

    async def add_iban(self, interaction: discord.Interaction, button: discord.ui.Button):

        guild = interaction.guild or bot.get_guild(SERVER_ID)

        if not guild:

            return await interaction.response.send_message("⚠️ Servidor não encontrado.", ephemeral=True)



        member = await fetch_member_safe(guild, interaction.user.id)

        if not member or not is_verified(member):

            return await interaction.response.send_message("⛔ Tens de estar **Verificado** para adicionar IBAN.", ephemeral=True)



        await interaction.response.send_modal(IbanModal())



    @discord.ui.button(label="Ver meu IBAN", style=discord.ButtonStyle.secondary, custom_id="iban_view")

    async def view_iban(self, interaction: discord.Interaction, button: discord.ui.Button):

        guild = interaction.guild or bot.get_guild(SERVER_ID)

        if not guild:

            return await interaction.response.send_message("⚠️ Servidor não encontrado.", ephemeral=True)



        member = await fetch_member_safe(guild, interaction.user.id)

        if not member or not is_verified(member):

            return await interaction.response.send_message("⛔ Tens de estar **Verificado** para ver IBAN.", ephemeral=True)



        row = get_iban(interaction.user.id)

        if not row:

            return await interaction.response.send_message("Ainda não tens IBAN guardado.", ephemeral=True)



        iban, updated_at = row

        await interaction.response.send_message(

            f"✅ Teu IBAN: **{iban}**\n🕒 Atualizado: {updated_at}",

            ephemeral=True

        )



# =========================

# APROVAR / REJEITAR

# =========================

class ApprovalView(discord.ui.View):

    def __init__(self, target_user_id: int):

        super().__init__(timeout=None)

        self.target_user_id = target_user_id



    async def _only_admin(self, interaction: discord.Interaction) -> bool:

        if interaction.user.id != ADMIN_USER_ID:

            await interaction.response.send_message("⛔ Só o admin pode aprovar/rejeitar.", ephemeral=True)

            return False

        return True



    @discord.ui.button(label="✅ Aprovar", style=discord.ButtonStyle.green, custom_id="approve_btn")

    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not await self._only_admin(interaction):

            return



        data = pending_accounts.get(self.target_user_id)

        if not data:

            await interaction.response.send_message("⚠️ Este pedido já não existe.", ephemeral=True)

            return



        guild = bot.get_guild(SERVER_ID)

        if not guild:

            await interaction.response.send_message("⚠️ Guild não encontrada.", ephemeral=True)

            return



        member = await fetch_member_safe(guild, self.target_user_id)

        if not member:

            await interaction.response.send_message("⚠️ Não consegui buscar o membro.", ephemeral=True)

            return



        role = guild.get_role(VERIFICADO_ROLE_ID)

        if not role:

            await interaction.response.send_message("⚠️ Cargo 'Verificado' não encontrado.", ephemeral=True)

            return



        try:

            await member.add_roles(role, reason="Verificação aprovada")

        except discord.Forbidden:

            await interaction.response.send_message(

                "⛔ Sem permissões para dar cargo. (Cargo do bot precisa estar acima do 'Verificado')",

                ephemeral=True

            )

            return



        data["status"] = "verified"

        verified_accounts[self.target_user_id] = data

        pending_accounts.pop(self.target_user_id, None)



        try:

            await member.send(

                "✅ **Verificação aprovada!**\n"

                f"📱 Rede: {data['social']}\n"

                f"🏷️ Username: {data['username']}\n\n"

                "Agora podes adicionar o teu IBAN aqui 👇",

                view=IbanButtons()

            )

        except:

            pass



        for child in self.children:

            child.disabled = True



        await interaction.message.edit(

            content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **APROVADO ✅**"),

            view=self

        )

        await interaction.response.send_message("✅ Aprovado e cargo atribuído.", ephemeral=True)



    @discord.ui.button(label="❌ Rejeitar", style=discord.ButtonStyle.red, custom_id="reject_btn")

    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not await self._only_admin(interaction):

            return



        data = pending_accounts.get(self.target_user_id)

        if not data:

            await interaction.response.send_message("⚠️ Este pedido já não existe.", ephemeral=True)

            return



        guild = bot.get_guild(SERVER_ID)

        member = await fetch_member_safe(guild, self.target_user_id) if guild else None



        if member:

            try:

                await member.send(

                    "❌ **Verificação rejeitada.**\n"

                    "Confere se o username está certo e tenta novamente."

                )

            except:

                pass



        pending_accounts.pop(self.target_user_id, None)



        for child in self.children:

            child.disabled = True



        await interaction.message.edit(

            content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **REJEITADO ❌**"),

            view=self

        )

        await interaction.response.send_message("❌ Rejeitado.", ephemeral=True)



# =========================

# COMANDOS (LIGAR / IBAN)

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

async def iban(ctx, member: discord.Member = None):

    if ctx.author.id != ADMIN_USER_ID:

        return await ctx.send("⛔ Só o admin pode usar este comando.")



    if member is None:

        return await ctx.send("Usa: `!iban @user`")



    row = get_iban(member.id)

    if not row:

        return await ctx.send(f"❌ {member.mention} não tem IBAN guardado.")



    iban_value, updated_at = row

    await ctx.send(f"🏦 IBAN de {member.mention}: **{iban_value}** | 🕒 {updated_at}")



# =========================

# ON_MESSAGE (UM SÓ) - Relay suporte + comandos

# =========================

@bot.event

async def on_message(message: discord.Message):

    if message.author.bot:

        return



    # 1) User -> staff (DM)

    if isinstance(message.channel, discord.DMChannel):

        thread_id = get_open_thread_for_user(message.author.id)

        if not thread_id:

            await message.channel.send("❌ Não encontrei ticket aberto. Abre um ticket em #💬┃suporte.")

            return



        thread = bot.get_channel(thread_id)

        if thread is None:

            try:

                thread = await bot.fetch_channel(thread_id)

            except:

                await message.channel.send("❌ Não consegui encontrar o ticket (talvez foi fechado).")

                return



        await thread.send(f"👤 **{message.author} (DM):**\n{message.content}")

        return



    # 2) Staff -> user (thread)

    if isinstance(message.channel, discord.Thread):

        user_id = get_user_for_thread(message.channel.id)

        if user_id:

            try:

                user = bot.get_user(user_id) or await bot.fetch_user(user_id)

                await user.send(f"🛠 **Staff:**\n{message.content}")

            except discord.Forbidden:

                await message.channel.send("⚠️ Não consegui enviar DM ao user (DMs fechadas).")

        return



    await bot.process_commands(message)



# =========================

# READY (registrar views persistentes)

# =========================

@bot.event

async def on_ready():

    init_db()



    if not getattr(bot, "_views_added", False):

        bot.add_view(MainView())

        bot.add_view(IbanButtons())

        bot.add_view(SuporteView())

        # ApprovalView não precisa add_view global porque é criado por target_user_id (dinâmico)

        bot._views_added = True



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

TOKEN = os.getenv("DISCORD_TOKEN")

if not TOKEN:

    raise RuntimeError("DISCORD_TOKEN não encontrado. Define a variável DISCORD_TOKEN na Railway/Render.")



keep_alive()

bot.run(TOKEN)

