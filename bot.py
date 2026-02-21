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



DB_PATH = "database.sqlite3"



# =========================

# BOT / INTENTS

# =========================

intents = discord.Intents.default()

intents.message_content = True

intents.members = True



bot = commands.Bot(command_prefix="!", intents=intents)



# =========================

# "DB" EM MEMÓRIA (pendentes/verificados)

# (Isto reseta ao reiniciar — IBAN é que fica persistente no SQLite)

# =========================

pending_accounts = {}   # user_id -> {"social":..., "username":..., "code":..., "status":"pending"}

verified_accounts = {}  # user_id -> {"social":..., "username":..., "code":..., "status":"verified"}



# =========================

# SQLITE (IBAN PERSISTENTE)

# =========================

def init_db():

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute("""

        CREATE TABLE IF NOT EXISTS ibans (

            user_id INTEGER PRIMARY KEY,

            iban TEXT NOT NULL,

            updated_at TEXT NOT NULL

        )

    """)

    conn.commit()

    conn.close()



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



def is_verified(member: discord.Member) -> bool:

    role = member.guild.get_role(VERIFICADO_ROLE_ID)

    return bool(role) and (role in member.roles)



# =========================

# MODALS

# =========================

class UsernameModal(discord.ui.Modal):

    def __init__(self, social: str, code: str):

        super().__init__(title="Ligar Conta")

        self.social = social

        self.code = code



        self.username = discord.ui.InputText(

            label="Coloca o teu username",

            placeholder="@teu_username",

            required=True,

            max_length=64

        )

        self.add_item(self.username)



    async def callback(self, interaction: discord.Interaction):

        user_id = interaction.user.id



        pending_accounts[user_id] = {

            "social": self.social,

            "username": str(self.username.value).strip(),

            "code": self.code,

            "status": "pending"

        }



        await interaction.response.send_message(

            f"✅ Pedido enviado!\n"

            f"**Rede:** {self.social}\n"

            f"**Username:** {pending_accounts[user_id]['username']}\n"

            f"**Código:** `{self.code}`\n\n"

            f"⏳ Agora aguarda a aprovação do staff.",

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



class IbanModal(discord.ui.Modal):

    def __init__(self):

        super().__init__(title="Adicionar / Atualizar IBAN")

        self.iban = discord.ui.TextInput(

            label="Escreve o teu IBAN",

            placeholder="AO06 0000 0000 0000 0000 0000 0",

            required=True,

            max_length=64,

        )

        self.add_item(self.iban)



    async def on_submit(self, interaction: discord.Interaction):

        # Só verificados

        guild = interaction.guild or bot.get_guild(SERVER_ID)

        if not guild:

            return await interaction.response.send_message("⚠️ Servidor não encontrado.", ephemeral=True)



        member = guild.get_member(interaction.user.id)

        if not member:

            try:

                member = await guild.fetch_member(interaction.user.id)

            except:

                member = None



        if not member or not is_verified(member):

            return await interaction.response.send_message(

                "⛔ Tens de estar **Verificado** para guardar IBAN.",

                ephemeral=True,

            )



        iban_value = str(self.iban.value).strip()

        set_iban(interaction.user.id, iban_value)



        await interaction.response.send_message("✅ IBAN guardado com sucesso.", ephemeral=True)



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

            custom_id="social_select",

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

            custom_id="btn_connect_social",

        )



    async def callback(self, interaction: discord.Interaction):

        view = discord.ui.View(timeout=120)

        view.add_item(SocialSelect())

        await interaction.response.send_message("Escolhe a rede social:", view=view, ephemeral=True)



class ViewAccountsButton(discord.ui.Button):

    def __init__(self):

        super().__init__(

            label="Ver minha conta",

            style=discord.ButtonStyle.blurple,

            custom_id="btn_view_account",

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

        super().__init__(timeout=None)  # persistente

        self.add_item(ConnectButton())

        self.add_item(ViewAccountsButton())



# =========================

# IBAN VIEW (persistente)

# =========================

class IbanButtons(discord.ui.View):

    def __init__(self):

        super().__init__(timeout=None)



    @discord.ui.button(

        label="Adicionar / Atualizar IBAN",

        style=discord.ButtonStyle.primary,

        custom_id="iban_add",

    )

    async def add_iban(self, interaction: discord.Interaction, button: discord.ui.Button):

        guild = interaction.guild or bot.get_guild(SERVER_ID)

        if not guild:

            return await interaction.response.send_message("⚠️ Servidor não encontrado.", ephemeral=True)



        member = guild.get_member(interaction.user.id)

        if not member:

            try:

                member = await guild.fetch_member(interaction.user.id)

            except:

                member = None



        if not member or not is_verified(member):

            return await interaction.response.send_message(

                "⛔ Tens de estar **Verificado** para adicionar IBAN.",

                ephemeral=True,

            )



        await interaction.response.send_modal(IbanModal())



    @discord.ui.button(

        label="Ver meu IBAN",

        style=discord.ButtonStyle.secondary,

        custom_id="iban_view",

    )

    async def view_iban(self, interaction: discord.Interaction, button: discord.ui.Button):

        guild = interaction.guild or bot.get_guild(SERVER_ID)

        if not guild:

            return await interaction.response.send_message("⚠️ Servidor não encontrado.", ephemeral=True)



        member = guild.get_member(interaction.user.id)

        if not member:

            try:

                member = await guild.fetch_member(interaction.user.id)

            except:

                member = None



        if not member or not is_verified(member):

            return await interaction.response.send_message(

                "⛔ Tens de estar **Verificado** para ver IBAN.",

                ephemeral=True,

            )



        row = get_iban(interaction.user.id)

        if not row:

            return await interaction.response.send_message("Ainda não tens IBAN guardado.", ephemeral=True)



        iban, updated_at = row

        await interaction.response.send_message(

            f"✅ Teu IBAN: **{iban}**\n🕒 Atualizado: {updated_at}",

            ephemeral=True,

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



        member = guild.get_member(self.target_user_id)

        if not member:

            try:

                member = await guild.fetch_member(self.target_user_id)

            except:

                member = None



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

                ephemeral=True,

            )

            return



        data["status"] = "verified"

        verified_accounts[self.target_user_id] = data

        pending_accounts.pop(self.target_user_id, None)



        # DM ao user + manda os botões de IBAN

        try:

            await member.send(

                "✅ **Verificação aprovada!**\n"

                f"📱 Rede: {data['social']}\n"

                f"🏷️ Username: {data['username']}\n\n"

                "Agora podes adicionar o teu IBAN aqui 👇",

                view=IbanButtons(),

            )

        except:

            pass



        for child in self.children:

            child.disabled = True



        await interaction.message.edit(

            content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **APROVADO ✅**"),

            view=self,

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

        member = None

        if guild:

            member = guild.get_member(self.target_user_id)

            if not member:

                try:

                    member = await guild.fetch_member(self.target_user_id)

                except:

                    member = None



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

            view=self,

        )

        await interaction.response.send_message("❌ Rejeitado.", ephemeral=True)



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

    """Opcional: manda o painel do IBAN no servidor (mas só verificados vão conseguir usar)."""

    if ctx.guild and ctx.guild.id != SERVER_ID:

        return

    await ctx.send("**Painel IBAN (apenas verificados)**", view=IbanButtons())



@bot.command()

async def iban(ctx, member: discord.Member = None):

    """Admin: ver IBAN de alguém com !iban @user"""

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

# READY (registrar views persistentes)

# =========================

@bot.event

async def on_ready():

    init_db()



    if not getattr(bot, "_views_added", False):

        bot.add_view(MainView())

        bot.add_view(IbanButtons())

        bot._views_added = True



    print(f"✅ Bot ligado como {bot.user}!")



# =========================

# WEB (Railway)

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

    raise RuntimeError("DISCORD_TOKEN não encontrado. Define a variável DISCORD_TOKEN na Railway.")



keep_alive()

bot.run(TOKEN)


