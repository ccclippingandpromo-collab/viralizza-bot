import os

import sqlite3

import discord

from discord.ext import commands

from discord.ui import View, Button, Select



# =========================

# CONFIG (TEUS IDS)

# =========================

SERVER_ID = 1473469552917741678

VERIFICACOES_CHANNEL_ID = 1473886076476067850

CAMPANHAS_CHANNEL_ID = 1473888170256105584  # (não usado aqui, mas deixei)

VERIFICADO_ROLE_ID = 1473886534439538699

ADMIN_USER_ID = 1376499031890460714



DB_PATH = "database.sqlite3"





# =========================

# DB (SQLite)

# =========================

def init_db():

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute(

        """

        CREATE TABLE IF NOT EXISTS ibans (

            user_id INTEGER PRIMARY KEY,

            iban TEXT NOT NULL,

            updated_at TEXT NOT NULL

        )

        """

    )

    conn.commit()

    conn.close()





def set_iban(user_id: int, iban: str):

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute(

        """

        INSERT INTO ibans (user_id, iban, updated_at)

        VALUES (?, ?, datetime('now'))

        ON CONFLICT(user_id) DO UPDATE SET

            iban=excluded.iban,

            updated_at=datetime('now')

        """,

        (user_id, iban),

    )

    conn.commit()

    conn.close()





def get_iban(user_id: int):

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute("SELECT iban, updated_at FROM ibans WHERE user_id = ?", (user_id,))

    row = cur.fetchone()

    conn.close()

    return row





# =========================

# BOT / INTENTS

# =========================

intents = discord.Intents.default()

intents.message_content = True

intents.members = True



bot = commands.Bot(command_prefix="!", intents=intents)



# "DB" em memória para pedidos de verificação

pending_accounts = {}   # user_id -> {"social":..., "username":..., "code":..., "status":"pending"}

verified_accounts = {}  # user_id -> {"social":..., "username":..., "code":..., "status":"verified"}





# =========================

# HELPERS

# =========================

def is_verified_member(guild: discord.Guild, user_id: int) -> bool:

    role = guild.get_role(VERIFICADO_ROLE_ID)

    if not role:

        return False

    member = guild.get_member(user_id)

    if not member:

        return False

    return role in member.roles





# =========================

# MODALS

# =========================

class UsernameModal(discord.ui.Modal, title="Ligar Conta"):

    username = discord.ui.TextInput(

        label="Coloca o teu username",

        placeholder="@teu_username",

        required=True,

        max_length=64,

    )



    def __init__(self, social: str, code: str):

        super().__init__()

        self.social = social

        self.code = code



    async def on_submit(self, interaction: discord.Interaction):

        user_id = interaction.user.id



        pending_accounts[user_id] = {

            "social": self.social,

            "username": str(self.username.value).strip(),

            "code": self.code,

            "status": "pending",

        }



        await interaction.response.send_message(

            "✅ Pedido enviado!\n"

            f"**Rede:** {self.social}\n"

            f"**Username:** {pending_accounts[user_id]['username']}\n"

            f"**Código:** `{self.code}`\n\n"

            "⏳ Agora aguarda a aprovação do staff.",

            ephemeral=True,

        )



        guild = bot.get_guild(SERVER_ID)

        if not guild:

            return



        channel = guild.get_channel(VERIFICACOES_CHANNEL_ID)

        if not channel:

            return



        view = ApprovalView(target_user_id=user_id)

        await channel.send(

            "🆕 **Novo pedido de verificação**\n"

            f"👤 User: {interaction.user.mention} (`{user_id}`)\n"

            f"📱 Rede: **{self.social}**\n"

            f"🏷️ Username: **{pending_accounts[user_id]['username']}**\n"

            f"🔑 Código: `{self.code}`\n"

            "📌 Status: **PENDENTE**",

            view=view,

        )





class IbanModal(discord.ui.Modal, title="Adicionar / Atualizar IBAN"):

    iban = discord.ui.TextInput(

        label="Escreve o teu IBAN",

        placeholder="AO06 0000 0000 0000 0000 0000 0",

        required=True,

        max_length=64,

    )



    async def on_submit(self, interaction: discord.Interaction):

        # só permite se estiver verificado

        guild = interaction.guild or bot.get_guild(SERVER_ID)

        if not guild or not is_verified_member(guild, interaction.user.id):

            return await interaction.response.send_message(

                "⛔ Tens de estar **Verificado** para guardar IBAN.",

                ephemeral=True,

            )



        iban_value = str(self.iban.value).strip()

        set_iban(interaction.user.id, iban_value)



        await interaction.response.send_message(

            f"✅ IBAN guardado com sucesso: **{iban_value}**",

            ephemeral=True,

        )





# =========================

# UI: SELEÇÃO DE SOCIAL

# =========================

class SocialSelect(Select):

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

        social = self.values[0]

        code = f"VZ-{interaction.user.id}"

        await interaction.response.send_modal(UsernameModal(social=social, code=code))





class ConnectButton(Button):

    def __init__(self):

        super().__init__(

            label="Conectar rede social",

            style=discord.ButtonStyle.green,

            custom_id="btn_connect_social",

        )



    async def callback(self, interaction: discord.Interaction):

        view = View(timeout=None)

        view.add_item(SocialSelect())

        await interaction.response.send_message(

            "Escolhe a rede social:",

            view=view,

            ephemeral=True,

        )





class ViewAccountsButton(Button):

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





class IbanButtons(View):

    def __init__(self):

        super().__init__(timeout=None)



    @discord.ui.button(

        label="Adicionar / Atualizar IBAN",

        style=discord.ButtonStyle.primary,

        custom_id="iban_add",

    )

    async def add_iban(self, interaction: discord.Interaction, button: discord.ui.Button):

        guild = interaction.guild or bot.get_guild(SERVER_ID)

        if not guild or not is_verified_member(guild, interaction.user.id):

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

        if not guild or not is_verified_member(guild, interaction.user.id):

            return await interaction.response.send_message(

                "⛔ Tens de estar **Verificado** para ver IBAN.",

                ephemeral=True,

            )



        row = get_iban(interaction.user.id)

        if not row:

            return await interaction.response.send_message(

                "Ainda não tens IBAN guardado.",

                ephemeral=True,

            )



        iban, updated_at = row

        await interaction.response.send_message(

            f"✅ Teu IBAN: **{iban}**\n🕒 Atualizado: {updated_at}",

            ephemeral=True,

        )





class MainView(View):

    def __init__(self):

        super().__init__(timeout=None)

        self.add_item(ConnectButton())

        self.add_item(ViewAccountsButton())





# =========================

# APROVAR / REJEITAR

# =========================

class ApprovalView(View):

    def __init__(self, target_user_id: int):

        super().__init__(timeout=None)

        self.target_user_id = target_user_id



    async def _only_admin(self, interaction: discord.Interaction) -> bool:

        if interaction.user.id != ADMIN_USER_ID:

            await interaction.response.send_message(

                "⛔ Só o admin pode aprovar/rejeitar.",

                ephemeral=True,

            )

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

                "⛔ Sem permissões para dar cargo. (O cargo do bot precisa estar acima do 'Verificado')",

                ephemeral=True,

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

                "Já tens acesso às campanhas e ao IBAN."

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

async def iban(ctx):

    if ctx.guild and ctx.guild.id != SERVER_ID:

        return

    await ctx.send("**IBAN (apenas para verificados)**", view=IbanButtons())





# =========================

# READY

# =========================

@bot.event

async def on_ready():

    init_db()



    # Regista views persistentes (para não morrerem após restart)

    bot.add_view(MainView())

    bot.add_view(IbanButtons())



    print(f"✅ Bot ligado como {bot.user}!")





# =========================

# RUN

# =========================

TOKEN = os.getenv("TOKEN")

if not TOKEN:

    raise RuntimeError("⚠️ TOKEN não encontrado. Define a variável de ambiente TOKEN no Render.")

bot.run(TOKEN)
