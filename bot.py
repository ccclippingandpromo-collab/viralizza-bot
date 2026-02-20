import os

import sqlite3

from datetime import datetime



import discord

from discord.ext import commands





# =========================

# CONFIG (TEUS IDS)

# =========================

SERVER_ID = 1473469552917741678

VERIFICACOES_CHANNEL_ID = 1473886076476067850

VERIFICADO_ROLE_ID = 1473886534439538699

ADMIN_USER_ID = 1376499031890460714





# =========================

# BOT / INTENTS

# =========================

intents = discord.Intents.default()

intents.message_content = True

intents.members = True  # necessário para dar cargos



bot = commands.Bot(command_prefix="!", intents=intents)





# =========================

# "DB" SIMPLES (MEMÓRIA)

# (pendentes/verificados perdem ao reiniciar)

# =========================

pending_accounts = {}   # user_id -> {"social":..., "username":..., "code":..., "status":"pending"}

verified_accounts = {}  # user_id -> {"social":..., "username":..., "code":..., "status":"verified"}





# =========================

# SQLITE (IBAN PERSISTENTE)

# =========================

DB_PATH = "database.db"



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

        VALUES (?, ?, ?)

        ON CONFLICT(user_id) DO UPDATE SET

            iban=excluded.iban,

            updated_at=excluded.updated_at

    """, (user_id, iban, datetime.utcnow().isoformat()))

    conn.commit()

    conn.close()



def get_iban(user_id: int):

    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()

    cur.execute("SELECT iban, updated_at FROM ibans WHERE user_id=?", (user_id,))

    row = cur.fetchone()

    conn.close()

    return row





# =========================

# MODAL + VIEW DO IBAN

# =========================

class IbanModal(discord.ui.Modal, title="Adicionar / Atualizar IBAN"):

    iban = discord.ui.TextInput(

        label="Escreve o teu IBAN",

        placeholder="AO06 0000 0000 0000 0000 0000 0",

        required=True,

        max_length=60

    )



    async def on_submit(self, interaction: discord.Interaction):

        iban_value = str(self.iban.value).strip()

        set_iban(interaction.user.id, iban_value)

        await interaction.response.send_message("✅ IBAN guardado com sucesso.", ephemeral=True)



class IbanButtons(discord.ui.View):

    def __init__(self):

        super().__init__(timeout=None)



    @discord.ui.button(

        label="Adicionar / Atualizar IBAN",

        style=discord.ButtonStyle.primary,

        custom_id="iban_add"

    )

    async def add_iban(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(IbanModal())



    @discord.ui.button(

        label="Ver meu IBAN",

        style=discord.ButtonStyle.secondary,

        custom_id="iban_view"

    )

    async def view_iban(self, interaction: discord.Interaction, button: discord.ui.Button):

        row = get_iban(interaction.user.id)

        if not row:

            return await interaction.response.send_message(

                "Ainda não tens IBAN guardado.",

                ephemeral=True

            )



        iban, updated_at = row

        await interaction.response.send_message(

            f"✅ Teu IBAN: **{iban}**\n🕒 Atualizado: {updated_at}",

            ephemeral=True

        )





# =========================

# UI PRINCIPAL (LIGAR CONTA)

# =========================

class UsernameModal(discord.ui.Modal, title="Ligar Conta"):

    username = discord.ui.TextInput(

        label="Coloca o teu username",

        placeholder="@teu_username",

        required=True

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

            options=options

        )



    async def callback(self, interaction: discord.Interaction):

        social = self.values[0]

        code = f"VZ-{interaction.user.id}"

        await interaction.response.send_modal(UsernameModal(social=social, code=code))





class ConnectButton(discord.ui.Button):

    def __init__(self):

        super().__init__(label="Conectar rede social", style=discord.ButtonStyle.green)



    async def callback(self, interaction: discord.Interaction):

        view = discord.ui.View()

        view.add_item(SocialSelect())

        await interaction.response.send_message(

            "Escolhe a rede social:",

            view=view,

            ephemeral=True

        )





class ViewAccountsButton(discord.ui.Button):

    def __init__(self):

        super().__init__(label="Ver minha conta", style=discord.ButtonStyle.blurple)



    async def callback(self, interaction: discord.Interaction):

        account = verified_accounts.get(interaction.user.id)



        if not account:

            msg = "❌ Nenhuma conta verificada ainda."

        else:

            row = get_iban(interaction.user.id)

            iban_txt = "❌ (ainda não guardaste IBAN)"

            if row:

                iban_txt = f"✅ `{row[0]}`"



            msg = (

                "✅ **Conta verificada**\n"

                f"📱 Rede: {account['social']}\n"

                f"🏷️ Username: {account['username']}\n"

                f"🔑 Código: `{account['code']}`\n"

                f"🏦 IBAN: {iban_txt}"

            )



        await interaction.response.send_message(msg, ephemeral=True)





class MainView(discord.ui.View):

    def __init__(self):

        super().__init__(timeout=None)

        self.add_item(ConnectButton())

        self.add_item(ViewAccountsButton())





# =========================

# APROVAR / REJEITAR (ADMIN)

# =========================

class ApprovalView(discord.ui.View):

    def __init__(self, target_user_id: int):

        super().__init__(timeout=None)

        self.target_user_id = target_user_id



    async def _only_admin(self, interaction: discord.Interaction) -> bool:

        if interaction.user.id != ADMIN_USER_ID:

            await interaction.response.send_message(

                "⛔ Só o admin pode aprovar/rejeitar.",

                ephemeral=True

            )

            return False

        return True



    @discord.ui.button(label="✅ Aprovar", style=discord.ButtonStyle.green)

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



        try:

            member = guild.get_member(self.target_user_id) or await guild.fetch_member(self.target_user_id)

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

                "⛔ Sem permissões para dar cargo. (Cargo do bot precisa estar acima do 'Verificado')",

                ephemeral=True

            )

            return



        # move pending -> verified

        data["status"] = "verified"

        verified_accounts[self.target_user_id] = data

        pending_accounts.pop(self.target_user_id, None)



        # DM ao user + botão de IBAN (só depois de aprovado)

        try:

            await member.send(

                "✅ **Verificação aprovada!**\n"

                f"📱 Rede: {data['social']}\n"

                f"🏷️ Username: {data['username']}\n\n"

                "Agora adiciona o teu IBAN aqui:",

                view=IbanButtons()

            )

        except:

            pass



        # Atualiza msg e desativa botões

        for child in self.children:

            child.disabled = True



        await interaction.message.edit(

            content=interaction.message.content.replace("📌 Status: **PENDENTE**", "📌 Status: **APROVADO ✅**"),

            view=self

        )

        await interaction.response.send_message("✅ Aprovado e cargo atribuído.", ephemeral=True)



    @discord.ui.button(label="❌ Rejeitar", style=discord.ButtonStyle.red)

    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not await self._only_admin(interaction):

            return



        data = pending_accounts.get(self.target_user_id)

        if not data:

            await interaction.response.send_message("⚠️ Este pedido já não existe.", ephemeral=True)

            return



        guild = bot.get_guild(SERVER_ID)

        if guild:

            try:

                member = guild.get_member(self.target_user_id) or await guild.fetch_member(self.target_user_id)

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

            view=self

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

async def iban(ctx, member: discord.Member = None):

    # comando admin: !iban @user

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

# READY (registar views persistentes)

# =========================

@bot.event

async def on_ready():

    init_db()



    # evita registar views várias vezes se o bot reconectar

    if not getattr(bot, "_views_added", False):

        bot.add_view(MainView())      # para o painel principal continuar a funcionar após restart

        bot.add_view(IbanButtons())   # para botões do IBAN persistirem após restart

        bot._views_added = True



    print(f"✅ Bot ligado como {bot.user}!")





# =========================

# RUN

# =========================

TOKEN = os.getenv("TOKEN")

if not TOKEN:

    raise RuntimeError("⚠️ TOKEN não encontrado. Define a variável de ambiente TOKEN no Render.")



bot.run(TOKEN)
