import discord

from discord.ext import commands

from discord.ui import Button, View, Select



# =========================

#  CONFIG (TEUS IDS)

# =========================

SERVER_ID = 1473469552917741678

VERIFICACOES_CHANNEL_ID = 1473886076476067850

CAMPANHAS_CHANNEL_ID = 1473888170256105584

VERIFICADO_ROLE_ID = 1473886534439538699

ADMIN_USER_ID = 1376499031890460714



# =========================

#  BOT / INTENTS

# =========================

intents = discord.Intents.default()

intents.message_content = True

intents.members = True  # necessário para dar cargos



bot = commands.Bot(command_prefix="!", intents=intents)



# =========================

#  "DB" SIMPLES EM MEMÓRIA

# =========================

pending_accounts = {}   # user_id -> {"social":..., "username":..., "code":..., "status":"pending"}

verified_accounts = {}  # user_id -> {"social":..., "username":..., "code":..., "status":"verified"}





# =========================

#  VIEWS / UI

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



        # Guarda pedido pendente

        pending_accounts[user_id] = {

            "social": self.social,

            "username": str(self.username.value).strip(),

            "code": self.code,

            "status": "pending"

        }



        # Mensagem ao user + mostra o "código"

        await interaction.response.send_message(

            f"✅ Pedido enviado!\n"

            f"**Rede:** {self.social}\n"

            f"**Username:** {pending_accounts[user_id]['username']}\n"

            f"**Código:** `{self.code}`\n\n"

            f"⏳ Agora aguarda a aprovação do staff.",

            ephemeral=True

        )



        # Envia para o canal de verificações com botões (aprovar/rejeitar)

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

            options=options

        )



    async def callback(self, interaction: discord.Interaction):

        social = self.values[0]

        code = f"VZ-{interaction.user.id}"



        # abre modal para username

        await interaction.response.send_modal(UsernameModal(social=social, code=code))





class ConnectButton(Button):

    def __init__(self):

        super().__init__(label="Conectar rede social", style=discord.ButtonStyle.green)



    async def callback(self, interaction: discord.Interaction):

        view = View()

        view.add_item(SocialSelect())

        await interaction.response.send_message(

            "Escolhe a rede social:",

            view=view,

            ephemeral=True

        )





class ViewAccountsButton(Button):

    def __init__(self):

        super().__init__(label="Ver minha conta", style=discord.ButtonStyle.blurple)



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





class MainView(View):

    def __init__(self):

        super().__init__(timeout=None)

        self.add_item(ConnectButton())

        self.add_item(ViewAccountsButton())





# =========================

#  APROVAR / REJEITAR (BOTÕES)

# =========================

class ApprovalView(View):

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



        # dá cargo

        try:

            await member.add_roles(role, reason="Verificação aprovada")

        except discord.Forbidden:

            await interaction.response.send_message(

                "⛔ Sem permissões para dar cargo. (O cargo do bot precisa estar acima do 'Verificado')",

                ephemeral=True

            )

            return



        # move de pending -> verified

        data["status"] = "verified"

        verified_accounts[self.target_user_id] = data

        pending_accounts.pop(self.target_user_id, None)



        # DM ao user

        try:

            await member.send(

                "✅ **Verificação aprovada!**\n"

                f"📱 Rede: {data['social']}\n"

                f"🏷️ Username: {data['username']}\n\n"

                "Já tens acesso às campanhas."

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

            view=self

        )

        await interaction.response.send_message("❌ Rejeitado.", ephemeral=True)





# =========================

#  COMANDO PARA ENVIAR O PAINEL

# =========================

@bot.command()

async def ligar(ctx):

    if ctx.guild and ctx.guild.id != SERVER_ID:

        return

    await ctx.send("**Ligar conta e verificar**", view=MainView())





# =========================

#  READY

# =========================

@bot.event

async def on_ready():

    print(f"✅ Bot ligado como {bot.user}!")





# =========================

#  RUN

# =========================
import os

TOKEN = os.getenv("TOKEN")
bot.run(TOKEN)