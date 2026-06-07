import asyncio
import json
import os
import random
import string
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

import discord
from aiohttp import ClientError, ClientSession, web
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENV / PUBLIC CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DEFAULT_STORE_URL = os.getenv("DEFAULT_STORE_URL", "https://www.moealturej.com").strip()
ROTATING_STATUSES = [
    status.strip()
    for status in os.getenv("ROTATING_STATUSES", "Watching /help,moealturej support,Watching tickets").split(",")
    if status.strip()
]
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0").strip()
WEB_PORT = int(os.getenv("PORT", os.getenv("WEB_PORT", "8080")))
KEEP_ALIVE_URL = os.getenv("KEEP_ALIVE_URL", "").strip()

# =========================
# BASIC SETTINGS
# =========================
DATA_FILE = Path("bot_data.json")
EMBED_COLOR = 0x7C3AED
ERROR_COLOR = 0xEF4444
SUCCESS_COLOR = 0x22C55E
STARTED_AT = datetime.now(timezone.utc)
web_runner: Optional[web.AppRunner] = None

TICKET_TYPES = {
    "general": {
        "label": "General support",
        "description": "Get help with general questions.",
        "emoji": "💬",
        "support_role_key": "ticket_role_general",
    },
    "hwid": {
        "label": "Key HWID reset",
        "description": "Request a HWID reset for your key.",
        "emoji": "🔑",
        "support_role_key": "ticket_role_hwid",
    },
    "key_not_received": {
        "label": "Key not received",
        "description": "Get help if your key was not delivered.",
        "emoji": "📦",
        "support_role_key": "ticket_role_key_not_received",
    },
}

# =========================
# INTENTS
# =========================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def setup_hook():
    await start_keep_alive_server()

# =========================
# LOCAL JSON DB
# =========================
def load_db() -> Dict[str, Any]:
    if not DATA_FILE.exists():
        return {"guilds": {}}
    try:
        with DATA_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {"guilds": {}}


def save_db(data: Dict[str, Any]) -> None:
    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def get_guild_config(guild_id: int) -> Dict[str, Any]:
    data = load_db()
    gid = str(guild_id)
    if gid not in data["guilds"]:
        data["guilds"][gid] = {
            "verified_role": None,
            "auto_role": None,
            "bot_admin_role": None,
            "welcome_channel": None,
            "verification_channel": None,
            "ticket_category": None,
            "ticket_panel_channel": None,
            "ticket_role_general": None,
            "ticket_role_hwid": None,
            "ticket_role_key_not_received": None,
            "store_url": DEFAULT_STORE_URL,
            "announce_image": None,
            "announce_footer": "moealturej",
            "stats_category": None,
            "stats_channels": {
                "members": None,
                "humans": None,
                "bots": None,
                "boosts": None,
            },
            "open_tickets": {},
        }
        save_db(data)

    # Keep older bot_data.json files compatible when new settings are added.
    defaults = {
        "verified_role": None,
        "auto_role": None,
        "bot_admin_role": None,
        "welcome_channel": None,
        "verification_channel": None,
        "ticket_category": None,
        "ticket_panel_channel": None,
        "ticket_role_general": None,
        "ticket_role_hwid": None,
        "ticket_role_key_not_received": None,
        "store_url": DEFAULT_STORE_URL,
        "announce_image": None,
        "announce_footer": "moealturej",
        "stats_category": None,
        "stats_channels": {
            "members": None,
            "humans": None,
            "bots": None,
            "boosts": None,
        },
        "open_tickets": {},
    }
    changed = False
    for key, value in defaults.items():
        if key not in data["guilds"][gid]:
            data["guilds"][gid][key] = value
            changed = True
    for key, value in defaults["stats_channels"].items():
        if key not in data["guilds"][gid].setdefault("stats_channels", {}):
            data["guilds"][gid]["stats_channels"][key] = value
            changed = True
    if changed:
        save_db(data)
    return data["guilds"][gid]


def update_guild_config(guild_id: int, key: str, value: Any) -> None:
    data = load_db()
    gid = str(guild_id)
    if gid not in data["guilds"]:
        get_guild_config(guild_id)
        data = load_db()
    data["guilds"][gid][key] = value
    save_db(data)


def set_nested(guild_id: int, section: str, key: str, value: Any) -> None:
    data = load_db()
    gid = str(guild_id)
    if gid not in data["guilds"]:
        get_guild_config(guild_id)
        data = load_db()
    data["guilds"][gid][section][key] = value
    save_db(data)


def add_open_ticket(guild_id: int, user_id: int, channel_id: int) -> None:
    data = load_db()
    gid = str(guild_id)
    uid = str(user_id)
    if gid not in data["guilds"]:
        get_guild_config(guild_id)
        data = load_db()
    data["guilds"][gid]["open_tickets"][uid] = channel_id
    save_db(data)


def remove_open_ticket(guild_id: int, user_id: int) -> None:
    data = load_db()
    gid = str(guild_id)
    uid = str(user_id)
    if gid in data["guilds"]:
        data["guilds"][gid].get("open_tickets", {}).pop(uid, None)
        save_db(data)

# =========================
# HELPERS
# =========================
def is_bot_admin(interaction: discord.Interaction) -> bool:
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return False

    if interaction.user.id == interaction.guild.owner_id:
        return True

    config = get_guild_config(interaction.guild.id)
    admin_role_id = config.get("bot_admin_role")
    if admin_role_id and any(role.id == admin_role_id for role in interaction.user.roles):
        return True

    # Fallback so the owner can set the admin role without breaking older setups.
    return admin_role_id is None and interaction.user.guild_permissions.manage_guild


def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not is_bot_admin(interaction):
            await interaction.response.send_message("You need the configured bot admin role to use this command.", ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)


def owner_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or interaction.user.id != interaction.guild.owner_id:
            await interaction.response.send_message("Only the server owner can use this command.", ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)


def make_embed(title: str, description: str, color: int = EMBED_COLOR) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        description=description,
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    return embed


def clean_channel_name(text: str) -> str:
    allowed = string.ascii_lowercase + string.digits + "-"
    text = text.lower().replace(" ", "-")
    return "".join(c for c in text if c in allowed)[:80]


async def safe_add_role(member: discord.Member, role_id: Optional[int], reason: str) -> bool:
    if not role_id:
        return False
    role = member.guild.get_role(role_id)
    if not role:
        return False
    try:
        await member.add_roles(role, reason=reason)
        return True
    except discord.Forbidden:
        return False


async def send_verified_dm(member: discord.Member, store_url: str) -> None:
    embed = make_embed(
        "Welcome to moealturej",
        (
            f"You are now verified in **{member.guild.name}**.\n\n"
            "You can now access the server, check updates, and open a support ticket if you need help."
        ),
        SUCCESS_COLOR,
    )
    embed.add_field(name="Store", value=store_url, inline=False)
    embed.set_thumbnail(url=member.guild.icon.url if member.guild.icon else member.display_avatar.url)
    embed.set_footer(text="moealturej verification")
    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        pass

# =========================
# VERIFICATION CAPTCHA
# =========================
class VerifyModal(discord.ui.Modal):
    def __init__(self, code: str):
        super().__init__(title="Verification Captcha")
        self.code = code
        self.answer = discord.ui.TextInput(
            label=f"Copy this text exactly: {code}",
            placeholder=code,
            required=True,
            min_length=len(code),
            max_length=len(code),
        )
        self.add_item(self.answer)

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("This only works inside a server.", ephemeral=True)

        if str(self.answer.value).strip() != self.code:
            return await interaction.response.send_message("Captcha failed. Press verify and try again.", ephemeral=True)

        config = get_guild_config(interaction.guild.id)
        ok = await safe_add_role(interaction.user, config.get("verified_role"), "User passed verification captcha")
        if not ok:
            return await interaction.response.send_message(
                "Captcha passed, but I could not give the verified role. Ask an admin to check my role position and setup.",
                ephemeral=True,
            )

        await send_verified_dm(interaction.user, config.get("store_url", DEFAULT_STORE_URL))
        await interaction.response.send_message("Verified successfully. You now have access. I also sent you a welcome DM.", ephemeral=True)


class VerifyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Verify", style=discord.ButtonStyle.success, emoji="✅", custom_id="moe_verify_button")
    async def verify_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        await interaction.response.send_modal(VerifyModal(code))

# =========================
# TICKETS
# =========================
class CloseTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, emoji="🔒", custom_id="moe_close_ticket")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.channel, discord.TextChannel):
            return await interaction.response.send_message("This can only be used inside a ticket channel.", ephemeral=True)

        topic = interaction.channel.topic or ""
        owner_id = None
        if "owner_id=" in topic:
            try:
                owner_id = int(topic.split("owner_id=")[1].split()[0])
            except ValueError:
                owner_id = None

        if not interaction.user.guild_permissions.manage_channels:
            config = get_guild_config(interaction.guild.id)
            allowed = False
            for ticket_info in TICKET_TYPES.values():
                role_id = config.get(ticket_info["support_role_key"])
                if role_id and any(role.id == role_id for role in getattr(interaction.user, "roles", [])):
                    allowed = True
            if owner_id == interaction.user.id:
                allowed = True
            if not allowed:
                return await interaction.response.send_message("You do not have permission to close this ticket.", ephemeral=True)

        await interaction.response.send_message("Closing ticket...", ephemeral=True)
        if owner_id:
            remove_open_ticket(interaction.guild.id, owner_id)
        await asyncio.sleep(2)
        await interaction.channel.delete(reason=f"Ticket closed by {interaction.user}")


class TicketSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label=info["label"],
                description=info["description"],
                emoji=info["emoji"],
                value=key,
            )
            for key, info in TICKET_TYPES.items()
        ]
        super().__init__(placeholder="Choose a ticket type...", options=options, custom_id="moe_ticket_select")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("This only works inside a server.", ephemeral=True)

        config = get_guild_config(interaction.guild.id)
        user_id = str(interaction.user.id)
        existing = config.get("open_tickets", {}).get(user_id)
        if existing:
            channel = interaction.guild.get_channel(existing)
            if channel:
                return await interaction.response.send_message(f"You already have an open ticket: {channel.mention}", ephemeral=True)
            remove_open_ticket(interaction.guild.id, interaction.user.id)

        ticket_key = self.values[0]
        ticket_info = TICKET_TYPES[ticket_key]
        category = interaction.guild.get_channel(config.get("ticket_category") or 0)
        if not isinstance(category, discord.CategoryChannel):
            return await interaction.response.send_message("Ticket category is not configured. Ask an admin to run `/set_ticket_category`.", ephemeral=True)

        support_role = interaction.guild.get_role(config.get(ticket_info["support_role_key"]) or 0)

        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True, read_message_history=True),
            interaction.guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
        }
        if support_role:
            overwrites[support_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True, read_message_history=True)

        name = clean_channel_name(f"ticket-{interaction.user.name}-{ticket_key}")
        channel = await interaction.guild.create_text_channel(
            name=name,
            category=category,
            overwrites=overwrites,
            topic=f"Ticket type={ticket_key} owner_id={interaction.user.id}",
            reason=f"Ticket opened by {interaction.user}",
        )
        add_open_ticket(interaction.guild.id, interaction.user.id, channel.id)

        ping_text = support_role.mention if support_role else "Support team"
        embed = make_embed(
            f"{ticket_info['emoji']} {ticket_info['label']}",
            f"Welcome {interaction.user.mention}. {ping_text} will help you here.\n\nUse the button below when this ticket is finished.",
        )
        await channel.send(content=f"{interaction.user.mention} {support_role.mention if support_role else ''}", embed=embed, view=CloseTicketView())
        await interaction.response.send_message(f"Ticket created: {channel.mention}", ephemeral=True)


class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketSelect())


# =========================
# KEEP-ALIVE WEB SERVER
# =========================
def bot_status_payload() -> Dict[str, Any]:
    uptime = datetime.now(timezone.utc) - STARTED_AT
    latency_ms = round(bot.latency * 1000) if bot.latency else None
    return {
        "status": "ok",
        "bot": str(bot.user) if bot.user else "starting",
        "guilds": len(bot.guilds),
        "latency_ms": latency_ms,
        "uptime_seconds": int(uptime.total_seconds()),
    }


async def keep_alive_home(request: web.Request) -> web.Response:
    payload = bot_status_payload()
    html = f"""
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>moealturej bot status</title>
        <style>
            body {{
                margin: 0;
                min-height: 100vh;
                display: grid;
                place-items: center;
                background: #050505;
                color: #fff;
                font-family: Inter, Arial, sans-serif;
            }}
            .card {{
                width: min(520px, calc(100% - 32px));
                padding: 30px;
                border: 1px solid rgba(255,255,255,.12);
                border-radius: 24px;
                background: linear-gradient(145deg, rgba(124,58,237,.18), rgba(255,255,255,.04));
                box-shadow: 0 24px 90px rgba(0,0,0,.45);
            }}
            h1 {{ margin: 0 0 10px; font-size: 28px; }}
            p {{ color: rgba(255,255,255,.72); line-height: 1.6; }}
            code {{ color: #c4b5fd; }}
        </style>
    </head>
    <body>
        <main class="card">
            <h1>Bot is online</h1>
            <p><strong>Status:</strong> {payload['status']}</p>
            <p><strong>Bot:</strong> {payload['bot']}</p>
            <p><strong>Servers:</strong> {payload['guilds']}</p>
            <p><strong>Latency:</strong> {payload['latency_ms']}ms</p>
            <p>Use <code>/health</code> for JSON health checks.</p>
        </main>
    </body>
    </html>
    """
    return web.Response(text=html, content_type="text/html")


async def keep_alive_health(request: web.Request) -> web.Response:
    return web.json_response(bot_status_payload())


async def start_keep_alive_server() -> None:
    global web_runner
    if web_runner is not None:
        return

    app = web.Application()
    app.router.add_get("/", keep_alive_home)
    app.router.add_get("/health", keep_alive_health)

    web_runner = web.AppRunner(app)
    await web_runner.setup()
    site = web.TCPSite(web_runner, WEB_HOST, WEB_PORT)
    await site.start()
    print(f"Keep-alive website running on http://{WEB_HOST}:{WEB_PORT}")


async def stop_keep_alive_server() -> None:
    global web_runner
    if web_runner is not None:
        await web_runner.cleanup()
        web_runner = None

# =========================
# EVENTS
# =========================
@bot.event
async def on_ready():
    bot.add_view(VerifyView())
    bot.add_view(TicketPanelView())
    bot.add_view(CloseTicketView())
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash commands.")
    except Exception as e:
        print(f"Slash command sync failed: {e}")

    if not rotate_status.is_running():
        rotate_status.start()
    if not update_stats.is_running():
        update_stats.start()
    if not self_ping.is_running():
        self_ping.start()

    print(f"Logged in as {bot.user}")


@bot.event
async def on_member_join(member: discord.Member):
    config = get_guild_config(member.guild.id)
    await safe_add_role(member, config.get("auto_role"), "Auto role on join")

    channel = member.guild.get_channel(config.get("welcome_channel") or 0)
    if isinstance(channel, discord.TextChannel):
        embed = make_embed(
            "Welcome",
            f"Welcome {member.mention} to **{member.guild.name}**. Please verify if required and open a ticket if you need support.",
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        await channel.send(embed=embed)

# =========================
# BACKGROUND TASKS
# =========================
@tasks.loop(seconds=45)
async def rotate_status():
    if not ROTATING_STATUSES:
        return
    status = ROTATING_STATUSES[rotate_status.current_loop % len(ROTATING_STATUSES)]
    if status.lower().startswith("watching "):
        text = status[9:]
        activity = discord.Activity(type=discord.ActivityType.watching, name=text)
    else:
        activity = discord.Game(name=status)
    await bot.change_presence(status=discord.Status.online, activity=activity)


@tasks.loop(minutes=10)
async def update_stats():
    for guild in bot.guilds:
        config = get_guild_config(guild.id)
        channels = config.get("stats_channels", {})
        if not channels:
            continue

        humans = len([m for m in guild.members if not m.bot])
        bots = len([m for m in guild.members if m.bot])
        members = guild.member_count or len(guild.members)
        boosts = guild.premium_subscription_count or 0

        stats = {
            "members": f"Members: {members}",
            "humans": f"Humans: {humans}",
            "bots": f"Bots: {bots}",
            "boosts": f"Boosts: {boosts}",
        }

        for key, name in stats.items():
            channel_id = channels.get(key)
            channel = guild.get_channel(channel_id or 0)
            if isinstance(channel, discord.VoiceChannel) and channel.name != name:
                try:
                    await channel.edit(name=name, reason="Live server stats update")
                except discord.HTTPException:
                    pass



@tasks.loop(minutes=1)
async def self_ping():
    # Pings the local keep-alive server every minute.
    # If KEEP_ALIVE_URL is set in .env, it will ping that instead.
    url = KEEP_ALIVE_URL or f"http://127.0.0.1:{WEB_PORT}/health"
    try:
        async with ClientSession() as session:
            async with session.get(url, timeout=15) as response:
                await response.text()
                if response.status >= 400:
                    print(f"Self-ping got HTTP {response.status} from {url}")
    except (ClientError, asyncio.TimeoutError) as e:
        print(f"Self-ping failed for {url}: {e}")

# =========================
# BASIC COMMANDS
# =========================
@bot.tree.command(name="ping", description="Check the bot latency.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(embed=make_embed("Pong", f"Latency: `{latency}ms`"), ephemeral=True)


@bot.tree.command(name="store", description="Get the store link.")
async def store(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message(DEFAULT_STORE_URL, ephemeral=True)
    config = get_guild_config(interaction.guild.id)
    await interaction.response.send_message(embed=make_embed("Store", f"Visit the store here:\n{config.get('store_url', DEFAULT_STORE_URL)}"))


@bot.tree.command(name="help", description="Show public bot commands.")
async def help_command(interaction: discord.Interaction):
    embed = make_embed(
        "Help",
        "Here are the public commands available in this server.",
    )
    embed.add_field(
        name="Public Commands",
        value=(
            "`/ping` - Check the bot latency\n"
            "`/store` - Get the store link\n"
            "`/help` - Show this help menu"
        ),
        inline=False,
    )
    embed.set_footer(text="moealturej support")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="commands", description="Show admin bot commands.")
@admin_only()
async def commands_menu(interaction: discord.Interaction):
    embed = make_embed(
        "Admin Commands",
        "Private setup and management commands for this server.",
    )
    embed.add_field(
        name="Owner",
        value="`/set_admin_role` - Set which role can use admin bot commands",
        inline=False,
    )
    embed.add_field(
        name="Verification / Welcome",
        value=(
            "`/set_verified_role` - Set the verified role\n"
            "`/set_auto_role` - Set the join autorole\n"
            "`/set_welcome_channel` - Set the welcome channel\n"
            "`/send_verification_panel` - Send the captcha panel"
        ),
        inline=False,
    )
    embed.add_field(
        name="Tickets",
        value=(
            "`/set_ticket_category` - Set the ticket category\n"
            "`/set_ticket_role` - Set support roles per ticket type\n"
            "`/send_ticket_panel` - Send the ticket panel"
        ),
        inline=False,
    )
    embed.add_field(
        name="Announcements / Store / Stats",
        value=(
            "`/set_store` - Set the store URL\n"
            "`/set_announce_assets` - Set default announcement assets\n"
            "`/announce` - Send an announcement embed\n"
            "`/stats_setup` - Create/connect live stats channels\n"
            "`/config_show` - Show saved server config"
        ),
        inline=False,
    )
    embed.set_footer(text="Only configured bot admins can see/use this menu")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# =========================
# SETUP COMMANDS
# =========================
@bot.tree.command(name="set_admin_role", description="Owner only: set the role allowed to use admin bot commands.")
@owner_only()
async def set_admin_role(interaction: discord.Interaction, role: discord.Role):
    update_guild_config(interaction.guild.id, "bot_admin_role", role.id)
    await interaction.response.send_message(f"Bot admin role set to {role.mention}. Members with this role can use `/commands` and setup commands.", ephemeral=True)


@bot.tree.command(name="set_verified_role", description="Set the role given after captcha verification.")
@admin_only()
async def set_verified_role(interaction: discord.Interaction, role: discord.Role):
    update_guild_config(interaction.guild.id, "verified_role", role.id)
    await interaction.response.send_message(f"Verified role set to {role.mention}.", ephemeral=True)


@bot.tree.command(name="set_auto_role", description="Set the role automatically given when a member joins.")
@admin_only()
async def set_auto_role(interaction: discord.Interaction, role: discord.Role):
    update_guild_config(interaction.guild.id, "auto_role", role.id)
    await interaction.response.send_message(f"Auto role set to {role.mention}.", ephemeral=True)


@bot.tree.command(name="set_welcome_channel", description="Set the welcome message channel.")
@admin_only()
async def set_welcome_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    update_guild_config(interaction.guild.id, "welcome_channel", channel.id)
    await interaction.response.send_message(f"Welcome channel set to {channel.mention}.", ephemeral=True)


@bot.tree.command(name="send_verification_panel", description="Send the verification captcha panel.")
@admin_only()
async def send_verification_panel(interaction: discord.Interaction, channel: discord.TextChannel):
    update_guild_config(interaction.guild.id, "verification_channel", channel.id)
    embed = make_embed(
        "Verify Access",
        "Click the button below and copy the captcha text exactly to receive your verified role.",
    )
    embed.set_footer(text="moealturej verification")
    await channel.send(embed=embed, view=VerifyView())
    await interaction.response.send_message(f"Verification panel sent in {channel.mention}.", ephemeral=True)


@bot.tree.command(name="set_ticket_category", description="Set the category where tickets will be created.")
@admin_only()
async def set_ticket_category(interaction: discord.Interaction, category: discord.CategoryChannel):
    update_guild_config(interaction.guild.id, "ticket_category", category.id)
    await interaction.response.send_message(f"Ticket category set to **{category.name}**.", ephemeral=True)


@bot.tree.command(name="set_ticket_role", description="Set the support role for a ticket type.")
@app_commands.choices(ticket_type=[
    app_commands.Choice(name="General support", value="general"),
    app_commands.Choice(name="Key HWID reset", value="hwid"),
    app_commands.Choice(name="Key not received", value="key_not_received"),
])
@admin_only()
async def set_ticket_role(interaction: discord.Interaction, ticket_type: app_commands.Choice[str], role: discord.Role):
    key = TICKET_TYPES[ticket_type.value]["support_role_key"]
    update_guild_config(interaction.guild.id, key, role.id)
    await interaction.response.send_message(f"{ticket_type.name} support role set to {role.mention}.", ephemeral=True)


@bot.tree.command(name="send_ticket_panel", description="Send the ticket creation panel.")
@admin_only()
async def send_ticket_panel(interaction: discord.Interaction, channel: discord.TextChannel):
    update_guild_config(interaction.guild.id, "ticket_panel_channel", channel.id)
    embed = make_embed(
        "Support Tickets",
        "Choose the ticket type that matches your issue. A private support channel will be created for you.",
    )
    embed.add_field(name="Options", value="💬 General support\n🔑 Key HWID reset\n📦 Key not received", inline=False)
    embed.set_footer(text="moealturej support")
    await channel.send(embed=embed, view=TicketPanelView())
    await interaction.response.send_message(f"Ticket panel sent in {channel.mention}.", ephemeral=True)


@bot.tree.command(name="set_store", description="Set the store URL used by /store.")
@admin_only()
async def set_store(interaction: discord.Interaction, url: str):
    update_guild_config(interaction.guild.id, "store_url", url)
    await interaction.response.send_message(f"Store URL set to: {url}", ephemeral=True)


@bot.tree.command(name="set_announce_assets", description="Set default announcement image and footer.")
@admin_only()
async def set_announce_assets(interaction: discord.Interaction, footer: Optional[str] = None, image_url: Optional[str] = None):
    if footer is not None:
        update_guild_config(interaction.guild.id, "announce_footer", footer)
    if image_url is not None:
        update_guild_config(interaction.guild.id, "announce_image", image_url)
    await interaction.response.send_message("Announcement assets updated.", ephemeral=True)


@bot.tree.command(name="announce", description="Send a clean webhook-style announcement embed.")
@admin_only()
async def announce(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    title: str,
    message: str,
    image_url: Optional[str] = None,
    footer: Optional[str] = None,
):
    config = get_guild_config(interaction.guild.id)
    embed = make_embed(title, message)
    final_image = image_url or config.get("announce_image")
    final_footer = footer or config.get("announce_footer") or "moealturej"
    if final_image:
        embed.set_image(url=final_image)
    embed.set_footer(text=final_footer)
    await channel.send(embed=embed)
    await interaction.response.send_message(f"Announcement sent in {channel.mention}.", ephemeral=True)


@bot.tree.command(name="stats_setup", description="Create or connect live server stats voice channels.")
@admin_only()
async def stats_setup(interaction: discord.Interaction, category: Optional[discord.CategoryChannel] = None):
    guild = interaction.guild
    if category is None:
        category = await guild.create_category("Server Stats", reason="Live server stats setup")

    update_guild_config(guild.id, "stats_category", category.id)

    config = get_guild_config(guild.id)
    current = config.get("stats_channels", {})
    created_or_found = {}

    base_overwrites = {
        guild.default_role: discord.PermissionOverwrite(connect=False, view_channel=True),
        guild.me: discord.PermissionOverwrite(connect=True, manage_channels=True, view_channel=True),
    }

    names = {
        "members": "Members: 0",
        "humans": "Humans: 0",
        "bots": "Bots: 0",
        "boosts": "Boosts: 0",
    }

    for key, default_name in names.items():
        channel = guild.get_channel(current.get(key) or 0)
        if not isinstance(channel, discord.VoiceChannel):
            channel = await guild.create_voice_channel(default_name, category=category, overwrites=base_overwrites, reason="Live stats channel created")
        created_or_found[key] = channel.id

    update_guild_config(guild.id, "stats_channels", created_or_found)
    await update_stats()
    await interaction.response.send_message(f"Live stats channels are set in **{category.name}**.", ephemeral=True)


@bot.tree.command(name="config_show", description="Show this server's bot configuration.")
@admin_only()
async def config_show(interaction: discord.Interaction):
    config = get_guild_config(interaction.guild.id)

    def role_text(role_id):
        role = interaction.guild.get_role(role_id or 0)
        return role.mention if role else "Not set"

    def channel_text(channel_id):
        channel = interaction.guild.get_channel(channel_id or 0)
        return channel.mention if channel else "Not set"

    embed = make_embed("Server Config", "Current saved settings for this server.")
    embed.add_field(name="Bot Admin Role", value=role_text(config.get("bot_admin_role")), inline=True)
    embed.add_field(name="Verified Role", value=role_text(config.get("verified_role")), inline=True)
    embed.add_field(name="Auto Role", value=role_text(config.get("auto_role")), inline=True)
    embed.add_field(name="Welcome Channel", value=channel_text(config.get("welcome_channel")), inline=True)
    embed.add_field(name="Ticket Category", value=channel_text(config.get("ticket_category")), inline=True)
    embed.add_field(name="General Support Role", value=role_text(config.get("ticket_role_general")), inline=True)
    embed.add_field(name="HWID Reset Role", value=role_text(config.get("ticket_role_hwid")), inline=True)
    embed.add_field(name="Key Not Received Role", value=role_text(config.get("ticket_role_key_not_received")), inline=True)
    embed.add_field(name="Store URL", value=config.get("store_url", DEFAULT_STORE_URL), inline=False)
    embed.add_field(name="Announcement Footer", value=config.get("announce_footer") or "Not set", inline=True)
    embed.add_field(name="Announcement Image", value=config.get("announce_image") or "Not set", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

# =========================
# START BOT
# =========================
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN. Add it to your .env file before starting the bot.")
    try:
        bot.run(BOT_TOKEN)
    finally:
        # discord.py closes the event loop after bot.run(), so normal cleanup may not run here.
        # The web server is tied to the bot process and will stop when the process exits.
        pass