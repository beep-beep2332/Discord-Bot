import asyncio
import base64
import hashlib
import hmac
import html
import json
import os
import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import discord
from aiohttp import ClientError, ClientSession, web
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

# =========================
# ENV CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/moealturej_bot").strip()
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "moealturej_bot").strip()

DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID", "").strip()
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost:8080").rstrip("/")
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", secrets.token_urlsafe(48)).strip()
OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "1222903158125105194"))
OWNER_CONTACT = os.getenv("OWNER_CONTACT", "Contact moealturej, the owner, to talk about using this bot for your server.").strip()

DEFAULT_STORE_URL = os.getenv("DEFAULT_STORE_URL", "https://www.moealturej.com").strip()
ROTATING_STATUSES = [
    s.strip() for s in os.getenv("ROTATING_STATUSES", "Watching /help,moealturej support,Watching tickets").split(",") if s.strip()
]
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0").strip()
WEB_PORT = int(os.getenv("PORT", os.getenv("WEB_PORT", "8080")))
KEEP_ALIVE_URL = os.getenv("KEEP_ALIVE_URL", "").strip()

EMBED_COLOR = 0x7C3AED
ERROR_COLOR = 0xEF4444
SUCCESS_COLOR = 0x22C55E
INFO_COLOR = 0x38BDF8
STARTED_AT = datetime.now(timezone.utc)

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

DEFAULT_GUILD_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "verified_role": None,
    "unverified_role": None,
    "auto_role": None,
    "bot_admin_role": None,
    "welcome_channel": None,
    "verification_channel": None,
    "verification_log_channel": None,
    "ticket_category": None,
    "ticket_panel_channel": None,
    "ticket_log_channel": None,
    "ticket_role_general": None,
    "ticket_role_hwid": None,
    "ticket_role_key_not_received": None,
    "store_url": DEFAULT_STORE_URL,
    "announce_image": None,
    "announce_footer": "moealturej",
    "stats_category": None,
    "stats_channels": {"members": None, "humans": None, "bots": None, "boosts": None},
    "open_tickets": {},
    "oauth_verify_join_enabled": True,
}

# =========================
# DISCORD / DB BOOT
# =========================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True  # Needed only to build ticket transcripts.

bot = commands.Bot(command_prefix="!", intents=intents)
web_runner: Optional[web.AppRunner] = None
mongo_client: Optional[AsyncIOMotorClient] = None
mdb = None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utcnow().isoformat()


async def init_mongo() -> None:
    global mongo_client, mdb
    mongo_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    mdb = mongo_client[MONGO_DB_NAME]
    await mdb.command("ping")
    await mdb.guild_configs.create_index("guild_id", unique=True)
    await mdb.oauth_states.create_index("expires_at", expireAfterSeconds=0)
    await mdb.sessions.create_index("expires_at", expireAfterSeconds=0)
    await mdb.ticket_events.create_index([("guild_id", 1), ("created_at", -1)])
    await mdb.verification_events.create_index([("guild_id", 1), ("created_at", -1)])


async def get_guild_config(guild_id: int) -> Dict[str, Any]:
    existing = await mdb.guild_configs.find_one({"guild_id": int(guild_id)}, {"_id": 0})
    if not existing:
        doc = {"guild_id": int(guild_id), **DEFAULT_GUILD_CONFIG, "created_at": now_iso(), "updated_at": now_iso()}
        await mdb.guild_configs.insert_one(doc)
        return {k: v for k, v in doc.items() if k != "_id"}

    update: Dict[str, Any] = {}
    for key, value in DEFAULT_GUILD_CONFIG.items():
        if key not in existing:
            update[key] = value
    for key, value in DEFAULT_GUILD_CONFIG["stats_channels"].items():
        if key not in existing.get("stats_channels", {}):
            update[f"stats_channels.{key}"] = value
    if update:
        update["updated_at"] = now_iso()
        await mdb.guild_configs.update_one({"guild_id": int(guild_id)}, {"$set": update})
        existing = await mdb.guild_configs.find_one({"guild_id": int(guild_id)}, {"_id": 0})
    return existing


async def set_config(guild_id: int, updates: Dict[str, Any]) -> None:
    await get_guild_config(guild_id)
    updates["updated_at"] = now_iso()
    await mdb.guild_configs.update_one({"guild_id": int(guild_id)}, {"$set": updates}, upsert=True)


async def add_open_ticket(guild_id: int, user_id: int, channel_id: int, ticket_type: str) -> None:
    await set_config(guild_id, {f"open_tickets.{user_id}": {"channel_id": int(channel_id), "type": ticket_type, "opened_at": now_iso()}})


async def remove_open_ticket(guild_id: int, user_id: int) -> None:
    await mdb.guild_configs.update_one({"guild_id": int(guild_id)}, {"$unset": {f"open_tickets.{user_id}": ""}, "$set": {"updated_at": now_iso()}})


async def save_event(collection: str, payload: Dict[str, Any]) -> None:
    payload.setdefault("created_at", now_iso())
    await mdb[collection].insert_one(payload)

# =========================
# AUTH / ACCESS HELPERS
# =========================
def sign_value(value: str) -> str:
    sig = hmac.new(DASHBOARD_SECRET.encode(), value.encode(), hashlib.sha256).hexdigest()
    return f"{value}.{sig}"


def unsign_value(signed: str) -> Optional[str]:
    try:
        value, sig = signed.rsplit(".", 1)
    except ValueError:
        return None
    expected = hmac.new(DASHBOARD_SECRET.encode(), value.encode(), hashlib.sha256).hexdigest()
    return value if hmac.compare_digest(sig, expected) else None


def is_owner_user(user_id: int) -> bool:
    return int(user_id) == OWNER_USER_ID


async def get_dashboard_user(request: web.Request) -> Optional[Dict[str, Any]]:
    raw = request.cookies.get("moe_session")
    if not raw:
        return None
    session_id = unsign_value(raw)
    if not session_id:
        return None
    session = await mdb.sessions.find_one({"session_id": session_id, "expires_at": {"$gt": utcnow()}}, {"_id": 0})
    return session


async def exchange_code(code: str, redirect_uri: str) -> Dict[str, Any]:
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    async with ClientSession() as session:
        async with session.post("https://discord.com/api/oauth2/token", data=data, headers=headers) as resp:
            body = await resp.json(content_type=None)
            if resp.status >= 400:
                raise web.HTTPBadRequest(text=f"Discord OAuth failed: {body}")
            return body


async def discord_get(path: str, token: str) -> Any:
    async with ClientSession() as session:
        async with session.get(f"https://discord.com/api{path}", headers={"Authorization": f"Bearer {token}"}) as resp:
            body = await resp.json(content_type=None)
            if resp.status >= 400:
                raise web.HTTPBadRequest(text=f"Discord API failed: {body}")
            return body


async def discord_put(path: str, token: str, payload: Dict[str, Any]) -> tuple[int, Any]:
    async with ClientSession() as session:
        async with session.put(f"https://discord.com/api{path}", headers={"Authorization": f"Bot {token}", "Content-Type": "application/json"}, json=payload) as resp:
            try:
                body = await resp.json(content_type=None)
            except Exception:
                body = await resp.text()
            return resp.status, body


def guild_manageable(user_guild: Dict[str, Any]) -> bool:
    permissions = int(user_guild.get("permissions", "0"))
    manage_guild = bool(permissions & 0x20)
    administrator = bool(permissions & 0x8)
    return manage_guild or administrator or bool(user_guild.get("owner"))


async def dashboard_can_access(user: Dict[str, Any], guild_id: int) -> bool:
    if is_owner_user(int(user["user_id"])):
        return True
    return False  # Private-use bot. Everyone except owner gets the contact page.


def member_is_command_admin(member: discord.Member, config: Dict[str, Any]) -> bool:
    if is_owner_user(member.id):
        return True
    if member.id == member.guild.owner_id:
        return True
    admin_role_id = config.get("bot_admin_role")
    if admin_role_id and any(role.id == int(admin_role_id) for role in member.roles):
        return True
    return admin_role_id is None and member.guild_permissions.manage_guild


def owner_private_message() -> str:
    return f"This bot is not for public use. {OWNER_CONTACT}"


def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command only works in a server.", ephemeral=True)
            return False
        config = await get_guild_config(interaction.guild.id)
        if not member_is_command_admin(interaction.user, config):
            await interaction.response.send_message(owner_private_message(), ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)


def guild_enabled_or_owner():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return True
        if is_owner_user(interaction.user.id):
            return True
        config = await get_guild_config(interaction.guild.id)
        if config.get("enabled"):
            return True
        await interaction.response.send_message(owner_private_message(), ephemeral=True)
        return False
    return app_commands.check(predicate)

# =========================
# UI HELPERS
# =========================
def make_embed(title: str, description: str, color: int = EMBED_COLOR) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color, timestamp=utcnow())


def clean_channel_name(text: str) -> str:
    allowed = string.ascii_lowercase + string.digits + "-"
    text = text.lower().replace(" ", "-")
    return "".join(c for c in text if c in allowed)[:80] or "ticket"


async def safe_add_role(member: discord.Member, role_id: Optional[int], reason: str) -> bool:
    if not role_id:
        return False
    role = member.guild.get_role(int(role_id))
    if not role:
        return False
    if role in member.roles:
        return True
    try:
        await member.add_roles(role, reason=reason)
        return True
    except discord.Forbidden:
        return False


async def safe_remove_role(member: discord.Member, role_id: Optional[int], reason: str) -> bool:
    if not role_id:
        return False
    role = member.guild.get_role(int(role_id))
    if not role or role not in member.roles:
        return False
    try:
        await member.remove_roles(role, reason=reason)
        return True
    except discord.Forbidden:
        return False


async def send_verified_dm(member: discord.Member, store_url: str) -> None:
    embed = make_embed(
        "Verified successfully",
        f"You are now verified in **{member.guild.name}**. You can access the server and open a ticket anytime you need help.",
        SUCCESS_COLOR,
    )
    embed.add_field(name="Store", value=store_url, inline=False)
    embed.set_thumbnail(url=member.guild.icon.url if member.guild.icon else member.display_avatar.url)
    embed.set_footer(text="moealturej verification")
    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        pass


async def log_verification(guild: discord.Guild, user: discord.abc.User, method: str, status: str, details: str = "") -> None:
    config = await get_guild_config(guild.id)
    await save_event("verification_events", {
        "guild_id": guild.id,
        "user_id": user.id,
        "username": str(user),
        "method": method,
        "status": status,
        "details": details,
    })
    channel = guild.get_channel(config.get("verification_log_channel") or 0)
    if isinstance(channel, discord.TextChannel):
        embed = make_embed("Verification Log", f"**User:** {user.mention if hasattr(user, 'mention') else user}\n**Method:** {method}\n**Status:** {status}\n{details}", SUCCESS_COLOR if status == "success" else ERROR_COLOR)
        await channel.send(embed=embed)


async def build_ticket_transcript(channel: discord.TextChannel) -> tuple[str, bytes]:
    lines = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<style>body{font-family:Arial;background:#0b0b10;color:#fff;padding:24px}.msg{border-bottom:1px solid #292938;padding:12px 0}.meta{color:#a8a8b8;font-size:13px}.content{white-space:pre-wrap;margin-top:6px}.att a{color:#c4b5fd}</style>",
        f"<title>Transcript #{html.escape(channel.name)}</title></head><body>",
        f"<h1>Transcript: #{html.escape(channel.name)}</h1>",
    ]
    async for msg in channel.history(limit=None, oldest_first=True):
        author = html.escape(str(msg.author))
        content = html.escape(msg.content or "")
        created = msg.created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines.append("<div class='msg'>")
        lines.append(f"<div class='meta'><strong>{author}</strong> • {created}</div>")
        if content:
            lines.append(f"<div class='content'>{content}</div>")
        if msg.embeds:
            for emb in msg.embeds:
                title = html.escape(emb.title or "Embed")
                desc = html.escape(emb.description or "")
                lines.append(f"<div class='content'>[Embed] <strong>{title}</strong><br>{desc}</div>")
        if msg.attachments:
            links = " ".join(f"<a href='{html.escape(a.url)}'>{html.escape(a.filename)}</a>" for a in msg.attachments)
            lines.append(f"<div class='att'>Attachments: {links}</div>")
        lines.append("</div>")
    lines.append("</body></html>")
    filename = f"transcript-{channel.guild.id}-{channel.id}.html"
    return filename, "\n".join(lines).encode("utf-8")

# =========================
# VERIFICATION VIEWS
# =========================
class OAuthVerifyView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)

    @discord.ui.button(label="Verify with Discord", style=discord.ButtonStyle.success, emoji="✅", custom_id="moe_oauth_verify")
    async def verify_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("This verification button only works inside the server.", ephemeral=True)

        config = await get_guild_config(interaction.guild.id)
        verified_role_id = config.get("verified_role")
        verified_role = interaction.guild.get_role(int(verified_role_id or 0)) if verified_role_id else None
        if verified_role and verified_role in interaction.user.roles:
            removed = await safe_remove_role(interaction.user, config.get("unverified_role"), "Already verified cleanup")
            extra = " I also removed your unverified role." if removed else ""
            await log_verification(interaction.guild, interaction.user, "panel-check", "already_verified", "User clicked verify but already had verified role." + extra)
            return await interaction.response.send_message(f"✅ You are already verified in **{interaction.guild.name}**.{extra}", ephemeral=True)

        url = f"{PUBLIC_BASE_URL}/verify/start?guild_id={interaction.guild.id}&user_id={interaction.user.id}"
        view = discord.ui.View(timeout=180)
        view.add_item(discord.ui.Button(label="Open secure verification", style=discord.ButtonStyle.link, emoji="🔐", url=url))
        await interaction.response.send_message("Click the secure OAuth2 link below to verify your Discord account.", view=view, ephemeral=True)


class TicketSelect(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=i["label"], description=i["description"], emoji=i["emoji"], value=k) for k, i in TICKET_TYPES.items()]
        super().__init__(placeholder="Choose a ticket type...", options=options, custom_id="moe_ticket_select")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("This only works inside a server.", ephemeral=True)
        config = await get_guild_config(interaction.guild.id)
        existing = config.get("open_tickets", {}).get(str(interaction.user.id))
        if existing:
            channel_id = existing.get("channel_id") if isinstance(existing, dict) else existing
            channel = interaction.guild.get_channel(int(channel_id or 0))
            if channel:
                return await interaction.response.send_message(f"You already have an open ticket: {channel.mention}", ephemeral=True)
            await remove_open_ticket(interaction.guild.id, interaction.user.id)

        ticket_key = self.values[0]
        ticket_info = TICKET_TYPES[ticket_key]
        category = interaction.guild.get_channel(config.get("ticket_category") or 0)
        if not isinstance(category, discord.CategoryChannel):
            return await interaction.response.send_message("Ticket category is not configured yet.", ephemeral=True)

        support_role = interaction.guild.get_role(config.get(ticket_info["support_role_key"]) or 0)
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True, read_message_history=True),
            interaction.guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
        }
        if support_role:
            overwrites[support_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True, read_message_history=True)

        channel = await interaction.guild.create_text_channel(
            name=clean_channel_name(f"ticket-{interaction.user.name}-{ticket_key}"),
            category=category,
            overwrites=overwrites,
            topic=f"owner_id={interaction.user.id} ticket_type={ticket_key}",
            reason=f"Ticket opened by {interaction.user}",
        )
        await add_open_ticket(interaction.guild.id, interaction.user.id, channel.id, ticket_key)
        await save_event("ticket_events", {"guild_id": interaction.guild.id, "user_id": interaction.user.id, "channel_id": channel.id, "event": "opened", "ticket_type": ticket_key})

        embed = make_embed(f"{ticket_info['emoji']} {ticket_info['label']}", f"Welcome {interaction.user.mention}. {support_role.mention if support_role else 'Support'} will help you here. Use the button below when finished.")
        await channel.send(content=f"{interaction.user.mention} {support_role.mention if support_role else ''}", embed=embed, view=CloseTicketView())
        await interaction.response.send_message(f"Ticket created: {channel.mention}", ephemeral=True)


class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketSelect())


class CloseTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, emoji="🔒", custom_id="moe_close_ticket")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.channel, discord.TextChannel):
            return await interaction.response.send_message("This can only be used inside a ticket channel.", ephemeral=True)
        topic = interaction.channel.topic or ""
        owner_id = None
        ticket_type = "unknown"
        for part in topic.split():
            if part.startswith("owner_id="):
                try: owner_id = int(part.split("=", 1)[1])
                except ValueError: pass
            if part.startswith("ticket_type="):
                ticket_type = part.split("=", 1)[1]

        config = await get_guild_config(interaction.guild.id)
        allowed = interaction.user.guild_permissions.manage_channels or (owner_id == interaction.user.id) or (isinstance(interaction.user, discord.Member) and member_is_command_admin(interaction.user, config))
        if not allowed:
            for info in TICKET_TYPES.values():
                role_id = config.get(info["support_role_key"])
                if role_id and any(role.id == int(role_id) for role in getattr(interaction.user, "roles", [])):
                    allowed = True
        if not allowed:
            return await interaction.response.send_message("You do not have permission to close this ticket.", ephemeral=True)

        await interaction.response.send_message("Saving transcript and closing ticket...", ephemeral=True)
        filename, transcript = await build_ticket_transcript(interaction.channel)
        import io

        owner = interaction.guild.get_member(owner_id or 0)
        close_embed = make_embed("Ticket Closed", f"Ticket `{interaction.channel.name}` was closed by {interaction.user.mention}.", INFO_COLOR)
        close_embed.add_field(name="Type", value=ticket_type, inline=True)
        if owner:
            try:
                await owner.send(embed=close_embed, file=discord.File(io.BytesIO(transcript), filename=filename))
            except discord.Forbidden:
                pass

        log_channel = interaction.guild.get_channel(config.get("ticket_log_channel") or 0)
        if isinstance(log_channel, discord.TextChannel):
            await log_channel.send(embed=close_embed, file=discord.File(io.BytesIO(transcript), filename=filename))

        await save_event("ticket_events", {"guild_id": interaction.guild.id, "user_id": owner_id, "channel_id": interaction.channel.id, "event": "closed", "ticket_type": ticket_type, "closed_by": interaction.user.id})
        if owner_id:
            await remove_open_ticket(interaction.guild.id, owner_id)
        await asyncio.sleep(2)
        await interaction.channel.delete(reason=f"Ticket closed by {interaction.user}")

# =========================
# WEB DASHBOARD
# =========================
def page(title: str, body: str) -> web.Response:
    css = """
    <style>
    :root{color-scheme:dark;--bg:#030306;--bg2:#070711;--glass:rgba(12,12,22,.74);--glass2:rgba(255,255,255,.055);--panel:rgba(14,14,25,.82);--panel2:rgba(124,58,237,.14);--line:rgba(255,255,255,.12);--line2:rgba(192,132,252,.35);--text:#f8f7ff;--muted:rgba(248,247,255,.66);--soft:rgba(248,247,255,.84);--purple:#8b5cf6;--purple2:#c084fc;--pink:#ec4899;--blue:#38bdf8;--green:#22c55e;--danger:#fb7185;--shadow:0 30px 110px rgba(0,0,0,.42)}
    *{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;min-height:100vh;background:radial-gradient(circle at 18% -10%,rgba(139,92,246,.38),transparent 34rem),radial-gradient(circle at 92% 12%,rgba(236,72,153,.18),transparent 30rem),radial-gradient(circle at 55% 96%,rgba(56,189,248,.12),transparent 32rem),linear-gradient(180deg,#05050a,#020204 68%,#05050a);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;overflow-x:hidden}body:before{content:"";position:fixed;inset:0;pointer-events:none;background-image:linear-gradient(rgba(255,255,255,.045) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.045) 1px,transparent 1px);background-size:72px 72px;mask-image:linear-gradient(to bottom,rgba(0,0,0,.9),transparent 82%);opacity:.55}body:after{content:"";position:fixed;inset:0;pointer-events:none;background:radial-gradient(circle at 50% 0,rgba(255,255,255,.08),transparent 38rem);mix-blend-mode:screen}a{color:#e9d5ff;text-decoration:none}.wrap{width:min(1220px,calc(100% - 30px));margin:auto;padding:28px 0 58px}.nav{position:sticky;top:14px;z-index:10;display:flex;justify-content:space-between;align-items:center;margin-bottom:28px;padding:12px 14px;border:1px solid var(--line);border-radius:24px;background:linear-gradient(135deg,rgba(8,8,15,.82),rgba(20,15,34,.68));backdrop-filter:blur(22px);box-shadow:0 22px 90px rgba(0,0,0,.36)}.brand{display:flex;align-items:center;gap:11px;font-weight:950;letter-spacing:-.05em}.brand:before{content:"✦";display:grid;place-items:center;width:38px;height:38px;border-radius:14px;background:linear-gradient(135deg,var(--purple),var(--pink) 55%,var(--blue));box-shadow:0 14px 50px rgba(139,92,246,.46)}.navlinks{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.navlinks a{padding:9px 12px;border-radius:14px;color:rgba(255,255,255,.74);font-weight:800;font-size:14px}.navlinks a:hover{background:rgba(255,255,255,.08);color:#fff}.hero{position:relative;overflow:hidden;border:1px solid var(--line);border-radius:34px;padding:38px;background:linear-gradient(145deg,rgba(139,92,246,.24),rgba(236,72,153,.08) 38%,rgba(56,189,248,.07) 62%,rgba(255,255,255,.04));box-shadow:var(--shadow)}.hero:before{content:"";position:absolute;inset:1px;border-radius:33px;border:1px solid rgba(255,255,255,.06);pointer-events:none}.hero:after{content:"";position:absolute;right:-100px;top:-120px;width:340px;height:340px;background:radial-gradient(circle,rgba(192,132,252,.38),transparent 68%);filter:blur(2px)}h1{font-size:clamp(34px,5.3vw,68px);letter-spacing:-.07em;line-height:.92;margin:0 0 13px;max-width:930px}h2{letter-spacing:-.04em;margin:0 0 12px;font-size:clamp(22px,2.4vw,31px)}h3{letter-spacing:-.03em;margin:0 0 10px;font-size:20px}.card,.guild,.panel{position:relative;border:1px solid var(--line);background:linear-gradient(145deg,var(--panel),rgba(255,255,255,.04));border-radius:26px;padding:23px;box-shadow:0 24px 90px rgba(0,0,0,.29);backdrop-filter:blur(20px);overflow:hidden}.card:before,.guild:before{content:"";position:absolute;inset:0;background:linear-gradient(135deg,rgba(255,255,255,.07),transparent 38%);pointer-events:none;opacity:.55}.guild{transition:transform .18s ease,border-color .18s ease,background .18s ease}.guild:hover{transform:translateY(-4px);border-color:var(--line2);background:linear-gradient(145deg,rgba(124,58,237,.2),rgba(255,255,255,.055))}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:16px}.section-title{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;margin:30px 0 13px}.btn,button{display:inline-flex;align-items:center;justify-content:center;gap:9px;border:0;border-radius:16px;background:linear-gradient(135deg,#7c3aed,#a855f7 55%,#ec4899);color:white;padding:12px 17px;font-weight:950;cursor:pointer;box-shadow:0 17px 46px rgba(124,58,237,.29);transition:transform .16s ease,filter .16s ease,box-shadow .16s ease}.btn:hover,button:hover{transform:translateY(-1px);filter:brightness(1.08);box-shadow:0 22px 58px rgba(124,58,237,.34)}.btn.secondary{background:rgba(255,255,255,.075);box-shadow:none;border:1px solid var(--line)}.muted{color:var(--muted);line-height:1.66}.pill{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;background:rgba(139,92,246,.14);color:#ede9fe;border:1px solid rgba(192,132,252,.28);font-size:13px;font-weight:900;box-shadow:inset 0 1px rgba(255,255,255,.08)}code{display:inline-block;max-width:100%;overflow:auto;padding:11px 13px;border-radius:15px;border:1px solid var(--line);background:rgba(0,0,0,.34);color:#ddd6fe}label{display:block;color:rgba(255,255,255,.84);font-size:13px;font-weight:900;letter-spacing:.01em}input,select,textarea{width:100%;margin:8px 0 16px;padding:14px 15px;border-radius:16px;border:1px solid rgba(255,255,255,.14);background:#10101b;color:#f8fafc;outline:none;box-shadow:inset 0 0 0 9999px rgba(255,255,255,.018);font:inherit}input::placeholder,textarea::placeholder{color:rgba(255,255,255,.36)}input:focus,select:focus,textarea:focus{border-color:rgba(192,132,252,.75);box-shadow:0 0 0 4px rgba(124,58,237,.18)}textarea{min-height:145px;resize:vertical;line-height:1.55}select{appearance:none;background-color:#10101b;background-image:linear-gradient(45deg,transparent 50%,#c4b5fd 50%),linear-gradient(135deg,#c4b5fd 50%,transparent 50%);background-position:calc(100% - 19px) 52%,calc(100% - 12px) 52%;background-size:7px 7px,7px 7px;background-repeat:no-repeat;padding-right:42px}select option{background:#0d0d18;color:#f8fafc}select option:hover,select option:checked{background:#7c3aed;color:#fff}.row{display:grid;grid-template-columns:1fr 1fr;gap:16px}.form-section{margin-top:17px;padding-top:17px;border-top:1px solid var(--line)}.savebar{position:sticky;bottom:14px;display:flex;justify-content:flex-end;margin-top:10px;padding:12px;border:1px solid var(--line);border-radius:22px;background:rgba(7,7,13,.8);backdrop-filter:blur(20px)}.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}.preview-shell{border:1px solid var(--line);border-radius:24px;background:linear-gradient(145deg,rgba(0,0,0,.28),rgba(255,255,255,.035));padding:16px}.preview-message{white-space:pre-wrap;color:#f8fafc;line-height:1.55;margin-bottom:12px;padding:13px 14px;border:1px solid rgba(255,255,255,.08);border-radius:16px;background:rgba(255,255,255,.045)}.preview-box{border:1px solid var(--line);border-left:4px solid var(--purple);border-radius:18px;background:rgba(0,0,0,.24);padding:18px;margin-top:8px}.preview-title{font-weight:950;font-size:20px;letter-spacing:-.025em}.preview-desc{white-space:pre-wrap;color:rgba(255,255,255,.78);line-height:1.55;margin-top:8px}.preview-footer{color:rgba(255,255,255,.48);font-size:12px;margin-top:14px}.preview-img{max-width:100%;border-radius:16px;margin-top:14px;border:1px solid var(--line)}.preview-thumb{float:right;width:88px;height:88px;object-fit:cover;border-radius:16px;margin-left:14px;margin-bottom:10px;border:1px solid var(--line)}.tiny{font-size:12px;color:rgba(255,255,255,.48)}@media(max-width:760px){.row{grid-template-columns:1fr}.nav{position:relative;top:0;align-items:flex-start;gap:12px;flex-direction:column}.hero{padding:25px}.grid{grid-template-columns:1fr}h1{font-size:39px}}
    </style>
    """
    html_doc = f"<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>{html.escape(title)}</title>{css}</head><body><main class='wrap'><nav class='nav'><div class='brand'>moealturej bot</div><div class='navlinks'><a href='/'>Dashboard</a><a href='/health'>Health</a><a href='/logout'>Logout</a></div></nav>{body}</main></body></html>"
    return web.Response(text=html_doc, content_type="text/html")

async def home(request: web.Request) -> web.Response:
    user = await get_dashboard_user(request)
    if not user:
        body = f"<section class='hero'><span class='pill'>🔒 Private control panel</span><h1>Private Discord bot dashboard</h1><p class='muted'>Login with Discord to manage approved servers, verification, tickets, transcripts, logs, and live stats.</p><a class='btn' href='/login'>Login with Discord</a><p class='muted'>{html.escape(OWNER_CONTACT)}</p></section>"
        return page("Dashboard", body)
    if not is_owner_user(int(user["user_id"])):
        return page("Not public", f"<section class='card'><h1>Not available publicly</h1><p class='muted'>{html.escape(owner_private_message())}</p></section>")

    guilds = user.get("guilds", [])
    cards = []
    bot_guild_ids = {g.id for g in bot.guilds}
    for g in guilds:
        if int(g["id"]) in bot_guild_ids and guild_manageable(g):
            icon = "🟢" if int(g["id"]) in bot_guild_ids else "⚪"
            cards.append(f"<div class='guild'><span class='pill'>{icon} Connected</span><h3>{html.escape(g['name'])}</h3><p class='muted'>Server ID: {g['id']}</p><a class='btn' href='/guild/{g['id']}'>Manage server</a></div>")
    body = f"<section class='hero'><span class='pill'>✅ Owner verified</span><h1>Welcome, {html.escape(user.get('username','owner'))}</h1><p class='muted'>Only Discord account ID <code>{OWNER_USER_ID}</code> can access full dashboard controls.</p></section><div class='section-title'><h2>Your servers</h2><span class='muted'>MongoDB synced</span></div><div class='grid'>{''.join(cards) or '<div class=card>No manageable bot servers found.</div>'}</div>"
    return page("Dashboard", body)


async def login(request: web.Request) -> web.Response:
    state = secrets.token_urlsafe(32)
    await mdb.oauth_states.insert_one({"state": state, "type": "dashboard", "expires_at": utcnow() + timedelta(minutes=10)})
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": f"{PUBLIC_BASE_URL}/oauth/callback",
        "response_type": "code",
        "scope": "identify guilds",
        "state": state,
        "prompt": "none",
    }
    raise web.HTTPFound(f"https://discord.com/oauth2/authorize?{urlencode(params)}")


async def oauth_callback(request: web.Request) -> web.Response:
    state = request.query.get("state", "")
    code = request.query.get("code", "")
    found = await mdb.oauth_states.find_one_and_delete({"state": state, "type": "dashboard", "expires_at": {"$gt": utcnow()}})
    if not found or not code:
        raise web.HTTPBadRequest(text="Invalid or expired OAuth state.")
    token = await exchange_code(code, f"{PUBLIC_BASE_URL}/oauth/callback")
    user = await discord_get("/users/@me", token["access_token"])
    guilds = await discord_get("/users/@me/guilds", token["access_token"])
    session_id = secrets.token_urlsafe(36)
    await mdb.sessions.insert_one({"session_id": session_id, "user_id": int(user["id"]), "username": user.get("username", "user"), "guilds": guilds, "expires_at": utcnow() + timedelta(days=7)})
    resp = web.HTTPFound("/")
    resp.set_cookie("moe_session", sign_value(session_id), max_age=604800, httponly=True, secure=PUBLIC_BASE_URL.startswith("https://"), samesite="Lax")
    raise resp


async def logout(request: web.Request) -> web.Response:
    raw = request.cookies.get("moe_session")
    sid = unsign_value(raw) if raw else None
    if sid:
        await mdb.sessions.delete_one({"session_id": sid})
    resp = web.HTTPFound("/")
    resp.del_cookie("moe_session")
    raise resp


async def guild_page(request: web.Request) -> web.Response:
    user = await get_dashboard_user(request)
    guild_id = int(request.match_info["guild_id"])
    if not user or not await dashboard_can_access(user, guild_id):
        return page("Not public", f"<section class='card'><h1>Not available publicly</h1><p class='muted'>{html.escape(owner_private_message())}</p></section>")
    guild = bot.get_guild(guild_id)
    if not guild:
        return page("Missing server", "<section class='card'><h1>Bot is not in this server</h1></section>")
    config = await get_guild_config(guild_id)

    def options(items, selected):
        out = ["<option value=''>Not set</option>"]
        for obj in items:
            sel = "selected" if selected and int(selected) == obj.id else ""
            out.append(f"<option value='{obj.id}' {sel}>{html.escape(obj.name)}</option>")
        return "".join(out)

    roles = [r for r in guild.roles if not r.is_default()]
    text_channels = guild.text_channels
    categories = guild.categories
    body = f"""
    <section class='hero'><span class='pill'>⚙️ Server controls</span><h1>{html.escape(guild.name)}</h1><p class='muted'>Manage verification, unverified role cleanup, tickets, transcripts, logs, stats, store links, and admin access from one clean MongoDB-backed panel.</p></section>
    <div class='section-title'><h2>Core settings</h2><span class='muted'>Saved per server</span></div>
    <form class='card' method='post'>
      <div class='row'><label>Bot availability<select name='enabled'><option value='true' {'selected' if config.get('enabled') else ''}>Enabled for this server</option><option value='false' {'selected' if not config.get('enabled') else ''}>Disabled / private only</option></select></label><label>Store URL<input name='store_url' value='{html.escape(config.get('store_url') or DEFAULT_STORE_URL)}' placeholder='https://your-store.com'></label></div>
      <div class='form-section'><h2>Verification</h2><p class='muted'>If a member already has the verified role, the bot now skips re-verifying and still removes the unverified role if configured.</p><div class='row'><label>Verified role<select name='verified_role'>{options(roles, config.get('verified_role'))}</select></label><label>Unverified role to remove<select name='unverified_role'>{options(roles, config.get('unverified_role'))}</select></label></div><div class='row'><label>Auto role on join<select name='auto_role'>{options(roles, config.get('auto_role'))}</select></label><label>Verification logs<select name='verification_log_channel'>{options(text_channels, config.get('verification_log_channel'))}</select></label></div><label>Verification panel channel<select name='verification_channel'>{options(text_channels, config.get('verification_channel'))}</select></label></div>
      <div class='form-section'><h2>Dashboard and access</h2><div class='row'><label>Bot admin role<select name='bot_admin_role'>{options(roles, config.get('bot_admin_role'))}</select></label><label>Welcome channel<select name='welcome_channel'>{options(text_channels, config.get('welcome_channel'))}</select></label></div></div>
      <div class='form-section'><h2>Tickets</h2><div class='row'><label>Ticket category<select name='ticket_category'>{options(categories, config.get('ticket_category'))}</select></label><label>Ticket transcript logs<select name='ticket_log_channel'>{options(text_channels, config.get('ticket_log_channel'))}</select></label></div><div class='row'><label>General support role<select name='ticket_role_general'>{options(roles, config.get('ticket_role_general'))}</select></label><label>HWID support role<select name='ticket_role_hwid'>{options(roles, config.get('ticket_role_hwid'))}</select></label></div><label>Key-not-received support role<select name='ticket_role_key_not_received'>{options(roles, config.get('ticket_role_key_not_received'))}</select></label></div>
      <div class='savebar'><button type='submit'>Save dashboard settings</button></div>
    </form>
    <div class='section-title'><h2>Send messages</h2><span class='muted'>Owner dashboard tools</span></div>
    <div class='grid'>
      <section class='card'><h2>📣 Announcement sender</h2><p class='muted'>Create a polished announcement embed with live preview and send it to any text channel.</p><a class='btn' href='/guild/{guild_id}/announcements'>Open announcement sender</a></section>
      <section class='card'><h2>✨ Embed sender</h2><p class='muted'>Build a custom embed with title, message, color, image, footer, and preview before sending.</p><a class='btn' href='/guild/{guild_id}/embeds'>Open embed sender</a></section>
      <section class='card'><h2>💌 User DM sender</h2><p class='muted'>Send fully custom private DMs with optional embeds, images, buttons-style links in text, and a live Discord-style preview.</p><a class='btn' href='/guild/{guild_id}/dms'>Open DM sender</a></section>
    </div>
    <div class='section-title'><h2>Setup links</h2></div><section class='card'><p class='muted'>OAuth verification URL:</p><code>{PUBLIC_BASE_URL}/verify/start?guild_id={guild_id}</code></section>
    """
    return page(guild.name, body)


async def guild_save(request: web.Request) -> web.Response:
    user = await get_dashboard_user(request)
    guild_id = int(request.match_info["guild_id"])
    if not user or not await dashboard_can_access(user, guild_id):
        raise web.HTTPForbidden(text=owner_private_message())
    data = await request.post()
    def as_int(name):
        value = str(data.get(name, "")).strip()
        return int(value) if value.isdigit() else None
    updates = {
        "enabled": str(data.get("enabled")) == "true",
        "store_url": str(data.get("store_url") or DEFAULT_STORE_URL).strip(),
        "verified_role": as_int("verified_role"),
        "unverified_role": as_int("unverified_role"),
        "auto_role": as_int("auto_role"),
        "bot_admin_role": as_int("bot_admin_role"),
        "welcome_channel": as_int("welcome_channel"),
        "verification_channel": as_int("verification_channel"),
        "verification_log_channel": as_int("verification_log_channel"),
        "ticket_category": as_int("ticket_category"),
        "ticket_log_channel": as_int("ticket_log_channel"),
        "ticket_role_general": as_int("ticket_role_general"),
        "ticket_role_hwid": as_int("ticket_role_hwid"),
        "ticket_role_key_not_received": as_int("ticket_role_key_not_received"),
    }
    await set_config(guild_id, updates)
    raise web.HTTPFound(f"/guild/{guild_id}")

def parse_embed_color(value: str) -> int:
    value = (value or "").strip().replace("#", "")
    if not value:
        return EMBED_COLOR
    try:
        return int(value, 16) & 0xFFFFFF
    except ValueError:
        return EMBED_COLOR


def channel_options(guild: discord.Guild, selected: Optional[int] = None) -> str:
    out = ["<option value=''>Select a channel</option>"]
    for channel in guild.text_channels:
        sel = "selected" if selected and int(selected) == channel.id else ""
        out.append(f"<option value='{channel.id}' {sel}>#{html.escape(channel.name)}</option>")
    return "".join(out)


def composer_page(guild: discord.Guild, mode: str, sent: bool = False) -> web.Response:
    is_announcement = mode == "announcement"
    title = "Announcement Sender" if is_announcement else "Embed Sender"
    defaults = {
        "title": "New Announcement" if is_announcement else "Embed Title",
        "message": "Write your embed description here..." if not is_announcement else "Write your announcement details here...",
        "content": "@everyone" if is_announcement else "",
        "footer": "moealturej",
        "color": "7C3AED",
    }
    body = f"""
    <section class='hero'><span class='pill'>{'📣 Premium announcement' if is_announcement else '✨ Premium embed'} composer</span><h1>{title}</h1><p class='muted'>Create a fully custom Discord message: optional text above the embed, optional embed, thumbnail image, large image, footer, color, and a live premium preview.</p></section>
    {'<section class="card" style="margin-top:16px"><span class="pill">✅ Sent successfully</span><p class="muted">Your message was sent to Discord.</p></section>' if sent else ''}
    <div class='section-title'><h2>Compose</h2><a class='btn secondary' href='/guild/{guild.id}'>Back to settings</a></div>
    <form class='grid' method='post'>
      <section class='card'>
        <label>Send to channel<select name='channel_id' required>{channel_options(guild)}</select></label>
        <div class='form-section'><h2>Message outside embed</h2><p class='muted'>This appears above the embed. Use it for pings, short notes, links, or send a plain message only.</p></div>
        <label>Top message / content<textarea id='contentInput' name='content' placeholder='Optional text shown above the embed'>{html.escape(defaults['content'])}</textarea></label>
        <div class='form-section'><h2>Embed builder</h2><label style='display:flex;align-items:center;gap:10px'><input id='embedEnabled' name='embed_enabled' type='checkbox' checked style='width:auto;margin:0'> Include embed</label></div>
        <label>Embed title<input id='titleInput' name='title' maxlength='256' value='{html.escape(defaults['title'])}'></label>
        <label>Embed description<textarea id='messageInput' name='message'>{html.escape(defaults['message'])}</textarea></label>
        <div class='row'><label>Color hex<input id='colorInput' name='color' value='{defaults['color']}' placeholder='7C3AED'></label><label>Footer<input id='footerInput' name='footer' value='{html.escape(defaults['footer'])}'></label></div>
        <div class='row'><label>Thumbnail image URL<input id='thumbInput' name='thumbnail_url' placeholder='Small top-right embed image URL'></label><label>Large image URL<input id='imageInput' name='image_url' placeholder='Large image under embed text URL'></label></div>
        <p class='tiny'>Discord supports one embed thumbnail and one large embed image. The top message is separate from the embed.</p>
        <div class='toolbar'><button type='submit'>{'Send announcement' if is_announcement else 'Send embed'}</button><a class='btn secondary' href='/guild/{guild.id}'>Cancel</a></div>
      </section>
      <section class='card'>
        <h2>Live Preview</h2>
        <p class='muted'>Preview includes the outside message, embed thumbnail, and large image.</p>
        <div class='preview-shell'>
          <div class='preview-message' id='contentPreview'></div>
          <div class='preview-box' id='previewBox'>
            <img class='preview-thumb' id='previewThumb' style='display:none'>
            <div class='preview-title' id='previewTitle'></div>
            <div class='preview-desc' id='previewDesc'></div>
            <img class='preview-img' id='previewImg' style='display:none'>
            <div class='preview-footer' id='previewFooter'></div>
          </div>
        </div>
      </section>
    </form>
    <script>
    const contentInput=document.getElementById('contentInput'), embedEnabled=document.getElementById('embedEnabled'), titleInput=document.getElementById('titleInput'), messageInput=document.getElementById('messageInput'), colorInput=document.getElementById('colorInput'), footerInput=document.getElementById('footerInput'), imageInput=document.getElementById('imageInput'), thumbInput=document.getElementById('thumbInput');
    const contentPreview=document.getElementById('contentPreview'), box=document.getElementById('previewBox'), pTitle=document.getElementById('previewTitle'), pDesc=document.getElementById('previewDesc'), pFooter=document.getElementById('previewFooter'), pImg=document.getElementById('previewImg'), pThumb=document.getElementById('previewThumb');
    function cleanHex(v){{v=(v||'7C3AED').replace('#','').trim(); return /^[0-9a-fA-F]{{6}}$/.test(v)?v:'7C3AED'}}
    function setImg(el,url){{url=(url||'').trim(); if(url){{el.src=url; el.style.display='block'}}else{{el.style.display='none'}}}}
    function updatePreview(){{
      const top=(contentInput.value||'').trim(); contentPreview.textContent=top||'No outside message. Only the embed will be sent.'; contentPreview.style.display=top||!embedEnabled.checked?'block':'none';
      box.style.display=embedEnabled.checked?'block':'none'; pTitle.textContent=titleInput.value||'Untitled'; pDesc.textContent=messageInput.value||''; pFooter.textContent=footerInput.value||''; box.style.borderLeftColor='#'+cleanHex(colorInput.value); setImg(pImg,imageInput.value); setImg(pThumb,thumbInput.value);
    }}
    [contentInput,embedEnabled,titleInput,messageInput,colorInput,footerInput,imageInput,thumbInput].forEach(el=>el.addEventListener('input',updatePreview)); embedEnabled.addEventListener('change',updatePreview); updatePreview();
    </script>
    """
    return page(title, body)

async def announcement_page(request: web.Request) -> web.Response:
    user = await get_dashboard_user(request)
    guild_id = int(request.match_info["guild_id"])
    if not user or not await dashboard_can_access(user, guild_id):
        raise web.HTTPForbidden(text=owner_private_message())
    guild = bot.get_guild(guild_id)
    if not guild:
        return page("Missing server", "<section class='card'><h1>Bot is not in this server</h1></section>")
    return composer_page(guild, "announcement", request.query.get("sent") == "1")


async def embed_page(request: web.Request) -> web.Response:
    user = await get_dashboard_user(request)
    guild_id = int(request.match_info["guild_id"])
    if not user or not await dashboard_can_access(user, guild_id):
        raise web.HTTPForbidden(text=owner_private_message())
    guild = bot.get_guild(guild_id)
    if not guild:
        return page("Missing server", "<section class='card'><h1>Bot is not in this server</h1></section>")
    return composer_page(guild, "embed", request.query.get("sent") == "1")


async def send_composer(request: web.Request, mode: str) -> web.Response:
    user = await get_dashboard_user(request)
    guild_id = int(request.match_info["guild_id"])
    if not user or not await dashboard_can_access(user, guild_id):
        raise web.HTTPForbidden(text=owner_private_message())
    guild = bot.get_guild(guild_id)
    if not guild:
        return page("Missing server", "<section class='card'><h1>Bot is not in this server</h1></section>")
    data = await request.post()
    channel_id = int(str(data.get("channel_id", "0")) or 0)
    channel = guild.get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        return page("Invalid channel", "<section class='card'><h1>Invalid channel</h1><p class='muted'>Choose a text channel the bot can send messages in.</p></section>")

    content = str(data.get("content") or "").strip()[:1900]
    embed_enabled = data.get("embed_enabled") == "on"
    embed = None
    title = str(data.get("title") or ("Announcement" if mode == "announcement" else "Embed"))[:256]

    if embed_enabled:
        message = str(data.get("message") or "").strip()[:4000]
        footer = str(data.get("footer") or "moealturej")[:2048]
        image_url = str(data.get("image_url") or "").strip()
        thumbnail_url = str(data.get("thumbnail_url") or "").strip()
        color = parse_embed_color(str(data.get("color") or ""))
        embed = make_embed(title, message or " ", color)
        if thumbnail_url:
            embed.set_thumbnail(url=thumbnail_url)
        if image_url:
            embed.set_image(url=image_url)
        if footer:
            embed.set_footer(text=footer)

    if not content and not embed:
        return page("Nothing to send", "<section class='card'><h1>Nothing to send</h1><p class='muted'>Add a top message, enable the embed, or both.</p></section>")

    await channel.send(content=content or None, embed=embed, allowed_mentions=discord.AllowedMentions.all() if mode == "announcement" else discord.AllowedMentions.none())
    await save_event("dashboard_events", {"guild_id": guild.id, "user_id": int(user["user_id"]), "event": f"send_{mode}", "channel_id": channel.id, "title": title, "has_content": bool(content), "has_embed": bool(embed)})
    raise web.HTTPFound(f"/guild/{guild.id}/{'announcements' if mode == 'announcement' else 'embeds'}?sent=1")

async def announcement_send(request: web.Request) -> web.Response:
    return await send_composer(request, "announcement")


async def embed_send(request: web.Request) -> web.Response:
    return await send_composer(request, "embed")


def member_select_options(guild: discord.Guild) -> str:
    """Build a manageable cached-member selector for the dashboard DM tool."""
    members = sorted(
        [m for m in guild.members if not m.bot],
        key=lambda m: (m.display_name or m.name).lower(),
    )[:500]
    out = ["<option value=''>Type/paste a Discord user ID or choose a cached member</option>"]
    for member in members:
        label = f"{member.display_name} (@{member.name}) — {member.id}"
        out.append(f"<option value='{member.id}'>{html.escape(label)}</option>")
    return "".join(out)


async def dm_page(request: web.Request) -> web.Response:
    user = await get_dashboard_user(request)
    guild_id = int(request.match_info["guild_id"])
    if not user or not await dashboard_can_access(user, guild_id):
        raise web.HTTPForbidden(text=owner_private_message())
    guild = bot.get_guild(guild_id)
    if not guild:
        return page("Missing server", "<section class='card'><h1>Bot is not in this server</h1></section>")
    sent = request.query.get("sent") == "1"
    failed = request.query.get("failed") == "1"
    reason = request.query.get("reason", "")[:180]
    body = f"""
    <section class='hero'><span class='pill'>💌 Premium DM composer</span><h1>User DM Sender</h1><p class='muted'>Send fully custom private DMs with text above the embed, optional embed, thumbnail image, large image, footer, color, and a live Discord-style preview.</p></section>
    {'<section class="card" style="margin-top:16px"><span class="pill">✅ DM sent</span><p class="muted">The private message was delivered successfully.</p></section>' if sent else ''}
    {'<section class="card" style="margin-top:16px"><span class="pill" style="background:rgba(251,113,133,.12);border-color:rgba(251,113,133,.28);color:#fecdd3">⚠️ DM failed</span><p class="muted">' + html.escape(reason or 'The bot could not DM that user. They may have DMs disabled or the ID was invalid.') + '</p></section>' if failed else ''}
    <div class='section-title'><h2>Compose private message</h2><a class='btn secondary' href='/guild/{guild.id}'>Back to settings</a></div>
    <form class='grid' method='post'>
      <section class='card'>
        <label>Choose cached member<select id='memberSelect'>{member_select_options(guild)}</select></label>
        <label>Discord user ID<input id='userIdInput' name='user_id' inputmode='numeric' pattern='[0-9]{{15,25}}' placeholder='1222903158125105194' required></label>
        <div class='form-section'><h2>Message outside embed</h2><p class='muted'>This appears as normal DM text above the embed. You can send only this, only an embed, or both.</p></div>
        <label>Top DM message<textarea id='plainInput' name='plain_message' placeholder='Custom text shown above the embed'></textarea></label>
        <div class='form-section'><h2>Optional embed</h2><label style='display:flex;align-items:center;gap:10px'><input id='embedEnabled' name='embed_enabled' type='checkbox' checked style='width:auto;margin:0'> Include embed</label></div>
        <label>Embed title<input id='titleInput' name='title' maxlength='256' value='Message from moealturej'></label>
        <label>Embed description<textarea id='messageInput' name='message'>Write your custom DM embed here...</textarea></label>
        <div class='row'><label>Color hex<input id='colorInput' name='color' value='7C3AED' placeholder='7C3AED'></label><label>Footer<input id='footerInput' name='footer' value='moealturej'></label></div>
        <div class='row'><label>Thumbnail image URL<input id='thumbInput' name='thumbnail_url' placeholder='Small top-right embed image URL'></label><label>Large image URL<input id='imageInput' name='image_url' placeholder='Large image under embed text URL'></label></div>
        <p class='tiny'>Use thumbnail for a small logo/profile image and large image for banners or previews.</p>
        <div class='toolbar'><button type='submit'>Send private DM</button><a class='btn secondary' href='/guild/{guild.id}'>Cancel</a></div>
      </section>
      <section class='card'>
        <h2>Live Preview</h2>
        <p class='muted'>This is a close preview of the DM the user will receive.</p>
        <div class='preview-shell'>
          <div class='preview-message' id='plainPreview'></div>
          <div class='preview-box' id='previewBox'>
            <img class='preview-thumb' id='previewThumb' style='display:none'>
            <div class='preview-title' id='previewTitle'></div>
            <div class='preview-desc' id='previewDesc'></div>
            <img class='preview-img' id='previewImg' style='display:none'>
            <div class='preview-footer' id='previewFooter'></div>
          </div>
        </div>
      </section>
    </form>
    <script>
    const memberSelect=document.getElementById('memberSelect'), userIdInput=document.getElementById('userIdInput'), plainInput=document.getElementById('plainInput'), embedEnabled=document.getElementById('embedEnabled');
    const titleInput=document.getElementById('titleInput'), messageInput=document.getElementById('messageInput'), colorInput=document.getElementById('colorInput'), footerInput=document.getElementById('footerInput'), imageInput=document.getElementById('imageInput'), thumbInput=document.getElementById('thumbInput');
    const box=document.getElementById('previewBox'), pTitle=document.getElementById('previewTitle'), pDesc=document.getElementById('previewDesc'), pFooter=document.getElementById('previewFooter'), pImg=document.getElementById('previewImg'), pThumb=document.getElementById('previewThumb'), plainPreview=document.getElementById('plainPreview');
    memberSelect.addEventListener('change',()=>{{if(memberSelect.value) userIdInput.value=memberSelect.value;}});
    function cleanHex(v){{v=(v||'7C3AED').replace('#','').trim(); return /^[0-9a-fA-F]{{6}}$/.test(v)?v:'7C3AED'}}
    function setImg(el,url){{url=(url||'').trim(); if(url){{el.src=url; el.style.display='block'}}else{{el.style.display='none'}}}}
    function updatePreview(){{
      const plain=(plainInput.value||'').trim(); plainPreview.textContent=plain||'No outside DM message. Only the embed will be sent.'; plainPreview.style.display=plain||!embedEnabled.checked?'block':'none';
      box.style.display=embedEnabled.checked?'block':'none'; pTitle.textContent=titleInput.value||'Untitled'; pDesc.textContent=messageInput.value||''; pFooter.textContent=footerInput.value||''; box.style.borderLeftColor='#'+cleanHex(colorInput.value); setImg(pImg,imageInput.value); setImg(pThumb,thumbInput.value);
    }}
    [plainInput,embedEnabled,titleInput,messageInput,colorInput,footerInput,imageInput,thumbInput].forEach(el=>el.addEventListener('input',updatePreview)); embedEnabled.addEventListener('change',updatePreview); updatePreview();
    </script>
    """
    return page("DM Sender", body)

async def dm_send(request: web.Request) -> web.Response:
    user = await get_dashboard_user(request)
    guild_id = int(request.match_info["guild_id"])
    if not user or not await dashboard_can_access(user, guild_id):
        raise web.HTTPForbidden(text=owner_private_message())
    guild = bot.get_guild(guild_id)
    if not guild:
        return page("Missing server", "<section class='card'><h1>Bot is not in this server</h1></section>")
    data = await request.post()
    raw_user_id = str(data.get("user_id", "")).strip()
    if not raw_user_id.isdigit():
        raise web.HTTPFound(f"/guild/{guild.id}/dms?failed=1&reason=Invalid+Discord+user+ID")
    target_id = int(raw_user_id)
    plain_message = str(data.get("plain_message") or "").strip()[:1900]
    embed_enabled = data.get("embed_enabled") == "on"
    embed = None
    if embed_enabled:
        title = str(data.get("title") or "Message from moealturej")[:256]
        message = str(data.get("message") or "").strip()[:4000]
        footer = str(data.get("footer") or "moealturej")[:2048]
        image_url = str(data.get("image_url") or "").strip()
        thumbnail_url = str(data.get("thumbnail_url") or "").strip()
        color = parse_embed_color(str(data.get("color") or ""))
        embed = make_embed(title, message or " ", color)
        if thumbnail_url:
            embed.set_thumbnail(url=thumbnail_url)
        if image_url:
            embed.set_image(url=image_url)
        if footer:
            embed.set_footer(text=footer)
    if not plain_message and not embed:
        raise web.HTTPFound(f"/guild/{guild.id}/dms?failed=1&reason=Write+a+plain+message+or+enable+an+embed+first")

    try:
        target = guild.get_member(target_id)
        if target is None:
            try:
                target = await guild.fetch_member(target_id)
            except discord.NotFound:
                target = await bot.fetch_user(target_id)
        await target.send(content=plain_message or None, embed=embed, allowed_mentions=discord.AllowedMentions.none())
    except discord.Forbidden:
        await save_event("dashboard_events", {"guild_id": guild.id, "user_id": int(user["user_id"]), "event": "send_dm_failed", "target_user_id": target_id, "reason": "forbidden"})
        raise web.HTTPFound(f"/guild/{guild.id}/dms?failed=1&reason=That+user+has+DMs+disabled+or+blocked+bot+DMs")
    except discord.HTTPException as exc:
        await save_event("dashboard_events", {"guild_id": guild.id, "user_id": int(user["user_id"]), "event": "send_dm_failed", "target_user_id": target_id, "reason": str(exc)[:300]})
        raise web.HTTPFound(f"/guild/{guild.id}/dms?failed=1&reason=Discord+rejected+the+DM+request")

    await save_event("dashboard_events", {"guild_id": guild.id, "user_id": int(user["user_id"]), "event": "send_dm", "target_user_id": target_id, "has_plain": bool(plain_message), "has_embed": bool(embed)})
    raise web.HTTPFound(f"/guild/{guild.id}/dms?sent=1")

async def verify_start(request: web.Request) -> web.Response:
    guild_id = int(request.query.get("guild_id", "0"))
    requested_user_id = int(request.query.get("user_id", "0") or 0)
    guild = bot.get_guild(guild_id)
    if not guild:
        return page("Verification", "<section class='card'><h1>Server not found</h1><p class='muted'>The bot is not in this server.</p></section>")
    if not requested_user_id:
        return page("Verification", f"<section class='hero'><span class='pill'>🔐 Secure verification</span><h1>Use the server verify button</h1><p class='muted'>For safety, verification links are generated privately after the bot checks your roles inside {html.escape(guild.name)}. Go back to Discord and click the verify button again.</p></section>")

    # Extra web-side guard. The main no-link check happens in the Discord button interaction,
    # but this prevents old/copied links from making already-verified members authorize again.
    if requested_user_id:
        config = await get_guild_config(guild_id)
        member = guild.get_member(requested_user_id)
        verified_role = guild.get_role(int(config.get("verified_role") or 0)) if config.get("verified_role") else None
        if member and verified_role and verified_role in member.roles:
            removed = await safe_remove_role(member, config.get("unverified_role"), "Already verified cleanup from web guard")
            await log_verification(guild, member, "web-precheck", "already_verified", "OAuth start blocked because member already had verified role." + (" Unverified role removed." if removed else ""))
            return page("Already verified", f"<section class='hero'><span class='pill'>✅ Already verified</span><h1>No action needed</h1><p class='muted'>You are already verified in {html.escape(guild.name)}. You can close this page.</p></section>")

    state = secrets.token_urlsafe(32)
    await mdb.oauth_states.insert_one({"state": state, "type": "verify", "guild_id": guild_id, "requested_user_id": requested_user_id, "expires_at": utcnow() + timedelta(minutes=10)})
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": f"{PUBLIC_BASE_URL}/verify/callback",
        "response_type": "code",
        "scope": "identify guilds.join",
        "state": state,
    }
    raise web.HTTPFound(f"https://discord.com/oauth2/authorize?{urlencode(params)}")


async def verify_callback(request: web.Request) -> web.Response:
    state = request.query.get("state", "")
    code = request.query.get("code", "")
    found = await mdb.oauth_states.find_one_and_delete({"state": state, "type": "verify", "expires_at": {"$gt": utcnow()}})
    if not found or not code:
        raise web.HTTPBadRequest(text="Invalid or expired verification state.")
    guild_id = int(found["guild_id"])
    guild = bot.get_guild(guild_id)
    if not guild:
        return page("Verification", "<section class='card'><h1>Server not found</h1></section>")
    token = await exchange_code(code, f"{PUBLIC_BASE_URL}/verify/callback")
    user = await discord_get("/users/@me", token["access_token"])
    user_id = int(user["id"])
    config = await get_guild_config(guild_id)

    # guilds.join lets the app add the user to the server when the bot is in that server.
    await discord_put(f"/guilds/{guild_id}/members/{user_id}", BOT_TOKEN, {"access_token": token["access_token"]})
    await asyncio.sleep(1)
    member = guild.get_member(user_id) or await guild.fetch_member(user_id)

    verified_role_id = config.get("verified_role")
    verified_role = guild.get_role(int(verified_role_id or 0)) if verified_role_id else None
    already_verified = bool(verified_role and verified_role in member.roles)

    if already_verified:
        role_ok = True
        details = "OAuth authorized. User already had the verified role."
    else:
        role_ok = await safe_add_role(member, verified_role_id, "User completed OAuth2 verification")
        details = "OAuth authorized. Verified role assigned." if role_ok else "OAuth authorized, but verified role was not assigned. Check role position/config."

    removed_unverified = await safe_remove_role(member, config.get("unverified_role"), "User completed OAuth2 verification")
    if removed_unverified:
        details += " Unverified role removed."

    await send_verified_dm(member, config.get("store_url", DEFAULT_STORE_URL))
    await log_verification(guild, member, "oauth2", "success" if role_ok else "failed", details)
    return page("Verified", f"<section class='hero'><span class='pill'>✅ Verified</span><h1>{'Already verified' if already_verified else 'Verified'}</h1><p class='muted'>You are verified in {html.escape(guild.name)}. You can close this page.</p></section>")


async def health(request: web.Request) -> web.Response:
    uptime = utcnow() - STARTED_AT
    return web.json_response({"status": "ok", "bot": str(bot.user) if bot.user else "starting", "guilds": len(bot.guilds), "latency_ms": round(bot.latency * 1000) if bot.latency else None, "uptime_seconds": int(uptime.total_seconds())})


async def start_web() -> None:
    global web_runner
    if web_runner:
        return
    app = web.Application(client_max_size=8 * 1024 ** 2)
    app.router.add_get("/", home)
    app.router.add_get("/login", login)
    app.router.add_get("/oauth/callback", oauth_callback)
    app.router.add_get("/logout", logout)
    app.router.add_get("/guild/{guild_id}", guild_page)
    app.router.add_post("/guild/{guild_id}", guild_save)
    app.router.add_get("/guild/{guild_id}/announcements", announcement_page)
    app.router.add_post("/guild/{guild_id}/announcements", announcement_send)
    app.router.add_get("/guild/{guild_id}/embeds", embed_page)
    app.router.add_post("/guild/{guild_id}/embeds", embed_send)
    app.router.add_get("/guild/{guild_id}/dms", dm_page)
    app.router.add_post("/guild/{guild_id}/dms", dm_send)
    app.router.add_get("/verify/start", verify_start)
    app.router.add_get("/verify/callback", verify_callback)
    app.router.add_get("/health", health)
    web_runner = web.AppRunner(app)
    await web_runner.setup()
    await web.TCPSite(web_runner, WEB_HOST, WEB_PORT).start()
    print(f"Dashboard running on http://{WEB_HOST}:{WEB_PORT}")

# =========================
# EVENTS / TASKS
# =========================
@bot.event
async def setup_hook():
    await init_mongo()
    await start_web()


@bot.event
async def on_ready():
    bot.add_view(TicketPanelView())
    bot.add_view(CloseTicketView())
    bot.add_view(OAuthVerifyView(0))
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash commands.")
    except Exception as e:
        print(f"Slash command sync failed: {e}")
    if not rotate_status.is_running(): rotate_status.start()
    if not update_stats.is_running(): update_stats.start()
    if not self_ping.is_running(): self_ping.start()
    print(f"Logged in as {bot.user}")


@bot.event
async def on_member_join(member: discord.Member):
    config = await get_guild_config(member.guild.id)
    verified_role = member.guild.get_role(int(config.get("verified_role") or 0)) if config.get("verified_role") else None
    if config.get("unverified_role") and not (verified_role and verified_role in member.roles):
        await safe_add_role(member, config.get("unverified_role"), "Unverified role on join")
    if config.get("auto_role"):
        await safe_add_role(member, config.get("auto_role"), "Auto role on join")
    channel = member.guild.get_channel(config.get("welcome_channel") or 0)
    if isinstance(channel, discord.TextChannel):
        embed = make_embed("Welcome", f"Welcome {member.mention} to **{member.guild.name}**. Please verify if required and open a ticket if you need support.")
        embed.set_thumbnail(url=member.display_avatar.url)
        await channel.send(embed=embed)


@tasks.loop(seconds=45)
async def rotate_status():
    if not ROTATING_STATUSES: return
    status = ROTATING_STATUSES[rotate_status.current_loop % len(ROTATING_STATUSES)]
    activity = discord.Activity(type=discord.ActivityType.watching, name=status[9:]) if status.lower().startswith("watching ") else discord.Game(name=status)
    await bot.change_presence(status=discord.Status.online, activity=activity)


@tasks.loop(minutes=10)
async def update_stats():
    for guild in bot.guilds:
        config = await get_guild_config(guild.id)
        channels = config.get("stats_channels", {})
        humans = len([m for m in guild.members if not m.bot])
        bots = len([m for m in guild.members if m.bot])
        members = guild.member_count or len(guild.members)
        boosts = guild.premium_subscription_count or 0
        stats = {"members": f"👥 Members: {members}", "humans": f"🧑 Humans: {humans}", "bots": f"🤖 Bots: {bots}", "boosts": f"🚀 Boosts: {boosts}"}
        for key, name in stats.items():
            channel = guild.get_channel(channels.get(key) or 0)
            if isinstance(channel, discord.VoiceChannel) and channel.name != name:
                try: await channel.edit(name=name, reason="Live server stats update")
                except discord.HTTPException: pass


@tasks.loop(minutes=1)
async def self_ping():
    url = KEEP_ALIVE_URL or f"http://127.0.0.1:{WEB_PORT}/health"
    try:
        async with ClientSession() as session:
            async with session.get(url, timeout=15) as response:
                await response.text()
    except (ClientError, asyncio.TimeoutError) as e:
        print(f"Self-ping failed for {url}: {e}")

# =========================
# COMMANDS
# =========================
@bot.tree.command(name="ping", description="Check bot latency.")
@guild_enabled_or_owner()
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message(embed=make_embed("Pong", f"Latency: `{round(bot.latency * 1000)}ms`"), ephemeral=True)


@bot.tree.command(name="store", description="Get the store link.")
@guild_enabled_or_owner()
async def store(interaction: discord.Interaction):
    config = await get_guild_config(interaction.guild.id) if interaction.guild else {"store_url": DEFAULT_STORE_URL}
    await interaction.response.send_message(embed=make_embed("Store", f"Visit the store here:\n{config.get('store_url', DEFAULT_STORE_URL)}"), ephemeral=True)


@bot.tree.command(name="help", description="Show public commands.")
async def help_command(interaction: discord.Interaction):
    embed = make_embed("Help", "Public commands available here.")
    embed.add_field(name="Commands", value="`/ping` - Check latency\n`/store` - Store link\n`/help` - This menu", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="commands", description="Show private owner/admin commands.")
@admin_only()
async def commands_menu(interaction: discord.Interaction):
    embed = make_embed("Admin Commands", "Private setup commands for this bot.")
    embed.add_field(name="Setup", value="`/setup_enable` `/set_admin_role` `/set_verified_role` `/set_unverified_role` `/set_auto_role` `/set_logs` `/set_ticket_category` `/set_ticket_role` `/stats_setup`", inline=False)
    embed.add_field(name="Panels", value="`/send_verification_panel` `/send_ticket_panel`", inline=False)
    embed.add_field(name="Content", value="`/set_store` `/announce` `/config_show`", inline=False)
    embed.add_field(name="Dashboard", value=f"{PUBLIC_BASE_URL}/", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="setup_enable", description="Owner: enable or disable this bot in this server.")
@admin_only()
async def setup_enable(interaction: discord.Interaction, enabled: bool):
    if not is_owner_user(interaction.user.id):
        return await interaction.response.send_message(owner_private_message(), ephemeral=True)
    await set_config(interaction.guild.id, {"enabled": enabled})
    await interaction.response.send_message(f"Server access is now {'enabled' if enabled else 'disabled/private'}.", ephemeral=True)


@bot.tree.command(name="set_admin_role", description="Set the role allowed to use admin bot commands.")
@admin_only()
async def set_admin_role(interaction: discord.Interaction, role: discord.Role):
    await set_config(interaction.guild.id, {"bot_admin_role": role.id})
    await interaction.response.send_message(f"Bot admin role set to {role.mention}.", ephemeral=True)


@bot.tree.command(name="set_verified_role", description="Set the role given after OAuth2 verification.")
@admin_only()
async def set_verified_role(interaction: discord.Interaction, role: discord.Role):
    await set_config(interaction.guild.id, {"verified_role": role.id})
    await interaction.response.send_message(f"Verified role set to {role.mention}.", ephemeral=True)


@bot.tree.command(name="set_unverified_role", description="Set the role removed after successful verification.")
@admin_only()
async def set_unverified_role(interaction: discord.Interaction, role: discord.Role):
    await set_config(interaction.guild.id, {"unverified_role": role.id})
    await interaction.response.send_message(f"Unverified role set to {role.mention}. It will be removed after verification.", ephemeral=True)


@bot.tree.command(name="set_auto_role", description="Set the role automatically given when a member joins.")
@admin_only()
async def set_auto_role(interaction: discord.Interaction, role: discord.Role):
    await set_config(interaction.guild.id, {"auto_role": role.id})
    await interaction.response.send_message(f"Auto role set to {role.mention}.", ephemeral=True)


@bot.tree.command(name="set_logs", description="Set verification and ticket transcript log channels.")
@admin_only()
async def set_logs(interaction: discord.Interaction, verification_logs: Optional[discord.TextChannel] = None, ticket_transcripts: Optional[discord.TextChannel] = None):
    updates = {}
    if verification_logs: updates["verification_log_channel"] = verification_logs.id
    if ticket_transcripts: updates["ticket_log_channel"] = ticket_transcripts.id
    await set_config(interaction.guild.id, updates)
    await interaction.response.send_message("Log channels updated.", ephemeral=True)


@bot.tree.command(name="send_verification_panel", description="Send the OAuth2 verification panel.")
@admin_only()
async def send_verification_panel(interaction: discord.Interaction, channel: discord.TextChannel):
    await set_config(interaction.guild.id, {"verification_channel": channel.id})
    embed = make_embed("Verify Access", "Click below to verify with Discord OAuth2. This securely confirms your Discord account and can add you to the server if needed.")
    embed.set_footer(text="moealturej OAuth2 verification")
    await channel.send(embed=embed, view=OAuthVerifyView(interaction.guild.id))
    await interaction.response.send_message(f"OAuth2 verification panel sent in {channel.mention}.", ephemeral=True)


@bot.tree.command(name="set_ticket_category", description="Set the category where tickets will be created.")
@admin_only()
async def set_ticket_category(interaction: discord.Interaction, category: discord.CategoryChannel):
    await set_config(interaction.guild.id, {"ticket_category": category.id})
    await interaction.response.send_message(f"Ticket category set to **{category.name}**.", ephemeral=True)


@bot.tree.command(name="set_ticket_role", description="Set the support role for a ticket type.")
@app_commands.choices(ticket_type=[app_commands.Choice(name="General support", value="general"), app_commands.Choice(name="Key HWID reset", value="hwid"), app_commands.Choice(name="Key not received", value="key_not_received")])
@admin_only()
async def set_ticket_role(interaction: discord.Interaction, ticket_type: app_commands.Choice[str], role: discord.Role):
    await set_config(interaction.guild.id, {TICKET_TYPES[ticket_type.value]["support_role_key"]: role.id})
    await interaction.response.send_message(f"{ticket_type.name} support role set to {role.mention}.", ephemeral=True)


@bot.tree.command(name="send_ticket_panel", description="Send the ticket panel.")
@admin_only()
async def send_ticket_panel(interaction: discord.Interaction, channel: discord.TextChannel):
    await set_config(interaction.guild.id, {"ticket_panel_channel": channel.id})
    embed = make_embed("Support Tickets", "Choose the ticket type that matches your issue. A private support channel will be created.")
    embed.add_field(name="Options", value="💬 General support\n🔑 Key HWID reset\n📦 Key not received", inline=False)
    await channel.send(embed=embed, view=TicketPanelView())
    await interaction.response.send_message(f"Ticket panel sent in {channel.mention}.", ephemeral=True)


@bot.tree.command(name="set_store", description="Set the store URL used by /store.")
@admin_only()
async def set_store(interaction: discord.Interaction, url: str):
    await set_config(interaction.guild.id, {"store_url": url})
    await interaction.response.send_message(f"Store URL set to: {url}", ephemeral=True)


@bot.tree.command(name="announce", description="Send a clean announcement embed.")
@admin_only()
async def announce(interaction: discord.Interaction, channel: discord.TextChannel, title: str, message: str, image_url: Optional[str] = None):
    config = await get_guild_config(interaction.guild.id)
    embed = make_embed(title, message)
    if image_url or config.get("announce_image"):
        embed.set_image(url=image_url or config.get("announce_image"))
    embed.set_footer(text=config.get("announce_footer") or "moealturej")
    await channel.send(embed=embed)
    await interaction.response.send_message(f"Announcement sent in {channel.mention}.", ephemeral=True)


@bot.tree.command(name="stats_setup", description="Create/connect emoji live server stats voice channels.")
@admin_only()
async def stats_setup(interaction: discord.Interaction, category: Optional[discord.CategoryChannel] = None):
    guild = interaction.guild
    if category is None:
        category = await guild.create_category("📊 Server Stats", reason="Live server stats setup")
    overwrites = {guild.default_role: discord.PermissionOverwrite(connect=False, view_channel=True), guild.me: discord.PermissionOverwrite(connect=True, manage_channels=True, view_channel=True)}
    defaults = {"members": "👥 Members: 0", "humans": "🧑 Humans: 0", "bots": "🤖 Bots: 0", "boosts": "🚀 Boosts: 0"}
    created = {}
    config = await get_guild_config(guild.id)
    for key, name in defaults.items():
        channel = guild.get_channel((config.get("stats_channels") or {}).get(key) or 0)
        if not isinstance(channel, discord.VoiceChannel):
            channel = await guild.create_voice_channel(name, category=category, overwrites=overwrites, reason="Live stats channel created")
        created[key] = channel.id
    await set_config(guild.id, {"stats_category": category.id, "stats_channels": created})
    await update_stats()
    await interaction.response.send_message(f"Emoji live stats channels are set in **{category.name}**.", ephemeral=True)


@bot.tree.command(name="config_show", description="Show this server's saved config.")
@admin_only()
async def config_show(interaction: discord.Interaction):
    config = await get_guild_config(interaction.guild.id)
    embed = make_embed("Server Config", "Current MongoDB settings.")
    for key in ["enabled", "verified_role", "unverified_role", "auto_role", "bot_admin_role", "verification_log_channel", "ticket_log_channel", "ticket_category", "store_url"]:
        embed.add_field(name=key, value=str(config.get(key)), inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

# =========================
# START
# =========================
if __name__ == "__main__":
    missing = [name for name, value in {
        "BOT_TOKEN": BOT_TOKEN,
        "DISCORD_CLIENT_ID": DISCORD_CLIENT_ID,
        "DISCORD_CLIENT_SECRET": DISCORD_CLIENT_SECRET,
        "PUBLIC_BASE_URL": PUBLIC_BASE_URL,
        "MONGO_URI": MONGO_URI,
    }.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required .env values: {', '.join(missing)}")
    bot.run(BOT_TOKEN)
