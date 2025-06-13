from datetime import datetime, timedelta, timezone
from pathlib import Path
from functools import wraps
from typing import Dict
import types

from nonebot import get_driver, on_command
from nonebot.log import logger
from nonebot.permission import SUPERUSER
from nonebot.adapters.onebot.v11 import Bot as OB11Bot, Message, MessageSegment, GroupMessageEvent, MessageEvent, GROUP_ADMIN, GROUP_OWNER
from nonebot.params import CommandArg
from typing import Union, List, Tuple
from src.plugins.nonebot_plugin_status.data_source import get_current_cpu_usage

# === 配置 ===
BASE_LOG_DIR = Path("data/msg_stats")
BASE_LOG_DIR.mkdir(parents=True, exist_ok=True)

async def send_text_as_forward_msg(bot: OB11Bot, event: Union[GroupMessageEvent, MessageEvent], text: Union[str, List[str]]):
    if isinstance(text, str):
        text = [text]

    nodes = [MessageSegment.node_custom(
        user_id=bot.self_id,
        nickname="小维Bot",
        content=Message(t)
    ) for t in text]

    try:
        if isinstance(event, GroupMessageEvent):
            await bot.send_group_forward_msg(
                group_id=event.group_id,
                messages=nodes
            )
        elif isinstance(event, MessageEvent):
            await bot.send_private_forward_msg(
                user_id=event.get_user_id(),
                messages=nodes
            )
        else:
            raise TypeError("Unsupported event type")

    except Exception as e:
        from nonebot.log import logger
        logger.error(f"发送转发消息失败: {e}")

def get_today_dir() -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dir_path = BASE_LOG_DIR / date_str
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path

def get_yesterday_dir() -> Path:
    date_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    dir_path = BASE_LOG_DIR / date_str
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def append_text(path: Path, text: str, encoding: str = "utf-8"):
    with path.open("a", encoding=encoding) as f:
        f.write(text)


class StatsManager:
    def __init__(self):
        self.current_date: str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.stats: Dict[str, int] = {"group": 0, "private": 0, "unknown": 0}
        self.group_stats: Dict[int, int] = {}  # 每群统计
        self._load_log()

    def update_date(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.current_date:
            self.current_date = today
            self.stats = {"group": 0, "private": 0, "unknown": 0}
            self.group_stats = {}
            self._load_log()

    def increment(self, category: str, group_id: int = None):
        self.update_date()
        self.stats[category] = self.stats.get(category, 0) + 1

        if category == "group" and group_id is not None:
            self.group_stats[group_id] = self.group_stats.get(group_id, 0) + 1
            self._write_group_csv()

        self._write_log()

    def log_message_detail(self, category: str, target_id: int, content, content_type: str):
        self.update_date()
        log_file = get_today_dir() / f"{category}.log"
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        content_str = self._format_content(content)
        append_text(log_file, f"{timestamp} | {target_id} | {content_str}\n")

    def _format_content(self, message) -> str:
        if isinstance(message, Message):
            parts = []
            for seg in message:
                if seg.type == "text":
                    parts.append(seg.data.get("text", ""))
                else:
                    parts.append(f"[{seg.type}]")
            return "".join(parts)
        elif isinstance(message, MessageSegment):
            if message.type == "text":
                return message.data.get("text", "")
            else:
                return f"[{message.type}]"
        elif isinstance(message, str):
            return message
        return "[unknown]" + str(message)[:50]

    def _write_log(self):
        file_path = get_today_dir() / "stats.log"
        content = (
            f"日期(UTC): {self.current_date}\n"
            f"群聊发送数: {self.stats['group']}\n"
            f"私聊发送数: {self.stats['private']}"
        )
        file_path.write_text(content, encoding="utf-8")

    def _write_group_csv(self):
        file_path = get_today_dir() / "group_stats.csv"
        lines = ["id,count"]
        for gid, count in self.group_stats.items():
            lines.append(f"{gid},{count}")
        file_path.write_text("\n".join(lines), encoding="utf-8")

    def _load_log(self):
        # 恢复 stats.log
        file_path = get_today_dir() / "stats.log"
        if file_path.exists():
            content = file_path.read_text(encoding="utf-8")
            try:
                for line in content.strip().splitlines():
                    if "群聊发送数" in line:
                        self.stats["group"] = int(line.split(":")[1].strip())
                    elif "私聊发送数" in line:
                        self.stats["private"] = int(line.split(":")[1].strip())
                    elif "未知类型" in line:
                        self.stats["unknown"] = int(line.split(":")[1].strip())
                logger.info(f"[msg_counter] 从日志恢复统计数据: {self.stats}")
            except Exception as e:
                logger.warning(f"[msg_counter] 恢复 stats.log 失败: {e}")

        # 恢复 group_stats.csv
        csv_path = get_today_dir() / "group_stats.csv"
        if csv_path.exists():
            try:
                lines = csv_path.read_text(encoding="utf-8").strip().splitlines()
                self.group_stats = {}
                for line in lines[1:]:
                    gid_str, count_str = line.strip().split(",")
                    self.group_stats[int(gid_str)] = int(count_str)
                logger.info(f"[msg_counter] 从 CSV 恢复群组统计数据: {self.group_stats}")
            except Exception as e:
                logger.warning(f"[msg_counter] 恢复 group_stats.csv 失败: {e}")


stats_manager = StatsManager()

# === Hook call_api ===
driver = get_driver()

ban_cmds = ["你不希望机器人自己触发的指令，如果你允许人机合一且人机可以执行高权限指令']

def patch_call_api(bot: OB11Bot):
    original_call_api = bot.call_api

    @wraps(original_call_api)
    async def wrapped_call_api(self: OB11Bot, api: str, **data):
        avg = await get_current_cpu_usage()
        
        logger.debug(f"[msg_counter] 拦截 API 调用: {api}, 参数: {str(data)[:300]}")

        message = data.get("message", Message())
        for seg in message:
            if isinstance(seg, MessageSegment) and seg.type == "text":
                text = seg.data.get("text", "")
                if any(text.startswith(cmd) for cmd in ban_cmds):
                    data['message'] = Message(MessageSegment.text("[禁止诱导触发指令]") + message)
                    logger.warning(f"[msg_counter] 检测到诱导触发指令: {text}，已替换为禁止触发提示")
                    break
                
        if api in {"send_group_msg", "send_group_msg_async", "send_private_msg", "send_private_msg_async", "send_msg", "send_msg_async"} and isinstance(message, Message):
            if avg > 90:
                message.append(MessageSegment.text(f"\n⚠️ 小维处于极端负载状态: {avg:.2f}%！响应较慢"))
            elif avg > 80:
                message.append(MessageSegment.text(f"\n⚠️ 小维处于高负载状态: {avg:.2f}%！"))
                
        if api in {"send_group_msg", "send_group_msg_async", "send_group_forward_msg"}:
            gid = data.get("group_id", -1)
            
            # 获取统计值
            stats_manager.update_date()
            group_count = stats_manager.group_stats.get(gid, 0)
            all_count = stats_manager.stats.get("group", 0)
            # 判断是否追加提示
            if ((all_count >= 3000 and (all_count%25==0 or group_count%10==0)) or all_count%100==0) and 'forward' not in api:
                last_seg = message[-1] if message else None
                if last_seg and last_seg.type in {"text", "image"}:
                    message.append(MessageSegment.text(f"\n📈 今日群聊已发送 {all_count}/3000 本群 {group_count} 超出后可能限制发言"))
                    data["message"] = message

            stats_manager.increment("group", gid)
            stats_manager.log_message_detail("group", gid, message, data.get("message_type", "text"))


        elif api in {"send_private_msg", "send_private_msg_async", "send_private_forward_msg"}:
            uid = data.get("user_id", -1)
            private_count = stats_manager.stats.get("private", 0)
            if private_count%20==0 and 'forward' not in api:
                last_seg = message[-1] if message else None
                if last_seg and last_seg.type in {"text", "image"}:
                    message.append(MessageSegment.text(f"\n📈 今日私聊共已发送 {private_count}"))
                    data["message"] = message
            
            stats_manager.increment("private", uid)
            stats_manager.log_message_detail("private", uid, message, data.get("message_type", "text"))

        elif api in {"send_msg", "send_msg_async", "send_forward_msg"}:
            msg_type = data.get("message_type", "unknown")
            target_id = data.get("group_id") if msg_type == "group" else data.get("user_id", -1)

            if not 'forward' not in api:
                # 群聊才处理提示追加
                if msg_type == "group":
                    stats_manager.update_date()
                    group_count = stats_manager.group_stats.get(target_id, 0)            
                    all_count = stats_manager.stats.get("group", 0)
                    if (all_count >= 3000 and (all_count%25==0 or group_count%10==0)) or all_count%100==0:
                        last_seg = message[-1] if message else None
                        if last_seg and last_seg.type in {"text", "image"}:
                            message.append(MessageSegment.text(f"\n📈 今日已发送 {all_count}/3000，本群 {group_count} 超出后可能限制发言"))
                            data["message"] = message
                else:
                    stats_manager.update_date()
                    private_count = stats_manager.stats.get("private", 0)
                    if private_count%20==0:
                        last_seg = message[-1] if message else None
                        if last_seg and last_seg.type in {"text", "image"}:
                            message.append(MessageSegment.text(f"\n📈 今日私聊共已发送 {private_count}"))
                            data["message"] = message

            stats_manager.increment(msg_type if msg_type in ["group", "private"] else "unknown", target_id)
            stats_manager.log_message_detail(msg_type, target_id, message, msg_type)


        return await original_call_api(api, **data)

    bot.call_api = types.MethodType(wrapped_call_api, bot)
    logger.success(f"[msg_counter] 成功 Hook Bot {bot.self_id} 的 call_api 方法")


@driver.on_bot_connect
async def handle_bot_connect(bot: OB11Bot):
    if isinstance(bot, OB11Bot):
        patch_call_api(bot)


# === 指令：今日统计 ===
cmd_stats = on_command("统计", aliases={"sc"}, permission=SUPERUSER, priority=5, block=True)
cmd_stats_yesterday = on_command("昨日统计", aliases={"scy"}, permission=SUPERUSER, priority=5, block=True)


@cmd_stats.handle()
async def handle_stats_cmd():
    file_path = get_today_dir() / "stats.log"

    if not file_path.exists():
        await cmd_stats.finish("📭 今天还没有任何消息发送记录。")

    content = file_path.read_text(encoding="utf-8")
    await cmd_stats.finish(f"📊 今日发送统计：\n\n{content}")

@cmd_stats_yesterday.handle()
async def handle_yesterday_stats_cmd():
    file_path = get_yesterday_dir() / "stats.log"

    if not file_path.exists():
        await cmd_stats_yesterday.finish("📭 昨天还没有任何消息发送记录。")

    content = file_path.read_text(encoding="utf-8")
    await cmd_stats_yesterday.finish(f"📊 昨日发送统计：\n\n{content}")

# === 指令：群组统计 ===
cmd_group_stats = on_command("群组统计", aliases={"gsc", "今日群组统计"}, permission=SUPERUSER|GROUP_OWNER|GROUP_ADMIN, priority=5, block=True)
cmd_group_stats_yesterday = on_command("昨日群组统计", aliases={"gscy"}, permission=SUPERUSER|GROUP_OWNER|GROUP_ADMIN, priority=5, block=True)


@cmd_group_stats.handle()
async def handle_group_stats_cmd(bot: OB11Bot, event: MessageEvent, args: Message = CommandArg()):
    csv_path = get_today_dir() / "group_stats.csv"

    if not csv_path.exists():
        await cmd_group_stats.finish("📭 今天还没有任何群组消息统计。")

    lines = csv_path.read_text(encoding="utf-8").strip().splitlines()
    if len(lines) <= 1:
        await cmd_group_stats.finish("📭 今天还没有任何群组消息记录。")

    groups = [line.split(",") for line in lines[1:]]
    groups.sort(key=lambda x: int(x[1]), reverse=True)

    arg_text = args.extract_plain_text().strip()
    gid = event.group_id if isinstance(event, GroupMessageEvent) else None

    if arg_text.isdigit():
        requested_gid = int(arg_text)
        if not isinstance(event, GroupMessageEvent) or requested_gid != event.group_id:
            if str(event.user_id) not in get_driver().config.superusers:
                await cmd_group_stats.finish("🚫 仅超级用户可查看其他群统计数据。")
        gid = requested_gid

    if arg_text.lower() == "all" or arg_text.lower() is None:
        if str(event.user_id) not in get_driver().config.superusers:
            await cmd_group_stats.finish("🚫 仅超级用户可查看全部群数据。")
        msg = "📚 群组发送统计：\n\n"
        for g, count in groups:
            msg += f"群号 {g}: {count} 条\n"
        return await send_text_as_forward_msg(bot, event, msg.strip())
        # await cmd_group_stats.finish(msg.strip())

    for g, count in groups:
        if int(g) == gid:
            await cmd_group_stats.finish(f"📊 群组 {gid} 今日(UTC)发送统计：{count} 条")
    await cmd_group_stats.finish(f"📭 群组 {gid} 今日(UTC)没有发送记录。")



@cmd_group_stats_yesterday.handle()
async def handle_yesterday_group_stats_cmd(bot: OB11Bot, event: MessageEvent, args: Message = CommandArg()):
    csv_path = get_yesterday_dir() / "group_stats.csv"

    if not csv_path.exists():
        await cmd_group_stats_yesterday.finish("📭 昨天还没有任何群组消息统计。")

    lines = csv_path.read_text(encoding="utf-8").strip().splitlines()
    if len(lines) <= 1:
        await cmd_group_stats_yesterday.finish("📭 昨天还没有任何群组消息记录。")

    groups = [line.split(",") for line in lines[1:]]
    groups.sort(key=lambda x: int(x[1]), reverse=True)

    arg_text = args.extract_plain_text().strip()
    gid = event.group_id if isinstance(event, GroupMessageEvent) else None

    if arg_text.isdigit():
        requested_gid = int(arg_text)
        if not isinstance(event, GroupMessageEvent) or requested_gid != event.group_id:
            if str(event.user_id) not in get_driver().config.superusers:
                await cmd_group_stats_yesterday.finish("🚫 仅超级用户可查看其他群统计数据。")
        gid = requested_gid

    if arg_text.lower() == "all" or arg_text.lower() is None:
        if str(event.user_id) not in get_driver().config.superusers:
            await cmd_group_stats_yesterday.finish("🚫 仅超级用户可查看全部群数据。")
        msg = "📚 昨日群组发送统计：\n\n"
        for g, count in groups:
            msg += f"群号 {g}: {count} 条\n"
        return await send_text_as_forward_msg(bot, event, msg.strip())
        # await cmd_group_stats_yesterday.finish(msg.strip())

    for g, count in groups:
        if int(g) == gid:
            await cmd_group_stats_yesterday.finish(f"📊 群组 {gid} 昨日(UTC)发送统计：{count} 条")
    await cmd_group_stats_yesterday.finish(f"📭 群组 {gid} 昨日(UTC)没有发送记录。")
