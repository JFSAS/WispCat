"""Install the bounded WeChat policy into an unused AstrBot 4.29.0 tree."""

import argparse
import hashlib
import shutil
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("source", type=Path)
args = parser.parse_args()
root = args.source
here = Path(__file__).resolve().parent
stage = root / "astrbot/core/pipeline/waking_check/stage.py"
main = root / "astrbot/builtin_stars/astrbot/main.py"
assert "wechat_group_policy" not in stage.read_text(), "policy is already installed"
expected = {
    "astrbot/core/pipeline/waking_check/stage.py": "ab99a142b20f6ac842f6a90504433ca1337b995491541e14d38010bcfe5e533b",
    "astrbot/builtin_stars/astrbot/main.py": "f10b05d9716f9b275da9998e979eb6a80a75fa8cb36a91616218f36bc27cd36f",
}
for name, digest in expected.items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, (
        "unsupported or modified AstrBot source: " + name
    )
s = stage.read_text()
s = s.replace(
    "from astrbot.core.star.star_handler import EventType, star_handlers_registry\n",
    "from astrbot.core.star.star_handler import EventType, star_handlers_registry\nfrom astrbot.core.wechat_group_policy import WeChatGroupPolicy\n",
    1,
)
s = s.replace(
    "        self.ctx = ctx\n",
    "        self.ctx = ctx\n        self.wechat_groups = WeChatGroupPolicy(ctx.astrbot_config)\n",
    1,
)
marker = "        # 设置 sender 身份\n"
assert s.count(marker) == 1
s = s.replace(
    marker,
    "        if not await self.wechat_groups.admit(event):\n            event.stop_event()\n            return\n\n"
    + marker,
)
t = main.read_text()
t = t.replace(
    "\nfrom .group_chat_context import GroupChatContext\n",
    "from astrbot.core.wechat_group_policy import APPLIED, apply_group_background\n\nfrom .group_chat_context import GroupChatContext\n",
    1,
)
for marker in [
    '        """处理只有一个 @ 或仅有唤醒前缀的消息，并等待用户下一条内容。"""\n',
    '        """群聊上下文感知"""\n',
]:
    assert t.count(marker) == 1
    t = t.replace(
        marker,
        marker + "        if event.get_extra(APPLIED, False):\n            return\n",
        1,
    )
marker = '        """在请求 LLM 前注入人格信息、Identifier、时间、回复内容等 System Prompt"""\n'
assert t.count(marker) == 1
t = t.replace(
    marker,
    marker + "        if apply_group_background(event, req):\n            return\n",
    1,
)
compile(s, str(stage), "exec")
compile(t, str(main), "exec")
stage.write_text(s)
main.write_text(t)
shutil.copy2(
    here / "wechat_group_policy.py", root / "astrbot/core/wechat_group_policy.py"
)
print(
    "installed_wechat_group_policy",
    hashlib.sha256((here / "wechat_group_policy.py").read_bytes()).hexdigest(),
)
