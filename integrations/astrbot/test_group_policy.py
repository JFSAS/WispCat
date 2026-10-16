"""Synthetic checks; never start a provider or a real messaging adapter."""

# ruff: noqa: E402

import copy
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

bootstrap_root = tempfile.TemporaryDirectory(prefix="astrbot-group-check-")
os.environ["ASTRBOT_ROOT"] = bootstrap_root.name
os.environ["ASTRBOT_DISABLE_METRICS"] = "1"

from astrbot.api.message_components import At, AtAll, Image, Plain, Reply
from astrbot.core.config.default import DEFAULT_CONFIG
from astrbot.core.platform.message_type import MessageType
from astrbot.core.wechat_group_policy import (
    APPLIED,
    SNAPSHOT,
    WeChatGroupPolicy,
    apply_group_background,
)
from astrbot.core.pipeline.waking_check.stage import WakingCheckStage
from astrbot.builtin_stars.astrbot.main import Main


class Event:
    def __init__(self, seq, text="", parts=None, group="group-a", sender="10001"):
        self._extras = {}
        self.message_str = text
        self.message_obj = SimpleNamespace(
            message_id=str(seq), timestamp=123, type=MessageType.GROUP_MESSAGE
        )
        self.parts = parts if parts is not None else [Plain(text)]
        self.group, self.sender = group, sender
        self.role = "member"
        self.stopped = self.is_wake = self.is_at_or_wake_command = False

    def get_platform_name(self):
        return "aiocqhttp"

    def get_platform_id(self):
        return "wechat-onebot"

    def get_message_type(self):
        return self.message_obj.type

    def get_self_id(self):
        return "10000"

    def get_sender_id(self):
        return self.sender

    def get_sender_name(self):
        return "synthetic-sender"

    def get_group_id(self):
        return self.group

    def get_messages(self):
        return self.parts

    def get_message_outline(self):
        return self.message_str or "[图片]"

    def set_extra(self, key, value):
        self._extras[key] = value

    def get_extra(self, key=None, default=None):
        return self._extras.get(key, default) if key else self._extras

    def stop_event(self):
        self.stopped = True

    def is_private_chat(self):
        return self.get_message_type() == MessageType.FRIEND_MESSAGE

    @property
    def unified_msg_origin(self):
        return "wechat-onebot:GroupMessage:" + self.group


class GroupPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.config = copy.deepcopy(DEFAULT_CONFIG)
        self.config["provider_ltm_settings"]["group_icl_enable"] = True
        self.config["provider_ltm_settings"]["group_message_max_cnt"] = 1000
        self.path = Path(self.temp.name) / "pending.db"
        self.policy = WeChatGroupPolicy(self.config, self.path)

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def test_only_explicit_at_and_private_commands(self):
        stage = WakingCheckStage()
        await stage.initialize(
            SimpleNamespace(
                astrbot_config=self.config, db_helper=None, astrbot_config_id="default"
            )
        )
        stage.wechat_groups = self.policy
        stage._umo_auto_name_recorder = SimpleNamespace(schedule=lambda _: None)
        for seq, text, parts in [
            (1, "hello", [Plain("hello")]),
            (2, "/sid", [Plain("/sid")]),
            (3, "all", [AtAll()]),
            (4, "other", [At(qq="10002")]),
            (5, "quoted", [Reply(id="q", sender_id="10000")]),
        ]:
            event = Event(seq, text, parts)
            await stage.process(event)
            self.assertTrue(event.stopped)
            self.assertFalse(event.is_at_or_wake_command)
        event = Event(6, "question", [At(qq="10000"), Plain("question")])
        await stage.process(event)
        self.assertFalse(event.stopped)
        self.assertTrue(event.is_at_or_wake_command)
        self.assertEqual(len(event.get_extra(SNAPSHOT)), 5)
        private = Event(7, "/sid")
        private.message_obj.type = MessageType.FRIEND_MESSAGE
        await stage.process(private)
        self.assertFalse(private.stopped)
        self.assertTrue(private.is_at_or_wake_command)

    async def test_exact_limit_snapshot_and_restart(self):
        for seq in range(1001):
            self.assertFalse(await self.policy.admit(Event(seq, str(seq))))
        trigger = Event(1002, "question", [At(qq="10000")])
        self.assertTrue(await self.policy.admit(trigger))
        frozen = trigger.get_extra(SNAPSHOT)
        self.assertEqual(len(frozen), 1000)
        self.assertEqual(frozen[0]["message"], "1")
        self.assertEqual(frozen[-1]["message"], "1000")
        await self.policy.admit(Event(1003, "after-trigger"))
        request = SimpleNamespace(extra_user_content_parts=[])
        self.assertTrue(apply_group_background(trigger, request))
        self.assertTrue(apply_group_background(trigger, request))
        self.assertEqual(len(request.extra_user_content_parts), 1)
        self.assertNotIn("after-trigger", request.extra_user_content_parts[0].text)
        restarted = WeChatGroupPolicy(self.config, self.path)
        self.assertFalse(await restarted.admit(trigger))
        next_trigger = Event(1004, "next", [At(qq="10000")])
        self.assertTrue(await restarted.admit(next_trigger))
        self.assertEqual(
            [r["message"] for r in next_trigger.get_extra(SNAPSHOT)], ["after-trigger"]
        )

    async def test_group_scope_self_echo_and_images(self):
        await self.policy.admit(Event(1, "a"))
        await self.policy.admit(Event(2, "b", group="group-b"))
        await self.policy.admit(Event(3, "self-echo", sender="10000"))
        image = Event(4, parts=[Image(file="synthetic.png")])
        self.assertFalse(await self.policy.admit(image))
        trigger = Event(5, "question", [At(qq="10000"), Image(file="synthetic.png")])
        self.assertTrue(await self.policy.admit(trigger))
        self.assertEqual(
            [r["message"] for r in trigger.get_extra(SNAPSHOT)], ["a", "[图片]"]
        )
        self.assertEqual(len(trigger.get_messages()), 2)
        self.assertTrue(trigger.get_extra(APPLIED))

    async def test_empty_at_has_no_followup_bypass_and_byte_limit(self):
        for seq in range(100):
            await self.policy.admit(Event(seq, "x" * 16000))
        trigger = Event(101, parts=[At(qq="10000")])
        self.assertTrue(await self.policy.admit(trigger))
        self.assertLess(len(trigger.get_extra(SNAPSHOT)), 100)
        self.assertTrue(trigger.message_str)
        main = Main.__new__(Main)
        self.assertEqual([r async for r in main.handle_empty_mention(trigger)], [])
        self.assertEqual([r async for r in main.on_message(trigger)], [])
        request = SimpleNamespace(extra_user_content_parts=[])
        await main.decorate_llm_req(trigger, request)
        self.assertEqual(len(request.extra_user_content_parts), 1)


if __name__ == "__main__":
    unittest.main()
