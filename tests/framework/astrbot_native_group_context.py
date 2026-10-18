"""Check upstream group image context without patching AstrBot or calling a model.

Run with the pinned AstrBot source on PYTHONPATH and its isolated Python env.
"""

import asyncio
import copy
import os
import tempfile
from types import SimpleNamespace


async def check() -> None:
    from astrbot.api.message_components import At, Image, Plain
    from astrbot.builtin_stars.astrbot.group_chat_context import GroupChatContext
    from astrbot.core.config.default import DEFAULT_CONFIG
    from astrbot.core.platform.message_type import MessageType

    class Event:
        def __init__(self, parts):
            self.parts = parts
            self.extra = {}
            self.unified_msg_origin = "aiocqhttp:GroupMessage:synthetic"
            self.message_obj = SimpleNamespace(
                sender=SimpleNamespace(nickname="synthetic-user")
            )

        def get_message_type(self):
            return MessageType.GROUP_MESSAGE

        def get_self_id(self):
            return "10000"

        def get_messages(self):
            return self.parts

        def set_extra(self, key, value):
            self.extra[key] = value

        def get_extra(self, key, default=None):
            return self.extra.get(key, default)

    config = copy.deepcopy(DEFAULT_CONFIG)
    config["provider_ltm_settings"].update(
        group_icl_enable=True,
        group_message_max_cnt=1000,
        image_caption=True,
        image_caption_provider_id="synthetic-vision",
    )
    context = SimpleNamespace(get_config=lambda **kwargs: config)
    history = GroupChatContext(None, context)
    calls = []

    async def caption(reference, provider, prompt):
        calls.append((reference, provider))
        return "synthetic image: a red square"

    history.get_image_caption = caption
    await history.handle_message(Event([Image(file="synthetic-image.png")]))
    trigger = Event([At(qq="10000"), Plain("what is in that earlier picture?")])
    await history.handle_message(trigger)
    await history.handle_message(Event([Plain("synthetic later message")]))
    request = SimpleNamespace(extra_user_content_parts=[])
    await history.on_req_llm(trigger, request)
    assert calls == [("synthetic-image.png", "synthetic-vision")]
    assert len(request.extra_user_content_parts) == 1
    text = request.extra_user_content_parts[0].text
    assert "a red square" in text and "synthetic later message" not in text
    assert len(history.raw_records[trigger.unified_msg_origin]) == 1
    assert "synthetic later message" in history.raw_records[trigger.unified_msg_origin][0]

    calls.clear()
    config["provider_ltm_settings"]["image_caption"] = False
    await history.handle_message(Event([Image(file="synthetic-image.png")]))
    assert not calls
    print("PASS: native prior-image caption, request boundary, concurrent later-message retention, caption toggle; live provider calls=0")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="wispcat-native-group-check-") as root:
        os.environ["ASTRBOT_ROOT"] = root
        os.environ["ASTRBOT_DISABLE_METRICS"] = "1"
        asyncio.run(check())
