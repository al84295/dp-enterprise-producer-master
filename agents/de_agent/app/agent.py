"""Tool-calling loop for the data engineering agent.

Uses the OpenAI-compatible Chat Completions API that Databricks Model Serving exposes, so any chat model
served on the workspace (for example a Claude or open model endpoint) can be swapped in by name.
"""
import json
import os

from prompts import SYSTEM_PROMPT
from tools import TOOL_SCHEMAS

MAX_STEPS = int(os.getenv("DE_AGENT_MAX_STEPS", "10"))
MAX_TOOL_CHARS = 12000


def databricks_client():
    """OpenAI client pointed at this workspace's serving endpoints, authenticated as the app."""
    from databricks.sdk.core import Config
    from openai import OpenAI
    cfg = Config()
    token = cfg.authenticate()["Authorization"].split(" ", 1)[1]
    return OpenAI(api_key=token, base_url=f"{cfg.host.rstrip('/')}/serving-endpoints")


def _as_dict(msg):
    """Assistant message -> plain dict we can send back next turn."""
    d = {"role": "assistant", "content": msg.content or ""}
    if getattr(msg, "tool_calls", None):
        d["tool_calls"] = [{"id": tc.id, "type": "function",
                            "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                           for tc in msg.tool_calls]
    return d


def run(history, toolbox, client, model, on_event=None, max_steps=MAX_STEPS):
    """Run the agent on a conversation. history = list of {"role", "content"} user/assistant turns.

    Returns the final answer text. on_event(kind, payload) streams progress: "tool_call", "tool_result".
    """
    emit = on_event or (lambda *_: None)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + list(history)
    for _ in range(max_steps):
        resp = client.chat.completions.create(model=model, messages=messages, tools=TOOL_SCHEMAS,
                                              tool_choice="auto", temperature=0.1, max_tokens=4000)
        msg = resp.choices[0].message
        messages.append(_as_dict(msg))
        calls = getattr(msg, "tool_calls", None) or []
        if not calls:
            return msg.content or ""
        for tc in calls:
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
                result = {"error": "Tool arguments were not valid JSON."}
            else:
                emit("tool_call", {"name": tc.function.name, "args": args})
                result = toolbox.call(tc.function.name, args)
            emit("tool_result", {"name": tc.function.name, "result": result})
            text = json.dumps(result, default=str)
            if len(text) > MAX_TOOL_CHARS:
                text = text[:MAX_TOOL_CHARS] + ' ... [truncated]'
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": text})
    return "I stopped after the maximum number of steps. Here is what I have so far; ask me to continue."
