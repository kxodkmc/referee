Xiaomi MiMo API 兼容 OpenAI API
BASE_URL OpenAI 兼容协议：https://api.xiaomimimo.com/v1
模型列表
mimo-v2.5-pro	
文本生成
深度思考
流式输出
函数调用
结构化输出
联网搜索
上下文窗口：1M
最大输出：128K
mimo-v2.5	
文本生成
全模态理解（音频、视频、图片）
深度思考
流式输出
函数调用
结构化输出
联网搜索
上下文窗口：1M
最大输出：128K
在请求中设置 thinking.type 参数控制深度思考开关：enabled 开启思考或 disabled 关闭思考。思考默认开启
多轮对话回传要求
在 Agent 类产品的多轮会话中开启深度思考，且历史会话中存在工具调用时，后续所有 user 交互轮次中回传的 assistant 如果包含了工具调用，必须完整回传 reasoning_content 字段，否则 API 将返回 400 错误。正确回传方式请参考调用示例的“深度思考下的多轮工具调用”。
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.environ.get("MIMO_API_KEY"),
    base_url="https://api.xiaomimimo.com/v1"
)

completion = client.chat.completions.create(
    model="mimo-v2.5-pro",
    messages=[
        {
            "role": "system",
            "content": "You are MiMo, an AI assistant developed by Xiaomi. Today is date: Tuesday, December 16, 2025. Your knowledge cutoff date is December 2024."
        },
        {
            "role": "user",
            "content": "Give me some tips for improving work efficiency."
        }
    ],
    max_completion_tokens=1024,
    stream=True,
    extra_body={
        "thinking": {"type": "enabled"}
    }
)

for chunk in completion:
    print(chunk.model_dump_json())
响应
data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":"","role":"assistant","tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"created":1781234029,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":"The user is asking"},"finish_reason":null,"index":0}],"created":1781234029,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":" for tips on"},"finish_reason":null,"index":0}],"created":1781234029,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":" improving work efficiency."},"finish_reason":null,"index":0}],"created":1781234029,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

...

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":", etc"},"finish_reason":null,"index":0}],"created":1781234030,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":"."},"finish_reason":null,"index":0}],"created":1781234030,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":"# Tips","role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"created":1781234030,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":" for Improving Work","role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"created":1781234030,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

...

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":" on any specific","role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"created":1781234037,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":" area?","role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"created":1781234037,"model":"mimo-v2.5-pro","object":"chat.completion.chunk"}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":"stop","index":0}],"created":1781234037,"model":"mimo-v2.5-pro","object":"chat.completion.chunk","usage":null}

data: {"id":"4e57d676fe464c09aa2f27fa652abc40","choices":[],"created":1781234037,"model":"mimo-v2.5-pro","object":"chat.completion.chunk","usage":{"completion_tokens":339,"prompt_tokens":61,"total_tokens":400,"completion_tokens_details":{"reasoning_tokens":41},"prompt_tokens_details":{}}}

data: [DONE]
在深度思考的多轮对话中，若涉及工具调用，回传 reasoning_content 可确保思考连续性，提升模型输出质量。
import os
import json
from openai import OpenAI

# Initialize client
client = OpenAI(
    api_key=os.environ.get("MIMO_API_KEY"),
    base_url="https://api.xiaomimimo.com/v1"
)

# Define tools
tools = [
    {
        "type": "function",
        "function": {
            "name": "get_current_weather",
            "description": "Get the current weather for a given city",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {"type": "string", "description": "City name, e.g. Beijing"},
                    "unit": {"type": "string", "enum": ["celsius", "fahrenheit"]}
                },
                "required": ["location"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_time",
            "description": "Get the current time in a given timezone",
            "parameters": {
                "type": "object",
                "properties": {
                    "timezone": {"type": "string", "description": "Timezone, e.g. Asia/Shanghai"}
                },
                "required": ["timezone"]
            }
        }
    }
]

# Tool execution functions (replace with real API calls in production)
def get_current_weather(location: str, unit: str = "celsius") -> str:
    weather_data = {"Beijing": "Sunny 25°C", "Shanghai": "Cloudy 22°C", "Shenzhen": "Rainy 28°C"}
    return weather_data.get(location, f"Weather unknown for {location}")

def get_time(timezone: str) -> str:
    from datetime import datetime
    return datetime.now().strftime(f"%Y-%m-%d %H:%M:%S ({timezone})")

TOOL_MAP = {
    "get_current_weather": lambda **kw: get_current_weather(**kw),
    "get_time": lambda **kw: get_time(**kw)
}

def run_turn(messages, turn_num):
    """Execute a single user turn: call model, run tools in a loop until final answer."""
    request_num = 0
    while True:
        request_num += 1
        print(f"\nRequest {turn_num}-{request_num}:")

        response = client.chat.completions.create(
            model="mimo-v2.5-pro",
            messages=messages,
            tools=tools,
            extra_body={"thinking": {"type": "enabled"}}
        )

        assistant_message = response.choices[0].message
        messages.append(assistant_message)

        # Print full model response
        print(f"reasoning_content: {assistant_message.reasoning_content}")
        print(f"content: \"{assistant_message.content}\"")
        print(f"tool_calls: {assistant_message.tool_calls}")

        # If no tool calls, we have the final answer
        if not assistant_message.tool_calls:
            break

        # Execute each tool call and append results
        for tool_call in assistant_message.tool_calls:
            func_name = tool_call.function.name
            func_args = json.loads(tool_call.function.arguments)
            result = TOOL_MAP[func_name](**func_args)

            print(f"-> Tool result [{func_name}]: {result}")
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": result
            })

# --- Multi-turn conversation ---
messages = []

# Turn 1
print("=== Turn 1 ===")
messages.append({"role": "user", "content": "How is the weather in Beijing today? What time is it now?"})
run_turn(messages, turn_num=1)

# Turn 2: reasoning_content from Turn 1 is already in messages via assistant_message
print("\n=== Turn 2 ===")
messages.append({"role": "user", "content": "How about Shanghai? And is it hotter or colder than Beijing?"})
run_turn(messages, turn_num=2)