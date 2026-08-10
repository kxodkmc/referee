DeepSeek API 使用
base_url (OpenAI):https://api.deepseek.com
model
deepseek-v4-flash
deepseek-v4-pro
双模型均是
上下文长度	1M
输出长度	最大 384K
价格（左Flash，右Pro）
百万tokens输入（缓存命中）	0.02元	0.025元
百万tokens输入（缓存未命中）	1元	3元
百万tokens输出	2元	6元

在调用 DeepSeek API 时，可能会遇到以下错误。这里列出了相关错误的原因及其解决方法。
错误码	描述
400 - 格式错误	原因：请求体格式错误
解决方法：请根据错误信息提示修改请求体
401 - 认证失败	原因：API key 错误，认证失败
解决方法：请检查您的 API key 是否正确，如没有 API key，请先 创建 API key
402 - 余额不足	原因：账号余额不足
解决方法：请确认账户余额，并前往 充值 页面进行充值
422 - 参数错误	原因：请求体参数错误
解决方法：请根据错误信息提示修改相关参数
429 - 请求速率达到上限	原因：请求速率（TPM 或 RPM）达到上限
解决方法：请合理规划您的请求速率。
500 - 服务器故障	原因：服务器内部故障
解决方法：请等待后重试。若问题一直存在，请联系我们解决
503 - 服务器繁忙	原因：服务器负载过高
解决方法：请稍后重试您的请求

思考模式开关与思考强度控制
思考模式开关{"thinking": {"type": "enabled/disabled"}}
思考强度控制{"reasoning_effort": "low/high/max"}
思考模式默认打开，且 effort 默认为 high
在 OpenAI SDK 中使用 Chat Completion 设置 thinking 参数时，需要将 thinking 参数传入 extra_body 中：

response = client.chat.completions.create(
  model="deepseek-v4-pro",
  # ...
  reasoning_effort="high",
  extra_body={"thinking": {"type": "enabled"}}
)
在每一轮对话过程中，模型会输出思维链内容（reasoning_content）和最终回答（content）。如果没有工具调用，则在下一轮对话中，之前轮输出的思维链内容不会被拼接到上下文中，如代码所示：
from openai import OpenAI
client = OpenAI(api_key="<DeepSeek API Key>", base_url="https://api.deepseek.com")

# Turn 1
messages = [{"role": "user", "content": "9.11 and 9.8, which is greater?"}]
response = client.chat.completions.create(
    model="deepseek-v4-pro",
    messages=messages,
    stream=True,
    reasoning_effort="high"
    extra_body={"thinking": {"type": "enabled"}},
)

reasoning_content = ""
content = ""

for chunk in response:
    if chunk.choices[0].delta.reasoning_content:
        reasoning_content += chunk.choices[0].delta.reasoning_content
    else:
        content += chunk.choices[0].delta.content

# Turn 2
# The reasoning_content will be ignored by the API
messages.append({"role": "assistant", "reasoning_content": reasoning_content, "content": content})
messages.append({'role': 'user', 'content': "How many Rs are there in the word 'strawberry'?"})
response = client.chat.completions.create(
    model="deepseek-v4-pro",
    messages=messages,
    stream=True,
    reasoning_effort="high"
    extra_body={"thinking": {"type": "enabled"}},
)

DeepSeek 模型的思考模式支持工具调用功能。模型在输出最终答案之前，可以进行多轮的思考与工具调用，以提升答案的质量。其调用模式如下
请注意，携带了 tools 参数的请求，在后续所有请求中，必须完整回传 reasoning_content 给 API。若您的代码中未正确回传 reasoning_content，API 会返回 400 报错
import os
import json
from openai import OpenAI
from datetime import datetime

# The definition of the tools
tools = [
    {
        "type": "function",
        "function": {
            "name": "get_date",
            "description": "Get the current date",
            "parameters": { "type": "object", "properties": {} },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get weather of a location, the user should supply the location and date.",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": { "type": "string", "description": "The city name" },
                    "date": { "type": "string", "description": "The date in format YYYY-mm-dd" },
                },
                "required": ["location", "date"]
            },
        }
    },
]

# The mocked version of the tool calls
def get_date_mock():
    return datetime.now().strftime("%Y-%m-%d")

def get_weather_mock(location, date):
    return "Cloudy 7~13°C"

TOOL_CALL_MAP = {
    "get_date": get_date_mock,
    "get_weather": get_weather_mock
}

def run_turn(turn, messages):
    sub_turn = 1
    while True:
        response = client.chat.completions.create(
            model='deepseek-v4-pro',
            messages=messages,
            tools=tools,
            reasoning_effort="high",
            extra_body={ "thinking": { "type": "enabled" } },
        )
        messages.append(response.choices[0].message)
        reasoning_content = response.choices[0].message.reasoning_content
        content = response.choices[0].message.content
        tool_calls = response.choices[0].message.tool_calls
        print(f"Turn {turn}.{sub_turn}\n{reasoning_content=}\n{content=}\n{tool_calls=}")
        # If there is no tool calls, then the model should get a final answer and we need to stop the loop
        if tool_calls is None:
            break
        for tool in tool_calls:
            tool_function = TOOL_CALL_MAP[tool.function.name]
            tool_result = tool_function(**json.loads(tool.function.arguments))
            print(f"tool result for {tool.function.name}: {tool_result}\n")
            messages.append({
                "role": "tool",
                "tool_call_id": tool.id,
                "content": tool_result,
            })
        sub_turn += 1
    print()

client = OpenAI(
    api_key=os.environ.get('DEEPSEEK_API_KEY'),
    base_url=os.environ.get('DEEPSEEK_BASE_URL'),
)

# The user starts a question
turn = 1
messages = [{
    "role": "user",
    "content": "How's the weather in Hangzhou Tomorrow"
}]
run_turn(turn, messages)

# The user starts a new question
turn = 2
messages.append({
    "role": "user",
    "content": "How's the weather in Guangzhou Tomorrow"
})
run_turn(turn, messages)
DeepSeek API 上下文硬盘缓存技术对所有用户默认开启，用户无需修改代码即可享用。

用户的每一个请求都会触发硬盘缓存的构建。若后续请求与之前的请求在前缀上存在重复，则重复部分只需要从缓存中拉取，计入“缓存命中”。
例一：多轮对话
第一次请求

messages: [
    {"role": "system", "content": "你是一位乐于助人的助手"},
    {"role": "user", "content": "中国的首都是哪里？"}
]

第二次请求

messages: [
    {"role": "system", "content": "你是一位乐于助人的助手"},
    {"role": "user", "content": "中国的首都是哪里？"},
    {"role": "assistant", "content": "中国的首都是北京。"},
    {"role": "user", "content": "美国的首都是哪里？"}
]

在上例中，第二次请求可以完整复用第一次请求的缓存前缀单元，这部分会计入“缓存命中”。

例二：长文本问答
第一次请求

messages: [
    {"role": "system", "content": "你是一位资深的财报分析师..."}
    {"role": "user", "content": "<财报内容>\n\n请总结一下这份财报的关键信息。"}
]

第二次请求

messages: [
    {"role": "system", "content": "你是一位资深的财报分析师..."}
    {"role": "user", "content": "<财报内容>\n\n请分析一下这份财报的盈利情况。"}
]

第三次请求

messages: [
    {"role": "system", "content": "你是一位资深的财报分析师..."}
    {"role": "user", "content": "<财报内容>\n\n请分析一下公司收入与支出占比。"}
]

在上例中，前两次请求不会命中缓存。前两次请求完成后，系统会识别出 system 消息 + user 消息中的<财报内容>为缓存前缀单元，并进行落盘。在第三次请求中，由于完整匹配了前面落盘的缓存前缀单元，则可命中缓存。

查看缓存命中情况
在 DeepSeek API 的返回中，我们在 usage 字段中增加了两个字段，来反映请求的缓存命中情况：

prompt_cache_hit_tokens：本次请求的输入中，缓存命中的 tokens 数

prompt_cache_miss_tokens：本次请求的输入中，缓存未命中的 tokens 数