import json
import random
import string
import ast
from langchain_core.messages import HumanMessage, ToolMessage, SystemMessage, AIMessage
from langchain_community.chat_models import ChatDatabricks


class ChatDatabricks_ToolConverter(ChatDatabricks):
  def predict_messages(self, messages, tools = []):
    p = ToolParser()
    new_messages = p.tools_to_human_ai(messages, tools)
    
    return p.format_response(ChatDatabricks.predict_messages(self, new_messages).content)



class ToolParser():
  def __init__(self):

    #self.input = input
    #self.tools = input.get('tools', [])
    #self.messages = input['messages']

    self.DEFAULT_INSTRUCTION = 'You are a helpful, respectful and honest assistant. Always answer as helpfully as possible, while being safe. Your answers should not include any harmful, unethical, racist, sexist, toxic, dangerous, or illegal content. Please ensure that your responses are socially unbiased and positive in nature.\n\nIf a question does not make any sense, or is not factually coherent, explain why instead of answering something not correct. If you don\'t know the answer to a question, please don\'t share false information.'

  def format_response(self, response):
    #print(f'LLM Response: {response}')

    #clean up the response
    response = response.strip()
    cnt = 0
    while '\_' in response:
      response = response.replace('\_', '_')
      cnt+=1
      if cnt > 100:
        break
    
    if not response.startswith('<!tool:'):
      if '<!tool:output|||content:' in response:
        response = response.replace('<!tool:output|||content:', '').replace('!>', '')
      return AIMessage(content=response)
    elif '!>' not in response:
      raise Exception(f'Invalid LLM output: {response}')
    
    start_tool = response.index('<!') + 2    
    end_tool = response.index('!>')

    tool_call = response[start_tool:end_tool].split('|||')

    tool_name = tool_call[0].split(':')[1]

    args = {}

    for arg in tool_call[1:]:
      colon = arg.index(':')
      args[arg[:colon]] = arg[colon+1:]

    if tool_name == 'output':
      return AIMessage(content=args['content'])
    else:
      kwargs =  {
        "tool_calls": [
          {
            "id": "call_" + ''.join(random.choices(string.ascii_lowercase, k=12)),
            "type": "function",
            "function": {
              "name": tool_name,
              "arguments": json.dumps(args)
            }                
          }
        ]
      }
      return AIMessage(content='', additional_kwargs = kwargs)

  def tools_to_human_ai(self, messages, tools):
    messages = [msg.copy() for msg in messages]
    new_messages = []
    if tools is None:
      tools = []

    system_starter = messages[0].content if messages[0].type == 'system' else self.DEFAULT_INSTRUCTION
    new_messages.append(SystemMessage(content = self.__system_message(system_starter, tools)))

    for m in messages:
      if m.type == 'human':
        new_messages.append(HumanMessage(content = m.content))
      elif m.type == 'tool':
        new_messages.append(self.__parse_tool(m))
      elif m.type == 'ai':
        new_messages.append(self.__parse_assistant(m))

    print(new_messages)
    return new_messages

    
  #def __assistant_starter(self):
  #  return  '```json\n{\n\t"tool_name": "'

  def __system_message(self, system, tools):

    system += self.__tool_system_message(tools)
    #print(f'System Message: {system}')

    return system
  
  def __parse_assistant(self, message):
    content = message.content
    tool_calls = message.additional_kwargs.get('tool_calls', [])
    if len(content) > 0: #return regular content
      return AIMessage(content = f"<!tool:output|||content:{content.strip()}!>")
      #return f' {content.strip()} {self.eos_token}'
      ret_obj = {"tool_name": "response", "arguments": {"content": content.strip()}}
      #return ' ```json\n{\n\t"tool_name": "response",\n\t"arguments": {"content": "' + content.strip() + '"}\n}\n``` ' + self.eos_token
    elif len(tool_calls) > 0: #return tool content
      tool_name = tool_calls[0]["function"]["name"]
      arguments = json.loads(tool_calls[0]["function"]["arguments"])
      #ret_obj = {"tool_name": tool_calls[0]["function"]["name"], "arguments": json.loads(tool_calls[0]["function"]["arguments"])}

      #return ' ```json\n{\n\t"tool_name": "' + tool_calls[0]["function"]["name"] + '",\n\t"arguments": ' + str(json.loads(tool_calls[0]["function"]["arguments"])) + '\n}\n``` ' + self.eos_token
    else:
      print(content)
      raise Exception("Invalid Assistant Message.")

    arg_str = ""
    for k in arguments.keys():
      arg_str += f"|||{k}:{arguments[k]}"
    ret_str = f"<!tool:{tool_name}{arg_str}!>"

    '''
    ret_str = '```json\n'
    ret_str += json.dumps(ret_obj).replace('"tool_name"', '\n\t"tool_name"').replace(' "arguments"', '\n\t"arguments"')[:-1] #format and remove the last character - }
    ret_str += '\n}\n```'
    '''
    return AIMessage(content = ret_str)
     
  def __parse_tool(self, message):
    #msg = message.to_json()
    
    return HumanMessage(content=f"<!tool:{message.name}|||response:{message.content}!>")
    
    '''
    content = {
      #"id": msg['tool_call_id'],
      "tool_name": message.name,
      "response": message.content
    }
    return HumanMessage(content = json.dumps(content))
    '''
    

  def __tool_to_str(self, tool):
    function = tool['function']
    ret = ""
    ret += f"Tool Name: {function['name']}\n"
    ret += f"Description: {function.get('description', '')}"
    if 'parameters' in function.keys() and 'properties' in function['parameters'].keys():
      ret += '\nArguments:'
      if 'required' in function['parameters'].keys():
        required = function['parameters']['required']
      prop = function['parameters']['properties']
      for arg in prop.keys():
        ret += f'\n name: {arg} '
        ret += '(required)' if arg in required else '(optional)'
        for x in prop[arg].keys():
          ret += f"\n\t{x}: {prop[arg][x]}"

    return ret

  def __tool_system_message(self, tools):

    tool_list = '\n\n'.join([self.__tool_to_str(tool) for tool in tools])

    tool_str = """
You are capable of using a variety of tools to answer a question or accomplish a task. Here are the tools available to you:

{tool_list}

Tool Name: helper_x--output
Description: Use this tool to respond to the user.
Arguments:
  name: content  (required)
	type: string
	description: The message to send to the user.
 
You must use these tools to respond. Your response must have the following format with the:

<!tool:tool_name|||first_argument_name:first_argument_value|||nth_argument_name:nth_argument_value!>

For example, to use a tool called "helper_0--find_product" with arguments "first_int" and "second_int", to answer the prompt "what is 8 times 10?" you would respond with:

<!tool:helper_0--find_product|||first_int:8|||second_int:10!>

When responding to the user, you must use this format, using the output tool! If you'd like to ask how the user is doing you must write:

<!tool:helper_x--output|||content:How are you today?!>

Remember, you must use one of the tools specified above, and you must start your response with <!tool:"""

    return tool_str.format(tool_list=tool_list)
 
