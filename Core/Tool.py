try:
  from databricks.sdk.runtime import *
except ImportError:
    # alternative solution
  pass

import inspect
import os
import openai
import json
import uuid
import datetime
#from FunctionDefiner import ChatFunctionBase
from langchain_openai import ChatOpenAI
from Core.ChatConverter import ChatDatabricks_ToolConverter
from langchain_core.messages import HumanMessage, AIMessage, ChatMessage, SystemMessage, ToolMessage
#from langchain.tools import format_tool_to_openai_function, YouTubeSearchTool, MoveFileTool

class ChatBot():

    __OPENAI_KEY = os.getenv("OPENAI_API_KEY")
    '''
    __BASE_TOOLS = [
            {
                "type": "function",
                "function": {
                    "name": "question",
                    "description": "Ask a clarifying question to request additional information to clear up ambiguity.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "q": {
                                "type": "string",
                                "description": "The question you want answered.",
                            },
                        },
                        "required": ["q"],
                    },
                },
            }
        ]
    '''
    
    def __init__(self, config = {}, helpers = []):
        instruction_prompt = config.get('instruction', "You are an assistant.")
        model = config.get('model', "gpt-4-turbo-preview")
        output_table = config.get('output_table', "")
        provider = config.get('provider', 'databricks')
        endpoint_type = config.get('endpoint-type', 'chat') #chat-basic, completions, embeddings

        if model == '3.5':
            model = "gpt-3.5-turbo"
        elif model == '4':
            model = "gpt-4-turbo-preview"
        elif model == 'mixtral':
            model = 'databricks-mixtral-8x7b-instruct'
            endpoint_type = 'chat-basic' 
        elif model == 'llama2':
            model = 'databricks-llama-2-70b-chat'
            endpoint_type = 'chat-basic' 
        elif model == 'dbrx' or model == 'databricks-dbrx-instruct':
            model = 'databricks-dbrx-instruct'
            endpoint_type = 'chat-basic' 
        self.INSTRUCTION_PROMPT = instruction_prompt
        self.model = model
        self.functions = []#self.__BASE_FUNCTION.copy()
        self.output_table = output_table
        self.endpoint_type = endpoint_type
        
        self.helpers = []
        for h in helpers:
            self.__add_helper(h)
        
        #openai.api_key = os.getenv('OPENAI_API_KEY', self)

        self.__initialize()

    def __add_helper(self, helper):
        ind = len(self.helpers)
        if helper != None:
            self.helpers.append(helper)
            for f in self.function_definitions(helper, f'helper_{ind}--'):
              self.functions.append(f)
        #print(self.functions)

    def __initialize(self):
        self.messages = []
        self.conversation = []
        self.conversation_id = str(uuid.uuid4())
        if self.INSTRUCTION_PROMPT != "":
            self.__add_message(SystemMessage(content=self.INSTRUCTION_PROMPT))
        #TODO: set this up to handle open ai, azureopenai and external models on databricks.
        if self.endpoint_type == 'chat-basic':
            self.llm = ChatDatabricks_ToolConverter(target_uri="databricks", endpoint=self.model, temperature=0.2)
        else:
            self.llm = ChatOpenAI(model = self.model, openai_api_key=self.__OPENAI_KEY)

    def __str__(self):
        ret = ""
        for msg in self.messages:
            ret += f"{msg.type}:\t{msg.content}"
        return ret

    def __add_message(self, message):
      self.messages.append(message)
      self.conversation.append({'conversation_id': self.conversation_id, 
                        'ordinal_position': len(self.messages),
                        'raw_message': message,
                        'output_message': self.__translate_message(message),
                        'created_date': datetime.datetime.now(),
                        'logged': False})

    def __log_conversation(self):
      if self.output_table and spark is not None:
        df = spark.createDataFrame(self.conversation).where('logged == False').drop('logged')
        df.write.mode('append').saveAsTable(self.output_table)
        for m in self.conversation:
          m['logged'] = True

    def run_thread(self, messages):
        self.__initialize()
        for msg in messages:
            self.__parse_msg(msg)
        return self.__submit_conversation()

    def __parse_msg(self, msg):
        tp = msg.get('type', 'user')
        role = msg.get('role', tp)

        if role == 'human' or role == 'user':
            self.__add_message(HumanMessage(content=msg.get('content', '')))
        elif role == 'ai' or role == 'assistant':
            tools = msg.get('tool_calls', [])
            tool_calls = { 'tool_calls' : tools } if len(tools) > 0 else {}
            kwargs = msg.get('additional_kwargs', tool_calls)
            self.__add_message(AIMessage(content = msg.get('content', ''), 
                                       additional_kwargs = kwargs
                                       )
                             )
        elif role == 'tool':
            self.__add_message(ToolMessage(content = msg.get('content', ''), 
                                         name = msg.get('name', ''), 
                                         tool_call_id = msg.get('tool_call_id', '')
                                         )
                             )
            
    def output_thread(self):
        return [self.__translate_message(msg) for msg in self.messages]
    
    def __translate_message(self, msg):
        if msg.type == 'system':
            #system
            return {
                'role': 'system',
                'content': msg.content
            }
        elif msg.type == 'human':
            #user
            return {
                'role': 'user',
                'content': msg.content
            }
        elif msg.type == 'ai':
            #assistant
            ret = {
                'role': 'assistant',
                'content': msg.content
            }
            tools = msg.additional_kwargs.get('tool_calls', None)
            if tools:
                ret['tool_calls'] = tools
            return ret
        elif msg.type == 'tool':
            #tool
            return {
                "role": "tool",
                "tool_call_id": msg.tool_call_id,
                "name": msg.name,
                "content": msg.content
            }

        return {}


    def prompt(self, prompt):
        """Continue a conversation"""
        self.__add_message(HumanMessage(content=prompt))
        return self.__submit_conversation()
    
    def reset(self, instruction_prompt = "default"):
        """Clear the old conversation and start a new conversation."""
        if instruction_prompt != "default":
            self.INSTRUCTION_PROMPT = instruction_prompt
        self.__initialize()


    def __submit_conversation(self):
        response = self.llm.predict_messages(self.messages, tools = self.functions if len(self.functions) >0 else None)
        #print(response)

        response_content = response.content
        #response_function = response.additional_kwargs['function_call'] if 'function_call' in response.additional_kwargs else None
        tool_calls = response.additional_kwargs.get('tool_calls', None)
        finish_reason = response.additional_kwargs.get('finish_reason', 'stop')

        if tool_calls == None:
            return self.__process_llm_response(response_content)
        #elif response_function['name'] != "question":
        #    return self.__process_function_call(response.additional_kwargs, response_function)
        elif tool_calls:
            return self.__process_tool_calls(tool_calls, response.additional_kwargs)
        #elif response_function['name'] == "question":
        #    return self.__process_llm_response(response_function['arguments'])
        else:
            #catch all
            return self.__process_llm_response(response_content)

    def __process_tool_calls(self, tool_calls, kwargs):
        self.__add_message(AIMessage(content = '', additional_kwargs = kwargs))
        for tool in tool_calls:
            function = tool['function']
            func_response = self.__process_function_call(function)
            self.__add_message(ToolMessage(content = func_response, name = function['name'], tool_call_id = tool['id']))
        return self.__submit_conversation()

    def __process_function_call(self, func):
        call = func['name']
        #print(f'tool: {call}')

        #functions are patterned as helper_<index>--<function_name>
        function_name = call.split('--')[1]
        helper_ind = int(call.split('--')[0].split('_')[1])

        helper = self.helpers[helper_ind]
      
        args = func['arguments']
        print(f'Calling function {function_name} with arguments {args}...')
        #check the function name and run it against the helper
        call_func = getattr(helper, function_name)
        ret = call_func(**json.loads(args, strict=False))# if args != '{}' else call_func()

        return ret

        #return self.__process_function_response(function_name, ret)

    def __process_llm_response(self, resp):
      print(f'LLM Response {resp}...')
      self.__add_message(AIMessage(content = resp))
      self.__log_conversation()
      return resp
        


    def function_definitions(self, object, name_prefix = ''):
        """
        Parses all internal functions of the object and returns an array of function definitions
        """
        definitions = []
        for name, func in object.__class__.__dict__.items():
            if callable(func) and not name.startswith('_') and name != 'function_definitions':#not in ['__module__', '__doc__', '__qualname__', '__dict__']:
                parameters = {}
                required = []
                for parameter in inspect.signature(func).parameters.values():
                    if parameter.name != 'self':
                      #print(parameter.annotation.__name__)
                      parameters[parameter.name] = {
                          #'name': parameter.name,
                          'type': self.__param_type(parameter.annotation.__name__)
                      }
                      if parameter.default is parameter.empty:
                          required.append(parameter.name)
                desc = func.__doc__.strip() if func.__doc__ != None else ''
                if hasattr(object, 'function_descriptor'):
                    desc = f'{object.function_descriptor} \n\n{desc}'
                definitions.append({
                    'type': 'function',
                    'function': {
                        'name': name_prefix + name,
                        'description': desc,
                        'parameters': {"type": "object", "properties": parameters, 'required': required},
                    }
                })
        return definitions
      

    def __param_type(self, defined_type):
      if defined_type == 'str':
        return 'string'
      elif defined_type == 'int':
        return 'integer'
      else:
        return defined_type

#test = lcChatBot()
#print(test.function_definitions())
