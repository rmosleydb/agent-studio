# agent-studio
A Databricks framework for quick Agent solutions.

###Initial Setup
To get started, access the `secrets` notebook in the `Setup` folder. There are **four** secrets that need to be created.

* Databricks Host - This is the workspace url. The sample code gets it configured for you.
* Databricks Token - This is the credential token that you will use to access the endpoints and underlying tools/data.
* Open AI - This is the Open AI Key that will be used when accessing Open AI. (It's recommended that you instead create an external model in databricks for open ai and point your agents to that.)
* Folder Path - This is the workspace path to the parent folder of Agent Studio.

**If you put your secrets in a different scope,** you'll need to update the scope in the necessary files.
* In the `Agent Studio` notebook, update the four references in Command 3.
* In the `Core/AgentCreator.py` file:
  + Update the environment variables in the `endpoint_config` variable at the top.
  + Update the secret references in the `master_template` variable.

### Use
There are two main notebooks that drive the application.
* Agent Studio - Run this notebook and click on the generated link at the bottom cells. This will open a Gradio App in a neighboring tab. Here is where you can setup your agent.
* Chatbot-App - Specify the endpoint name and a description, and run the notebook. Click the generated link at the bottom, and a Gradio App will open up in a neighboring tab. Use this to test your agent.

###Tools
After creating or modifying tool python files, run the `Parse Tools` notebook to generate the yaml for the tools.