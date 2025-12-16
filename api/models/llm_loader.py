from functools import lru_cache
from api.core.config import settings

import os

# from langchain.chat_models import init_chat_model

# from langchain_huggingface.llms import HuggingFacePipeline
# from langchain_huggingface.chat_models.huggingface import ChatHuggingFace
# from transformers import AutoTokenizer, pipeline

from langchain_google_genai import ChatGoogleGenerativeAI

from langchain.agents import create_agent
from api.utils.retrieval import retrieve_close_chunks

# os.environ["HUGGINGFACEHUB_API_TOKEN"] = settings.HUGGINGFACEHUB_API_TOKEN
os.environ["GEMINI_API_KEY"] = settings.GEMINI_API_KEY


@lru_cache
def load_llm_agent():

    print("[LLM Loader] Loading LLM agent...")
    # # model = init_chat_model(
    # #                         settings.LLM_MODEL_NAME,
    # #                         model_provider="huggingface",)
    # #                         # temperature=settings.LLM_TEMPERATURE,
    # #                         # device=settings.DEVICE)

    # tokenizer = AutoTokenizer.from_pretrained(settings.LLM_MODEL_NAME)
    # tokenizer.model_max_length = 512
    # tokenizer.pad_token = tokenizer.eos_token

    # # Internal check inside transformers
    # if tokenizer.chat_template is None:
    #     # BLOOMZ_INSTRUCTION_TEMPLATE = (
    #     #     "{% for message in messages %}"
    #     #         "{% if message['role'] == 'user' %}"
    #     #             "{{ 'Instruction: ' + message['content'] + '\nResponse:' }}"
    #     #         "{% elif message['role'] == 'system' %}"
    #     #             "{{ message['content'] + '\n' }}"
    #     #         "{% else %}"
    #     #             "{{ message['content'] }}"
    #     #         "{% endif %}"
    #     #     "{% endfor %}"
    #     # )
    #     GPT2_INSTRUCTION_TEMPLATE = (
    #         "{% for message in messages %}"
    #             "{% if message['role'] == 'system' %}"
    #                 "{{ message['content'] + '\n\n' }}"
    #             "{% elif message['role'] == 'user' %}"
    #                 "{{ 'Instrucao: ' + message['content'] + '\nResposta:' }}"
    #             "{% elif message['role'] == 'assistant' %}"
    #                 "{{ message['content'] + '\n' }}"
    #             "{% endif %}"
    #         "{% endfor %}"
    #     )

    #     tokenizer.chat_template = GPT2_INSTRUCTION_TEMPLATE
    #     print("[LLM Loader] Applied custom chat template to tokenizer.")

    # pipe = pipeline(
    #     "text-generation",
    #     model=settings.LLM_MODEL_NAME,
    #     tokenizer=tokenizer,
    #     max_length=512,
    #     truncation=True,
    #     temperature=settings.LLM_TEMPERATURE,
    #     device= settings.DEVICE,
    #     max_new_tokens=512,
    #     padding=True
    # )

    # model = ChatHuggingFace( llm = HuggingFacePipeline(pipeline=pipe), tokenizer=tokenizer )

    model = ChatGoogleGenerativeAI(
        model=settings.LLM_MODEL_NAME,
        temperature=0.0
    )

    tools = [retrieve_close_chunks]

    agent = create_agent(model, tools, system_prompt=settings.LLM_SYSTEM_PROMPT)
    print("[LLM Loader] LLM agent loaded.")

    return agent