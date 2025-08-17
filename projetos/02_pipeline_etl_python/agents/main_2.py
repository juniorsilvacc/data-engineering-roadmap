from agno.agent import Agent
from agno.models.groq import Groq
from agno.tools.postgres import PostgresTools
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize PostgresTools with connection details
postgres_tools = PostgresTools(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    db_name=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    table_schema="public",
)

# Create an agent with the PostgresTools
agent = Agent(tools=[postgres_tools], model=Groq(id="llama-3.3-70b-versatile"),)

agent.print_response("Fale todas as tabelas do banco de dados", markdown=True)

agent.print_response("""
Faça uma quary para pegar todas as cotações de bitcoin.
""")