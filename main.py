import psycopg2
import json

# 1. Função para conectar ao banco
def conectar():
   
    return psycopg2.connect(
        host="localhost",
        database="dblog",
        user="postgres",
        password="kerby3948"
    )
    
    
    # verifica a coneção 
    try:
        conn = conectar()
        print(" Conexão bem-sucedida com o banco de dados!")
        conn.close()
    except psycopg2.Error as e:
        print("Erro ao conectar:", e)

# 3. Criar tabelas no banco

def criar_tabelas():
    with conectar() as conn:
        with conn.cursor() as cursor:
            # Apaga se já existir e cria nova tabela memoria
            cursor.execute("""
                DROP TABLE IF EXISTS memoria;
                CREATE UNLOGGED TABLE memoria (
                    id INT PRIMARY KEY,
                    A INT,
                    B INT
                );
            """)

            # Apaga se já existir e cria tabela log_redo para armazenar logs
            cursor.execute("""
                DROP TABLE IF EXISTS log_redo;
                CREATE TABLE log_redo (
                    id SERIAL PRIMARY KEY,
                    transacao_id TEXT,   -- Identificador da transação (ex: T1, T2...)
                    operacao TEXT,       -- Tipo: INSERT, UPDATE, DELETE, ou NULL para BEGIN/COMMIT
                    dados JSONB,         -- Dados da operação armazenados em JSON
                    estado TEXT          -- Estado da operação: BEGIN, OPERACAO ou COMMIT
                );
            """)
    print("Tabelas criadas com sucesso.")
    