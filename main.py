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

def criar_tabelas():
    with conectar() as conn:
        with conn.cursor() as cur:
            # Criação das tabelas
            cur.execute("""
                CREATE UNLOGGED TABLE IF NOT EXISTS clientes_em_memoria (
                    id SERIAL PRIMARY KEY,
                    nome TEXT,
                    saldo NUMERIC
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS log (
                    operacao TEXT,
                    id_cliente INT,
                    nome TEXT,
                    saldo NUMERIC
                );
            """)
    print("Tabelas criadas com sucesso")


if __name__ == "__main__":
    criar_tabelas()
    
    

    