import psycopg2
import json

# 1. Função para conectar ao banco
def conectar():
    # Retorna um contexto de conexão, permitindo uso com 'with'
    return psycopg2.connect(
        host="localhost",
        database="dblog",
        user="postgres",
        password="kerby3948"
    )
    
    try:
        conn = conectar()
        print("✅ Conexão bem-sucedida com o banco de dados!")
        conn.close()
    except psycopg2.Error as e:
        print("❌ Erro ao conectar:", e)
