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
    
    
def redo():
    try:
        conn = conectar()
        conn.autocommit = True
        cur = conn.cursor()

        print("\n Iniciando REDO com base na tabela de log...\n")

        # Recupera todos os registros da tabela de log
        cur.execute("SELECT operacao, id_cliente, nome, saldo FROM log ORDER BY id_cliente ASC;")
        logs = cur.fetchall()

        transacoes_refeitas = set()

        for operacao, id_cliente, nome, saldo in logs:
            transacoes_refeitas.add(id_cliente)

            if operacao == 'INSERT':
                cur.execute("""
                    INSERT INTO clientes_em_memoria (id, nome, saldo)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (id_cliente, nome, saldo))
                print(f" REDO INSERT: Cliente {id_cliente}, Nome={nome}, Saldo={saldo}")

            elif operacao == 'UPDATE':
                cur.execute("""
                    UPDATE clientes_em_memoria
                    SET saldo = %s
                    WHERE id = %s;
                """, (saldo, id_cliente))
                print(f" REDO UPDATE: Cliente {id_cliente}, Novo Saldo={saldo}")

        if not logs:
            print("Nenhuma operação encontrada na tabela de log.")
        else:
            print("\n REDO finalizado para os clientes:")
            for cid in sorted(transacoes_refeitas):
                print(f" - Cliente ID: {cid}")

    except Exception as e:
        print(f" Erro ao executar REDO: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__":
    criar_tabelas()
    redo()
    
    

    