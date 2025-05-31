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
    
    
    
# Carrega dados iniciais na tabela memoria a partir do arquivo JSON
def carregar_dados_iniciais():
    with open("medado.json", "r") as f:
        dados = json.load(f)["INITIAL"]

    with conectar() as conn:
        with conn.cursor() as cursor:
            for i in range(len(dados["id"])):
                id_val = dados["id"][i]
                a_val = dados["A"][i]
                b_val = dados["B"][i]
                cursor.execute(
                    "INSERT INTO memoria (id, A, B) VALUES (%s, %s, %s)",
                    (id_val, a_val, b_val)
                )
        conn.commit()
    print("Dados iniciais inseridos na tabela memoria.")
    
    
    
# Carrega o log para a tabela log_redo
def carregar_log():
    with conectar() as conn:
        with conn.cursor() as cursor:
            with open("EntradLog.txt", "r") as f:
                linhas = [line.strip() for line in f.readlines()]

            for linha in linhas:
                if "<crash>" in linha:
                    break
                elif linha.startswith("<start"):
                    if "CKPT" in linha:
                        continue
                    tid = linha.replace("<start ", "").replace(">", "")
                    cursor.execute(
                        "INSERT INTO log_redo (transacao_id, operacao, dados, estado) VALUES (%s, %s, %s, %s)",
                        (tid, None, None, 'BEGIN')
                    )
                elif linha.startswith("<commit"):
                    tid = linha.replace("<commit ", "").replace(">", "")
                    cursor.execute(
                        "INSERT INTO log_redo (transacao_id, operacao, dados, estado) VALUES (%s, %s, %s, %s)",
                        (tid, None, None, 'COMMIT')
                    )
                elif linha.startswith("<T"):  # UPDATE
                    parts = linha.strip('<>').split(',')
                    if len(parts) == 5:
                        tid, rid, attr, old_val, new_val = parts
                        dados_json = json.dumps({"id": int(rid), attr: int(new_val)})
                        cursor.execute(
                            "INSERT INTO log_redo (transacao_id, operacao, dados, estado) VALUES (%s, %s, %s, %s)",
                            (tid, 'UPDATE', dados_json, 'OPERACAO')
                        )
                elif linha.startswith("<I"):  # INSERT
                    parts = linha.strip('<>').split(',')
                    if len(parts) == 5:
                        _, tid, rid, a_val, b_val = parts
                        dados_json = json.dumps({"id": int(rid), "A": int(a_val), "B": int(b_val)})
                        cursor.execute(
                            "INSERT INTO log_redo (transacao_id, operacao, dados, estado) VALUES (%s, %s, %s, %s)",
                            (tid, 'INSERT', dados_json, 'OPERACAO')
                        )
                elif linha.startswith("<D"):  # DELETE
                    parts = linha.strip('<>').split(',')
                    if len(parts) == 3:
                        _, tid, rid = parts
                        dados_json = json.dumps({"id": int(rid)})
                        cursor.execute(
                            "INSERT INTO log_redo (transacao_id, operacao, dados, estado) VALUES (%s, %s, %s, %s)",
                            (tid, 'DELETE', dados_json, 'OPERACAO')
                        )
        conn.commit()
    print("Log carregado com sucesso na tabela log_redo.")
    
    
# Aplica REDO para restaurar estado após queda
def aplicar_redo():
    with conectar() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("SELECT DISTINCT transacao_id FROM log_redo WHERE estado = 'COMMIT'")
                transacoes_commitadas = [row[0] for row in cursor.fetchall()]
                print("Transações para REDO:", transacoes_commitadas)

                for tid in transacoes_commitadas:
                    cursor.execute(
                        "SELECT operacao, dados FROM log_redo WHERE transacao_id = %s AND estado = 'OPERACAO'",
                        (tid,)
                    )
                    operacoes = cursor.fetchall()

                    for operacao, dados_json in operacoes:
                        # Verifica se 'dados_json' já é um dict (caso contrário, converta)
                        if isinstance(dados_json, str):
                            dados = json.loads(dados_json)  # Converte de string para dict
                        else:
                            dados = dados_json  # Se já for dict, não faz nada

                        if operacao == 'UPDATE':
                            colunas = [f"{k} = %s" for k in dados if k != 'id']
                            valores = [dados[k] for k in dados if k != 'id']
                            sql = f"UPDATE memoria SET {', '.join(colunas)} WHERE id = %s"
                            valores.append(dados["id"])
                            cursor.execute(sql, valores)

                        elif operacao == 'INSERT':
                            colunas = ', '.join(dados.keys())
                            placeholders = ', '.join(['%s'] * len(dados))
                            valores = list(dados.values())
                            sql = f"INSERT INTO memoria ({colunas}) VALUES ({placeholders})"
                            cursor.execute(sql, valores)

                        elif operacao == 'DELETE':
                            sql = "DELETE FROM memoria WHERE id = %s"
                            cursor.execute(sql, (dados["id"],))
                
                conn.commit()
                print("REDO concluído.")

            except Exception as e:
                print("Erro durante a execução do REDO:", e)





if __name__ == "__main__":
    criar_tabelas()
    carregar_dados_iniciais()
    carregar_log()
    
    print("Simule a queda do banco e depois execute novamente[teste 1,2] .\n")
    aplicar_redo()
    
    