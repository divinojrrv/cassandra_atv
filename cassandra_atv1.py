from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Conectar-se ao cluster Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# Criar ou usar um keyspace
keyspace_name = "todas_tarefas_keyspace"
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
session.set_keyspace(keyspace_name)

# Criar ou usar uma tabela
table_name = "tarefas_table"
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        _id INT PRIMARY KEY,
        descricao TEXT
    )
"""
session.execute(create_table_query)

# Obter o último ID
query = "SELECT max(_id) FROM tarefas_table"
ultimo_id = session.execute(query).one().max

if ultimo_id is None:
    ultimo_id = 0

while True:
    print("Escolha uma opção:")
    print("1. Adicionar Tarefa")
    print("2. Listar Tarefas")
    print("3. Remover Tarefa")
    print("4. Sair")

    escolha = input()

    while escolha not in ["1", "2", "3", "4"]:
        print("Escolha inválida. Escolha uma opção do menu.")
        escolha = input()

    if escolha == "1":
        descricao = input("Digite a descrição da tarefa: ")

        # Atualizar o último ID usado
        ultimo_id += 1

        # Inserir a nova tarefa na tabela
        insert_query = f"INSERT INTO {table_name} (_id, descricao) VALUES (?, ?)"
        session.execute(insert_query, (ultimo_id, descricao))

        print("Tarefa adicionada com sucesso!")
        print("________________________________________________________")

    elif escolha == "2":
        # Listar as tarefas
        query = f"SELECT _id, descricao FROM {table_name}"
        tarefas = session.execute(query)
        for tarefa in tarefas:
            print(tarefa._id, tarefa.descricao)
        print("________________________________________________________")

    elif escolha == "3":
        id = input("Digite o ID da tarefa a ser removida: ")

        # Verificar se a tarefa existe e, em seguida, removê-la
        query = f"SELECT _id FROM {table_name} WHERE _id = ?"
        tarefa_a_remover = session.execute(query, (int(id),)).one()
        
        if tarefa_a_remover:
            delete_query = f"DELETE FROM {table_name} WHERE _id = ?"
            session.execute(delete_query, (int(id),))
            print("Tarefa removida com sucesso!")
        else:
            print("ID de tarefa inválido.")
        print("________________________________________________________")

    else:
        print("Saindo...")
        break
