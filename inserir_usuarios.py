import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider  # Se não tiver auth, não precisaimport uuid
import uuid

# Configurações do cluster
KEYSPACE = "seminario"
NODES = ["cassandra1", "cassandra2"]  # IPs dos containers Cassandra1 e Cassandra2

def criar_keyspace(session):
    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{
        'class': 'NetworkTopologyStrategy',
  	    'datacenter1': 2
    }};
    """)
    print(f"Keyspace '{KEYSPACE}' pronto.")

def criar_tabela(session):
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {KEYSPACE}.usuarios (
        id uuid PRIMARY KEY,
        email text,
        nome text,
        idade int
    );
    """)
    print("Tabela 'usuarios' pronta.")

def inserir_dados(session, csv_file):
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            session.execute(f"""
            INSERT INTO {KEYSPACE}.usuarios (id, email, nome, idade)
            VALUES (%s, %s, %s, %s)
            """, (uuid.UUID(row['id']), row['email'], row['nome'], int(row['idade'])))
    print("Dados inseridos com sucesso.")

def main():
    print("Conectando ao cluster Cassandra...")
    cluster = Cluster(NODES)
    session = cluster.connect()

    criar_keyspace(session)
    criar_tabela(session)
    inserir_dados(session, "usuarios.csv")

    cluster.shutdown()
    print("Conexão encerrada.")

if __name__ == "__main__":
    main()
