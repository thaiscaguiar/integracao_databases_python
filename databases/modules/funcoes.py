# Imports de libs builtin
import time, datetime as dt
# Imports de libs e classes
import pandas as pd
from modules.venda import Venda
from modules.connector import Interface_db


# Função para leitura de arquivo CSV, retornando em formato de DataFrame
def leitura_csv(nome_, sep_):
    """Função para realizar leitura de arquivo csv"""
    try:
        df = pd.read_csv(nome_, sep=sep_)                        
    except FileNotFoundError:
        print("Arquivo não encontrado.")
    except pd.errors.EmptyDataError:
        print("Sem dados no CSV.")
    except pd.errors.ParserError:
        print("Erro ao fazer o Parse.")
    except Exception:
        print("Erro na leitura do CSV.")                        
    else:
        print(f"Leitura do arquivo {nome_} realizada com sucesso!")
        return df


# Função para transformar DataFrame em lista de objetos do tipo venda
def transformar_em_lista_objetos_vendas(dataframe):
    """Função para retornar lista com a transformação de dataframe de dados em lista de objetos do tipo Vendas"""
    
    lista_objetos = []
    for index, row in dataframe.iterrows():
        dado = Venda(row[0], row[1], row[2])
        lista_objetos.append(dado)
    return lista_objetos


# Função para popular a base de dados da Filial (MySQL) a partir de lista de objetos do tipo Venda
def popular_bd_filial(df):
    try:
        
        interface_db = Interface_db("mysql://root:senha12345@127.0.0.1:3306/oldtech")

        # Transformando o dataframe de dados em lista de objetos do tipo Venda
        lista_objetos = transformar_em_lista_objetos_vendas(df)
    
        #Percorrer lista de objetos do tipo Venda e faz os inserts
        try:
            print("Percorrendo lista de objetos para inserir no banco...")
            valores = ""
            inicio = time.time()
            for i in range(len(lista_objetos)):
                valores = valores + f", ({lista_objetos[i].nota_fiscal}, '{lista_objetos[i].vendedor}', '{lista_objetos[i].total}')"
            valores = valores[1:]
            interface_db.insert_mysql(f"INSERT INTO venda (nota_fiscal, vendedor, total) VALUES {valores}")
            fim = time.time()
            
        except Exception as e:
            print(str(e))
        else:
            print(f"Lista de objetos do tipo venda inserida no MySQL (Filial)! Em {(fim - inicio)} segundos.")

    except Exception as e:
        
        print(str(e))


# Função para popular a base de dados da Matriz (Cassandra) a partir de lista de objetos do tipo Venda
def popular_bd_matriz(df):
    try:
    
        interface_db = Interface_db("cassandra://127.0.0.1:9042/oldtech")

        # Transformando o dataframe de dados em lista de objetos do tipo Venda
        lista_objetos = transformar_em_lista_objetos_vendas(df)
    
        #Percorrer lista de objetos do tipo Dados e faz os inserts
        try:
            print("\nPercorrendo a lista de objetos para inserir no banco...")
            inicio = time.time()
            for i in range(len(lista_objetos)):                              
                valores = f"(uuid(), {lista_objetos[i].nota_fiscal}, '{lista_objetos[i].vendedor}', '{lista_objetos[i].total}')"
                interface_db.insert_cassandra(f"INSERT INTO venda (id, nota_fiscal, vendedor, total) VALUES {valores}")
            fim = time.time()
        except Exception as e:
            print(str(e))
        else:
            print(f"Lista de objetos do tipo venda inserida no Cassandra (Matriz)! Em {(fim - inicio)} segundos.")
    except Exception as e:
        print(str(e))


# Função para buscar Dados da Filial (MySQL) e retornar em formato de DataFrame
def buscar_dados_filial():
    interface_db = Interface_db("mysql://root:senha12345@127.0.0.1:3306/oldtech")
    df_filial = interface_db.get_all("venda")
    return df_filial
    

# Função para preparar Dataframe que contem os dados da Filial para enviar a Matriz (Cassandra)
def preparar_dados_filial(dataframe):
    
    # Removendo coluna 0 (primary key auto_increment gerado no MySQL) 
    dataframe.drop([0], axis=1, inplace=True)
    
    # Removendo valores duplicados
    dataframe.drop_duplicates(inplace=True)
    
    # Preenchendo com 'Sem vendedor preenchido' quando encontrar o nome do vendedor vazio
    dataframe[2].fillna('Sem vendedor preenchido',inplace=True)
    
    # Removendo linhas com valores nulos em todas as colunas
    dataframe.dropna(how='all', inplace=True)
    
    return dataframe

# Função para inserir a base da filial no Cassandra
def filial_para_matriz(dataframe):
    interface_db = Interface_db("cassandra://127.0.0.1:9042/oldtech")
    # print(dataframe)
    try:
        print("\nPercorrendo dataframe para inserir no banco...")
        inicio = time.time()
        for row in dataframe.to_dict("records"):
            interface_db.insert_cassandra(f"INSERT INTO venda (id, nota_fiscal, vendedor, total) VALUES (uuid(), {row[1]}, '{row[2]}', '{row[3]}');")
        fim = time.time()
        
    except Exception as e:
        print("Erro: ", str(e))
    else:
        print(f"Dataframe inserido no Cassandra (Matriz)! Em {(fim - inicio)} segundos.")
    
# Função para limpar bases MySQL e Cassandra
def limpar_bases():
    try:
        print("\nLimpando bases MySQL e Cassandra...")
        inicio = time.time()
        interface_db1 = Interface_db("mysql://root:senha12345@127.0.0.1:3306/oldtech")
        interface_db1.delete_all_mysql("venda")
        interface_db2 = Interface_db("cassandra://127.0.0.1:9042/oldtech")
        interface_db2.delete_all_cassandra("venda")
        fim = time.time()
    except Exception as e:
        print(str(e))
    else:
        print(f"Processo concluído com sucesso! Em {(fim - inicio)} segundos.")