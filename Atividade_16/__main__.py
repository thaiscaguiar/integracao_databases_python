# _author_ = "Diego Alves, Pedro Henrique, Thaís Aguiar"
# _license_ = "Beerware"
# _version_ = "0.0.1"

from modules.funcoes import *
import pandas as pd
import os

if __name__ == "__main__":
    try:
        # -------------------------------------------------    
        # Mensagens apresentadas ao usuário no sistema
        # -------------------------------------------------
        msg_1 = "Pressione [Enter] para voltar ao menu principal"
        msg_2 = "[1] Carregar arquivos csv"
        msg_3 = "[2] Popular bds (Cassandra/Matriz x MySQL/Filial)"
        msg_4 = "[3] Buscar dados da Filial"
        msg_5 = "[4] Preparar dados da Filial"
        msg_6 = "[5] Inserir dados da Filial na Matriz"
        msg_7 = "[6] Limpar bases"
        
       # Laço principal
        while True:

            # Limpar a tela
            os.system("cls")

            # Alternativas do menu principal
            op = int(input(f"\n{msg_2}\n{msg_3}\n{msg_4}\n{msg_5}\n{msg_6}\n{msg_7}\n\n\nQualquer outra tecla para sair\n\n> "))

            # Verifica qual opção do menu foi selecionada
            if op > 0 and op < 7:
                
                # -------------------------------------------------    
                # Carregar arquivos csv
                # -------------------------------------------------
                if op == 1:

                    # Limpar a tela
                    os.system("cls")

                    print(msg_2, "\n")
                    df_filial = leitura_csv("Sistema_A_SQL.csv",",")
                    df_matriz = leitura_csv("Sistema_B_NoSQL.csv",",")
            
                    # While para retornar ao menu principal
                    while True:
                        x = input(f"\n\n {msg_1}")
                        break

                # -------------------------------------------------    
                # Popular bds (Cassandra/Matriz x MySQL/Filial) 
                # -------------------------------------------------
                elif op == 2:
    
                    # Limpar a tela
                    os.system("cls")
    
                    print(msg_3, "\n")
                    
                    popular_bd_filial(df_filial)
                    popular_bd_matriz(df_matriz)
                                        
                    # While para retornar ao menu principal
                    while True:
                        x = input(f"\n\n{msg_1}")
                        break
                # -------------------------------------------------    
                # Buscar dados da Filial (MySQL)
                # -------------------------------------------------
                elif op == 3:
    
                    # Limpar a tela
                    os.system("cls")
    
                    print(msg_4, "\n")
                    
                    df_filial = buscar_dados_filial()
                    print("\nBase da dados MySQL (registros, colunas)")
                    print(df_filial.shape)
                    
                    # While para retornar ao menu principal
                    while True:
                        x = input(f"\n\n{msg_1}")
                        break
        
                # -------------------------------------------------    
                # Preparar dados da Filial (MySQL em DataFrame)
                # -------------------------------------------------
                elif op == 4:
    
                    # Limpar a tela
                    os.system("cls")
    
                    print(msg_5, "\n")
                    
                    df_filial = preparar_dados_filial(df_filial)
                    
                    
                    # While para retornar ao menu principal
                    while True:
                        x = input(f"\n\n{msg_1}")
                        break

                # -------------------------------------------------    
                # Inserir dados da Filial na Matriz
                # -------------------------------------------------
                elif op == 5:
    
                    # Limpar a tela
                    os.system("cls")
    
                    print(msg_6, "\n")
                    
                    filial_para_matriz(df_filial)
                    
                    
                    # While para retornar ao menu principal
                    while True:
                        x = input(f"\n\n{msg_1}")
                        break
        
                # -------------------------------------------------    
                # Limpar bases
                # -------------------------------------------------
                elif op == 6:
    
                    # Limpar a tela
                    os.system("cls")
    
                    print(msg_7, "\n")
                    
                    limpar_bases()
                                        
                    # While para retornar ao menu principal
                    while True:
                        x = input(f"\n\n{msg_1}")
                        break
                
                else:
                    break

            else:
                break
            
    except KeyboardInterrupt:
        print(f"O usuário optou por encerrar o programa")
        
    except Exception as e:
        print(str(e))
        
    finally:
        print(f"Bye bye")
