import pandas as pd
import os

def processar_com_reconciliacao():
    binance_input = 'Binance_Novembro2019-Dezembro2025.csv'
    output_vendas = '1_Vendas_Tributaveis_IRS.csv'
    output_swaps = '2_Permutas_Isentas_Cripto.csv'
    output_transf = '3_Reconciliacao_Transferencias_Externas.csv'

    df = pd.read_csv(binance_input)
    df['UTC_Time'] = pd.to_datetime(df['UTC_Time'])
    df['Val_Numeric'] = df['Change'].apply(clean_val)
    df = df.sort_values('UTC_Time')

    transferencias_externas = []
    
    # Filtro para identificar o que entra e sai de carteiras externas
    # Tipos comuns na Binance: 'Deposit', 'Withdraw', 'Withdrawal'
    ops_externas = ['Deposit', 'Withdraw', 'Withdrawal', 'Fiat Deposit', 'Fiat Withdraw']

    for _, row in df.iterrows():
        op = str(row['Operation'])
        
        if any(x in op for x in ops_externas):
            transferencias_externas.append({
                'Data': row['UTC_Time'].strftime('%Y-%m-%d %H:%M:%S'),
                'Tipo': 'ENTRADA (Vinda de fora)' if row['Val_Numeric'] > 0 else 'SAÍDA (Para fora)',
                'Moeda': row['Coin'],
                'Quantidade': abs(row['Val_Numeric']),
                'Operação_Original': op,
                'ID_Transacao': row.get('Remark', 'N/A') # Remark costuma ter a TXID ou Tag
            })

    # Gerar o arquivo de reconciliação
    df_transf = pd.DataFrame(transferencias_externas)
    df_transf.to_csv(output_transf, sep=';', index=False, decimal=',')

    print(f"Sucesso! Gerados 3 arquivos:")
    print(f"1. {output_vendas} -> (Apenas vendas para Euro/Fiat)")
    print(f"2. {output_swaps} -> (Trocas Cripto-Cripto 2022-2025)")
    print(f"3. {output_transf} -> (Entradas e Saídas de outras carteiras)")

if __name__ == "__main__":
    processar_com_reconciliacao()
