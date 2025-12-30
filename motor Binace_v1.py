import pandas as pd
import re
import os

def clean_val(val_str):
    if pd.isna(val_str): return 0.0
    if isinstance(val_str, float): return val_str
    s = re.sub(r'[^0-9,\.-]', '', str(val_str))
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def processar_portugal_fifo(bt_file, binance_file):
    # 1. Carregar Inventário Inicial (BitcoinTrade)
    # Assume que o arquivo segue o padrão do Relatorio_FIFO_Completo_Contraparte.csv
    print("Carregando inventário da BitcoinTrade...")
    df_bt = pd.read_csv(bt_file, sep=';', decimal=',')
    
    inventory = {}
    # Inicializa o inventário com os lotes remanescentes da BT
    # Lógica: Filtra o que ainda não saiu (estoque)
    for _, row in df_bt.iterrows():
        moeda = row['Moeda']
        if row['operação'] in ['Compra', 'Entrada por Transferência', 'Deposit']:
            if moeda not in inventory: inventory[moeda] = []
            inventory[moeda].append({
                'qty': float(row['quantidade']),
                'cost': float(row['Valor (Custo FIFO)']),
                'date': row['Data']
            })

    # 2. Carregar e Limpar dados da Binance
    print("Processando dados da Binance...")
    df_bin = pd.read_csv(binance_file)
    df_bin['UTC_Time'] = pd.to_datetime(df_bin['UTC_Time'])
    df_bin['Val_Numeric'] = df_bin['Change'].apply(clean_val)
    df_bin = df_bin.sort_values('UTC_Time')

    final_output = []

    # Agrupar por segundo para casar Swaps e Taxas
    for ts, group in df_bin.groupby('UTC_Time'):
        data_s = ts.strftime('%Y-%m-%d')
        hora_s = ts.strftime('%H:%M:%S')
        
        # Identificar o que saiu e o que entrou no segundo
        saidas = group[group['Val_Numeric'] < 0]
        entradas = group[group['Val_Numeric'] > 0]
        
        # REGRA PORTUGAL: Swap Cripto-Cripto (Manter Custo e Data)
        if not saidas.empty and not entradas.empty:
            for _, s in saidas.iterrows():
                moeda_sai = s['Coin']
                qtd_sai = abs(s['Val_Numeric'])
                
                # Se a contraparte for FIAT (EUR/BRL), é EVENTO TRIBUTÁVEL
                is_fiat_exit = any(curr in str(entradas['Coin'].values) for curr in ['EUR', 'BRL', 'USD'])
                
                if is_fiat_exit:
                    # Calcula ganho de capital (FIFO)
                    custo_total = 0
                    if moeda_sai in inventory:
                        while qtd_sai > 0 and inventory[moeda_sai]:
                            lote = inventory[moeda_sai][0]
                            if lote['qty'] <= qtd_sai:
                                custo_total += lote['cost']
                                qtd_sai -= lote['qty']
                                inventory[moeda_sai].pop(0)
                            else:
                                proporcao = qtd_sai / lote['qty']
                                custo_total += lote['cost'] * proporcao
                                lote['cost'] -= lote['cost'] * proporcao
                                lote['qty'] -= qtd_sai
                                qtd_sai = 0
                    
                    final_output.append({
                        'operação': 'Venda Fiat (Tributável)',
                        'Data': data_s, 'hora': hora_s,
                        'Moeda': moeda_sai, 'quantidade': s['Val_Numeric'],
                        'Valor (Custo FIFO)': round(custo_total, 2),
                        'Ativo_Contraparte': entradas['Coin'].iloc[0],
                        'Valor_Recebido_Contraparte': entradas['Val_Numeric'].sum()
                    })
                else:
                    # Permuta Cripto-Cripto: Herança de Lote
                    for _, e in entradas.iterrows():
                        if moeda_sai in inventory and inventory[moeda_sai]:
                            # O novo ativo entra com a data e custo do ativo antigo
                            lote_origem = inventory[moeda_sai][0] 
                            moeda_ent = e['Coin']
                            if moeda_ent not in inventory: inventory[moeda_ent] = []
                            
                            inventory[moeda_ent].append({
                                'qty': e['Val_Numeric'],
                                'cost': lote_origem['cost'], # Mantém custo histórico
                                'date': lote_origem['date']   # Mantém data original
                            })
                            # (Simplificação: consome o lote de saída)
                            inventory[moeda_sai].pop(0)

        # Tratar Rendimentos (Categoria E)
        rendimentos = group[group['Operation'].str.contains('Interest|Reward|Distribution', na=False)]
        for _, r in rendimentos.iterrows():
            m = r['Coin']
            if m not in inventory: inventory[m] = []
            inventory[m].append({
                'qty': r['Val_Numeric'],
                'cost': 0.0, # Conforme instrução: custo zero para apuração
                'date': data_s
            })

    # Gerar CSV final
    pd.DataFrame(final_output).to_csv('Relatorio_Portugal_Final.csv', sep=';', index=False)
    print("Relatório gerado com sucesso!")

# Para rodar:
# processar_portugal_fifo('Relatorio_FIFO_Completo_Contraparte.csv', 'Binance_Novembro2019-Dezembro2025.csv')
