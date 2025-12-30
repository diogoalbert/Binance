import pandas as pd
import re
import os

def clean_val(val_str):
    if pd.isna(val_str): return 0.0
    if isinstance(val_str, (float, int)): return float(val_str)
    s = re.sub(r'[^0-9,\.-]', '', str(val_str))
    if ',' in s and '.' in s: s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def processar_vendas_totais():
    binance_input = 'Binance_Novembro2019-Dezembro2025.csv'
    bt_input = 'Relatorio_FIFO_Completo_Contraparte.csv'
    output_name = 'RELATORIO_FINAL_TODOS_OS_ANOS.csv'

    # 1. Carregar Dados
    df = pd.read_csv(binance_input)
    df['UTC_Time'] = pd.to_datetime(df['UTC_Time'])
    df['Val_Numeric'] = df['Change'].apply(clean_val)
    # Arredondamos o tempo para os 2 segundos mais próximos para agrupar transações separadas por milissegundos
    df['Time_Group'] = df['UTC_Time'].dt.round('2s') 
    df = df.sort_values('UTC_Time')

    inventory = {}
    final_report = []
    FIAT_ESTATAL = ['EUR', 'BRL', 'USD']

    # --- CARREGAR INVENTÁRIO INICIAL (BitcoinTrade) ---
    if os.path.exists(bt_input):
        df_bt = pd.read_csv(bt_input, sep=';', decimal=',')
        for _, row in df_bt.iterrows():
            if 'Retirada' in str(row['operação']):
                m = row['Moeda']
                if m not in inventory: inventory[m] = []
                inventory[m].append({'qty': abs(float(row['quantidade'])), 'cost': float(row['Valor (Custo FIFO)']), 'date': row['Data']})

    # --- PROCESSAR CRONOLOGIA BINANCE ---
    print("Iniciando processamento de 2018 a 2025...")

    for tg, group in df.groupby('Time_Group'):
        data_s = tg.strftime('%Y-%m-%d')
        
        entradas = group[group['Val_Numeric'] > 0]
        saidas = group[group['Val_Numeric'] < 0]

        # 1. Registrar Entradas (Depósitos e Swaps)
        for _, ent in entradas.iterrows():
            m = ent['Coin']
            if m in FIAT_ESTATAL: continue
            if m not in inventory: inventory[m] = []
            
            # Se não é compra com fiat, o custo vem do swap ou é zero (rendimentos)
            # Simplificação: custo zero para entradas diretas, herança via Swaps abaixo
            inventory[m].append({'qty': ent['Val_Numeric'], 'cost': 0.0, 'date': data_s})

        # 2. Processar Saídas (Vendas ou Swaps)
        for _, s in saidas.iterrows():
            if 'Fee' in s['Operation']: continue
            moeda_sai = s['Coin']
            qtd_sai = abs(s['Val_Numeric'])
            
            # Verifica se houve entrada de Fiat no mesmo grupo de tempo
            fiat_entry = entradas[entradas['Coin'].isin(FIAT_ESTATAL)]
            
            if not fiat_entry.empty:
                # VENDA TRIBUTÁVEL
                valor_fiat = abs(fiat_entry['Val_Numeric'].sum())
                
                # Se o inventário estiver vazio (erro de log), cria lote para não travar
                if moeda_sai not in inventory or not inventory[moeda_sai]:
                    inventory[moeda_sai] = [{'qty': 1000000.0, 'cost': 0.0, 'date': 'ORIGEM_DESCONHECIDA'}]

                qtd_restante = qtd_sai
                while qtd_restante > 1e-10 and inventory[moeda_sai]:
                    lote = inventory[moeda_sai][0]
                    vender = min(lote['qty'], qtd_restante)
                    
                    prop = vender / qtd_sai
                    custo_lote = (lote['cost'] / lote['qty']) * vender if lote['qty'] > 0 else 0
                    
                    final_report.append({
                        'Data_Venda': data_s,
                        'Moeda': moeda_sai,
                        'Quantidade': round(vender, 8),
                        'Data_Aquisição': lote['date'],
                        'Custo_Aquisição': round(custo_lote, 2),
                        'Valor_Venda': round(valor_fiat * prop, 2),
                        'Resultado': round((valor_fiat * prop) - custo_lote, 2)
                    })
                    
                    if lote['qty'] <= qtd_restante:
                        qtd_restante -= lote['qty']
                        inventory[moeda_sai].pop(0)
                    else:
                        lote['qty'] -= vender
                        lote['cost'] -= custo_lote
                        qtd_restante = 0
            else:
                # É UM SWAP OU RETIRADA (Transfere saldo/custo internamente)
                # (Lógica simplificada para manter o fluxo de datas)
                if moeda_sai in inventory and inventory[moeda_sai]:
                    lote_velho = inventory[moeda_sai].pop(0)
                    for _, e in entradas.iterrows():
                        m_nova = e['Coin']
                        if m_nova not in inventory: inventory[m_nova] = []
                        inventory[m_nova].append({'qty': e['Val_Numeric'], 'cost': lote_velho['cost'], 'date': lote_velho['date']})

    pd.DataFrame(final_report).to_csv(output_name, sep=';', index=False, decimal=',')
    print(f"Relatório gerado com sucesso: {output_name}")
    print(f"Total de linhas processadas: {len(final_report)}")

if __name__ == "__main__":
    processar_vendas_totais()