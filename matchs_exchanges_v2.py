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

def processar_sistema_completo():
    # Arquivos de entrada
    binance_input = 'Binance_Novembro2019-Dezembro2025.csv'
    bt_input = 'Relatorio_FIFO_Completo_Contraparte.csv'
    
    # Arquivos de saída
    out_irs = '1_IRS_Vendas_Portugal.csv'
    out_swaps = '2_Historico_Swaps_2022_2025.csv'
    out_reconciliacao = '3_Reconciliacao_Transferencias.csv'

    # 1. CARREGAR E PREPARAR DADOS
    if not os.path.exists(binance_input):
        print(f"Erro: Arquivo {binance_input} não encontrado.")
        return

    df_bin = pd.read_csv(binance_input)
    df_bin['UTC_Time'] = pd.to_datetime(df_bin['UTC_Time'])
    df_bin['Val_Numeric'] = df_bin['Change'].apply(clean_val)
    df_bin['Time_Group'] = df_bin['UTC_Time'].dt.round('5s')
    df_bin = df_bin.sort_values('UTC_Time')

    bt_retiradas = []
    if os.path.exists(bt_input):
        df_bt = pd.read_csv(bt_input, sep=';', decimal=',')
        # Filtramos o que saiu da BitcoinTrade para casar com o que entrou na Binance
        bt_retiradas = df_bt[df_bt['operação'].str.contains('Retirada|Withdraw', na=False)].copy()
        bt_retiradas['quantidade'] = bt_retiradas['quantidade'].apply(lambda x: abs(float(str(x).replace(',','.'))))

    # 2. PROCESSAMENTO DE INVENTÁRIO E VENDAS
    inventory = {}
    report_irs = []
    report_swaps = []
    report_transf = []
    
    FIAT = ['EUR', 'BRL', 'USD']

    for tg, group in df_bin.groupby('Time_Group'):
        data_s = tg.strftime('%Y-%m-%d')
        entradas = group[group['Val_Numeric'] > 0]
        saidas = group[group['Val_Numeric'] < 0]

        # A) REGISTRO DE ENTRADAS (Depósitos e Swaps)
        for _, ent in entradas.iterrows():
            m = ent['Coin']
            qtd = ent['Val_Numeric']
            
            if m in FIAT: continue

            # Se for Depósito, tentamos o Auto-Match com a BitcoinTrade
            status_match = "Interno/Compra"
            custo_herdado = 0.0
            data_origem = data_s

            if ent['Operation'] == 'Deposit':
                # Busca na BT uma retirada da mesma moeda e valor similar (erro de 1% para taxas)
                match = bt_retiradas[(bt_retiradas['Moeda'] == m) & 
                                     (bt_retiradas['quantidade'] >= qtd * 0.99) & 
                                     (bt_retiradas['quantidade'] <= qtd * 1.01)].head(1)
                
                if not match.empty:
                    custo_herdado = float(match['Valor (Custo FIFO)'].iloc[0])
                    data_origem = match['Data'].iloc[0]
                    status_match = f"MATCH: Vindo de BitcoinTrade ({data_origem})"
                    # Removemos para não casar duas vezes
                    bt_retiradas = bt_retiradas.drop(match.index[0])
                else:
                    status_match = "DEPÓSITO SEM ORIGEM (Verificar)"

                report_transf.append({
                    'Data': data_s, 'Moeda': m, 'Qtd': qtd, 
                    'Tipo': 'ENTRADA', 'Status': status_match
                })

            if m not in inventory: inventory[m] = []
            inventory[m].append({'qty': qtd, 'cost': custo_herdado, 'date': data_origem})

        # B) PROCESSAMENTO DE SAÍDAS (Vendas ou Swaps)
        for _, s in saidas.iterrows():
            if 'Fee' in s['Operation']: continue
            m_sai = s['Coin']
            qtd_sai = abs(s['Val_Numeric'])
            
            # Se for retirada para fora da Binance
            if s['Operation'] in ['Withdraw', 'Withdrawal']:
                report_transf.append({'Data': data_s, 'Moeda': m_sai, 'Qtd': qtd_sai, 'Tipo': 'SAÍDA', 'Status': 'Para Carteira Externa'})
                if m_sai in inventory and inventory[m_sai]: inventory[m_sai].pop(0)
                continue

            fiat_entry = entradas[entradas['Coin'].isin(FIAT)]
            if not fiat_entry.empty:
                # VENDA TRIBUTÁVEL (IRS)
                valor_fiat = abs(fiat_entry['Val_Numeric'].sum())
                if m_sai in inventory and inventory[m_sai]:
                    lote = inventory[m_sai].pop(0)
                    report_irs.append({
                        'Data_Venda': data_s, 'Moeda': m_sai, 'Quantidade': qtd_sai,
                        'Data_Aquisição': lote['date'], 'Custo_Aquisição': lote['cost'],
                        'Valor_Venda': valor_fiat, 'Resultado': valor_fiat - lote['cost']
                    })
            else:
                # SWAP (Permuta Isenta)
                for _, e in entradas.iterrows():
                    if m_sai in inventory and inventory[m_sai]:
                        lote_v = inventory[m_sai].pop(0)
                        report_swaps.append({
                            'Data': data_s, 'Saiu': m_sai, 'Entrou': e['Coin'],
                            'Custo_Transferido': lote_v['cost'], 'Data_Original': lote_v['date']
                        })

    # Gerar os 3 arquivos
    pd.DataFrame(report_irs).to_csv(out_irs, sep=';', index=False, decimal=',')
    pd.DataFrame(report_swaps).to_csv(out_swaps, sep=';', index=False, decimal=',')
    pd.DataFrame(report_transf).to_csv(out_reconciliacao, sep=';', index=False, decimal=',')

    print(f"\nProcessamento concluído!")
    print(f"- {len(report_irs)} linhas de vendas para IRS.")
    print(f"- {len(report_swaps)} linhas de swaps (2022-2025).")
    print(f"- {len(report_transf)} transferências reconciliadas.")

if __name__ == "__main__":
    processar_sistema_completo()