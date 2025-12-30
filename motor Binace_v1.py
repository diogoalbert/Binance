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

def processar_portugal_final_2025():
    bt_input = 'Relatorio_FIFO_Completo_Contraparte.csv'
    binance_input = 'Binance_Novembro2019-Dezembro2025.csv'
    output_name = 'Relatorio_IRS_Portugal_2025_Final.csv'

    inventory = {'Binance': {}}
    
    # 1. HERANÇA DA BITCOINTRADE (Sincronização de saída/entrada)
    bt_transferencias = {}
    if os.path.exists(bt_input):
        df_bt = pd.read_csv(bt_input, sep=';', decimal=',')
        for _, row in df_bt.iterrows():
            if 'Retirada' in str(row['operação']):
                m = row['Moeda']
                if m not in bt_transferencias: bt_transferencias[m] = []
                bt_transferencias[m].append({'qty': abs(float(row['quantidade'])), 'cost': float(row['Valor (Custo FIFO)']), 'date': row['Data']})

    # 2. PROCESSAR BINANCE (Foco no Change e não no nome da Operação)
    df_bin = pd.read_csv(binance_input)
    df_bin['UTC_Time'] = pd.to_datetime(df_bin['UTC_Time'])
    df_bin['Val_Numeric'] = df_bin['Change'].apply(clean_val)
    df_bin = df_bin.sort_values('UTC_Time')

    final_report = []
    FIAT_ESTATAL = ['EUR', 'BRL', 'USD']

    for ts, group in df_bin.groupby('UTC_Time'):
        data_evento = ts.strftime('%Y-%m-%d')
        
        # Filtramos moedas que entram (+) e que saem (-)
        entradas = group[group['Val_Numeric'] > 0]
        saidas = group[group['Val_Numeric'] < 0]

        # A) ALIMENTAR INVENTÁRIO (Qualquer entrada que não seja Fiat)
        for _, ent in entradas.iterrows():
            m = ent['Coin']
            if m in FIAT_ESTATAL: continue # Fiat não entra no inventário de cripto
            
            if m not in inventory['Binance']: inventory['Binance'][m] = []
            
            # Se for depósito, tenta match com BT
            custo, dt_acq = 0.0, data_evento
            if ent['Operation'] == 'Deposit' and m in bt_transferencias and bt_transferencias[m]:
                lote_origem = bt_transferencias[m].pop(0)
                custo, dt_acq = lote_origem['cost'], lote_origem['date']
            
            inventory['Binance'][m].append({'qty': ent['Val_Numeric'], 'cost': custo, 'date': dt_acq})

        # B) PROCESSAR SAÍDAS (Vendas para Fiat ou Swaps)
        if not saidas.empty:
            for _, s in saidas.iterrows():
                # Ignoramos taxas isoladas (serão deduzidas do custo no futuro se necessário)
                if 'Fee' in s['Operation']: continue
                
                moeda_sai = s['Coin']
                qtd_total_venda = abs(s['Val_Numeric'])
                
                # Identifica se houve entrada de Fiat no mesmo segundo (Venda Tributável)
                fiat_entry = entradas[entradas['Coin'].isin(FIAT_ESTATAL)]
                
                if not fiat_entry.empty:
                    valor_fiat_recebido = abs(fiat_entry['Val_Numeric'].sum())
                    
                    # DESMEMBRAMENTO POR LOTE (FIFO)
                    qtd_restante = qtd_total_venda
                    if moeda_sai in inventory['Binance']:
                        while qtd_restante > 1e-10 and inventory['Binance'][moeda_sai]:
                            lote = inventory['Binance'][moeda_sai][0]
                            vender_deste_lote = min(lote['qty'], qtd_restante)
                            
                            # Proporções para o rateio
                            prop_venda = vender_deste_lote / qtd_total_venda
                            custo_prop = (lote['cost'] / lote['qty']) * vender_deste_lote if lote['qty'] > 0 else 0
                            receita_prop = valor_fiat_recebido * prop_venda
                            
                            final_report.append({
                                'Data_Venda': data_evento,
                                'Moeda': moeda_sai,
                                'Quantidade': round(vender_deste_lote, 8),
                                'Data_Aquisição': lote['date'], # UMA DATA POR LINHA
                                'Custo_Aquisição': round(custo_prop, 2),
                                'Valor_Venda': round(receita_prop, 2),
                                'Resultado': round(receita_prop - custo_prop, 2),
                                'Moeda_Recebida': fiat_entry['Coin'].iloc[0]
                            })
                            
                            # Atualiza lote
                            if lote['qty'] <= qtd_restante:
                                qtd_restante -= lote['qty']
                                inventory['Binance'][moeda_sai].pop(0)
                            else:
                                lote['qty'] -= vender_deste_lote
                                lote['cost'] -= custo_prop
                                qtd_restante = 0
                else:
                    # SWAP (Troca Cripto-Cripto/Stablecoin): Herança de Custo
                    # Consome o lote da moeda que sai e passa o custo para a que entra
                    for _, e in entradas.iterrows():
                        if moeda_sai in inventory['Binance'] and inventory['Binance'][moeda_sai]:
                            lote_origem = inventory['Binance'][moeda_sai].pop(0)
                            m_nova = e['Coin']
                            if m_nova not in inventory['Binance']: inventory['Binance'][m_nova] = []
                            inventory['Binance'][m_nova].append({
                                'qty': e['Val_Numeric'], 
                                'cost': lote_origem['cost'], 
                                'date': lote_origem['date']
                            })

    # Exportação
    pd.DataFrame(final_report).to_csv(output_name, sep=';', index=False, decimal=',')
    print(f"Sucesso! Relatório desmembrado (2018-2025) gerado em: {output_name}")

if __name__ == "__main__":
    processar_portugal_final_2025()