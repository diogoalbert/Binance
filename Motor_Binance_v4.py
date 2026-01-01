import pandas as pd
import re
import os
from datetime import datetime
from decimal import Decimal, getcontext

# Precisão absoluta para evitar erros de inventário por arredondamento infinitesimal
getcontext().prec = 60

def clean_val_dec(val_str):
    if pd.isna(val_str): return Decimal('0')
    if isinstance(val_str, (float, int)): return Decimal(str(val_str))
    s = re.sub(r'[^0-9,\.-]', '', str(val_str))
    if ',' in s and '.' in s: s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try: return Decimal(s)
    except: return Decimal('0')

def processar_motor_v7_final():
    binance_input = 'Binance_Novembro2019-Dezembro2025.csv'
    bt_input = 'Relatorio_FIFO_Completo_Contraparte.csv'
    
    # Nomes dos 3 arquivos de Saída
    out_irs = '1_Vendas_IRS_Formatado.csv'
    out_swaps = '2_Historico_Swaps_Audit.csv'
    out_reconciliacao = '3_Reconciliacao_Transferencias.csv'

    if not os.path.exists(binance_input):
        print(f"Erro: Arquivo {binance_input} não encontrado.")
        return

    # 1. Carregamento e Preparação com precisão Decimal
    df_bin = pd.read_csv(binance_input)
    df_bin['UTC_Time'] = pd.to_datetime(df_bin['UTC_Time'])
    df_bin['Val_Dec'] = df_bin['Change'].apply(clean_val_dec)
    # Agrupamento de 2s para capturar moedas que entram/saem no mesmo segundo
    df_bin['Time_Group'] = df_bin['UTC_Time'].dt.round('2s')
    df_bin = df_bin.sort_values('UTC_Time')

    bt_retiradas = pd.DataFrame()
    if os.path.exists(bt_input):
        try:
            df_bt = pd.read_csv(bt_input, sep=';', decimal=',')
            mask = df_bt['operação'].str.contains('Retirada|Withdraw', na=False, case=False)
            bt_retiradas = df_bt[mask].copy()
            bt_retiradas['qtd_dec'] = bt_retiradas['quantidade'].apply(clean_val_dec)
            bt_retiradas['custo_dec'] = bt_retiradas['Valor (Custo FIFO)'].apply(clean_val_dec)
        except:
            print("Aviso: Falha ao ler BitcoinTrade. Prosseguindo como Origem Externa.")

    inventory = {}
    report_irs, report_swaps, report_transf = [], [], []
    FIAT = ['EUR', 'BRL', 'USD', 'GBP']

    print("Iniciando Processamento Auditável (v7.1)...")

    for tg, group in df_bin.groupby('Time_Group'):
        # Extrair Data e Hora separadamente conforme solicitado
        data_s = tg.strftime('%Y-%m-%d')
        hora_s = tg.strftime('%H:%M:%S')
        
        entradas = group[group['Val_Dec'] > 0]
        saidas = group[group['Val_Dec'] < 0]

        # --- A) ENTRADAS (Alimentar Inventário) ---
        for _, ent in entradas.iterrows():
            m = ent['Coin']
            if m in FIAT: continue
            
            qtd_in = ent['Val_Dec']
            custo_in, data_aq, origem, is_ext = Decimal('0'), data_s, "Rendimento/Binance", False

            if ent['Operation'] in ['Deposit', 'Fiat Deposit']:
                if not bt_retiradas.empty:
                    # Match rigoroso por quantidade (margem de erro mínima para taxas de rede)
                    match = bt_retiradas[(bt_retiradas['Moeda'] == m) & 
                                         (bt_retiradas['qtd_dec'].apply(lambda x: abs(x - qtd_in) < Decimal('0.00001')))].head(1)
                    if not match.empty:
                        custo_in = match['custo_dec'].iloc[0]
                        data_aq = str(match['Data'].iloc[0])
                        origem = "BitcoinTrade (Histórico)"
                        bt_retiradas = bt_retiradas.drop(match.index[0])
                    else:
                        origem, is_ext = "Origem Externa", True
                else:
                    origem, is_ext = "Origem Externa", True
                
                # Registo na Reconciliação com Data e Hora separadas
                report_transf.append({
                    'Data': data_s, 'Hora': hora_s, 'Moeda': m, 'Qtd': float(qtd_in), 
                    'Tipo': 'ENTRADA', 'Status': origem
                })

            if m not in inventory: inventory[m] = []
            inventory[m].append({'qty': qtd_in, 'cost': custo_in, 'date': data_aq, 'origem': origem, 'is_ext': is_ext})

        # --- B) SAÍDAS (Vendas Fiat, Swaps, Levantamentos) ---
        for _, s in saidas.iterrows():
            if 'Fee' in s['Operation']: continue
            m_sai = s['Coin']
            qtd_sai = abs(s['Val_Dec'])

            if s['Operation'] in ['Withdraw', 'Withdrawal']:
                report_transf.append({
                    'Data': data_s, 'Hora': hora_s, 'Moeda': m_sai, 'Qtd': float(qtd_sai), 
                    'Tipo': 'SAÍDA', 'Status': 'Para Carteira Externa'
                })
                if m_sai in inventory and inventory[m_sai]: inventory[m_sai].pop(0)
                continue

            fiat_entry = entradas[entradas['Coin'].isin(FIAT)]
            if not fiat_entry.empty:
                # VENDA PARA FIAT (Evento Tributável)
                moeda_fiat = fiat_entry['Coin'].iloc[0]
                val_fiat_total = abs(fiat_entry['Val_Dec'].sum())
                
                qtd_restante = qtd_sai
                while qtd_restante > 0 and m_sai in inventory and inventory[m_sai]:
                    lote = inventory[m_sai][0]
                    vender = min(lote['qty'], qtd_restante)
                    
                    prop = vender / qtd_sai
                    custo_prop = (lote['cost'] / lote['qty']) * vender if lote['qty'] > 0 else Decimal('0')
                    receita_prop = val_fiat_total * prop
                    
                    # Cálculo de Dias e Isenção
                    status_isento = ""
                    try:
                        d_venda = pd.to_datetime(data_s)
                        d_aq = pd.to_datetime(lote['date'])
                        delta = (d_venda - d_aq).days
                        if lote['is_ext']:
                            status_isento = "TBD"
                        else:
                            status_isento = f"{delta} dias (ISENTO)" if delta > 365 else f"{delta} dias"
                    except: status_isento = "TBD"

                    report_irs.append({
                        'Data_Venda': data_s,
                        'Ativo': m_sai,
                        'Moeda_Venda': moeda_fiat,
                        'Valor_Venda': float(round(receita_prop, 2)),
                        'Data_Aquisicao': lote['date'],
                        'Custo_Aquisicao_USD': float(round(custo_prop, 2)),
                        'Origem_Externa': lote['origem'],
                        'Resultado': float(round(receita_prop - custo_prop, 2)),
                        'Isento_365d': status_isento
                    })
                    
                    if lote['qty'] <= qtd_restante:
                        qtd_restante -= lote['qty']
                        inventory[m_sai].pop(0)
                    else:
                        lote['qty'] -= vender
                        lote['cost'] -= custo_prop
                        qtd_restante = 0
            else:
                # SWAP (Herança de Custo 0.00 ou Histórico)
                for _, e in entradas.iterrows():
                    if m_sai in inventory and inventory[m_sai]:
                        lv = inventory[m_sai].pop(0)
                        if e['Coin'] not in inventory: inventory[e['Coin']] = []
                        inventory[e['Coin']].append({
                            'qty': e['Val_Dec'], 'cost': lv['cost'], 'date': lv['date'], 
                            'origem': lv['origem'], 'is_ext': lv['is_ext']
                        })
                        report_swaps.append({
                            'Data': data_s, 'Hora': hora_s, 'Saiu': m_sai, 'Entrou': e['Coin'], 
                            'Custo_Herdado': float(lv['cost']), 'Data_Orig': lv['date']
                        })

    # 3. Exportação com colunas exatas
    df_irs = pd.DataFrame(report_irs)
    cols_irs = ['Data_Venda', 'Ativo', 'Moeda_Venda', 'Valor_Venda', 'Data_Aquisicao', 'Custo_Aquisicao_USD', 'Origem_Externa', 'Resultado', 'Isento_365d']
    if not df_irs.empty:
        df_irs[cols_irs].to_csv(out_irs, sep=';', index=False, decimal=',')

    pd.DataFrame(report_swaps).to_csv(out_swaps, sep=';', index=False, decimal=',')

    df_reconc = pd.DataFrame(report_transf)
    if not df_reconc.empty:
        cols_reconc = ['Data', 'Hora', 'Moeda', 'Qtd', 'Tipo', 'Status']
        df_reconc[cols_reconc].to_csv(out_reconciliacao, sep=';', index=False, decimal=',')
    
    print(f"\nSucesso! Gerados:")
    print(f"1. {out_irs} (Com colunas personalizadas)")
    print(f"2. {out_swaps} (Com Data e Hora)")
    print(f"3. {out_reconciliacao} (Com Data e Hora separadas)")

if __name__ == "__main__":
    processar_motor_v7_final()