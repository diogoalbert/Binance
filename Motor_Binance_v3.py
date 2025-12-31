import pandas as pd
import re
import os
from datetime import datetime

def clean_val(val_str):
    if pd.isna(val_str): return 0.0
    if isinstance(val_str, (float, int)): return float(val_str)
    s = re.sub(r'[^0-9,\.-]', '', str(val_str))
    if ',' in s and '.' in s: s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def processar_relatorio_final_customizado():
    # Arquivos
    binance_input = 'Binance_Novembro2019-Dezembro2025.csv'
    bt_input = 'Relatorio_FIFO_Completo_Contraparte.csv'
    output_custom = 'Relatorio_Final_IRS_Formatado.csv'

    # 1. Carregar Dados
    if not os.path.exists(binance_input):
        print(f"Arquivo {binance_input} não encontrado.")
        return

    df_bin = pd.read_csv(binance_input)
    df_bin['UTC_Time'] = pd.to_datetime(df_bin['UTC_Time'])
    df_bin['Val_Numeric'] = df_bin['Change'].apply(clean_val)
    # Agrupamento de 5s para capturar transações fragmentadas
    df_bin['Time_Group'] = df_bin['UTC_Time'].dt.round('5s')
    df_bin = df_bin.sort_values('UTC_Time')

    # Carregar BitcoinTrade para Match
    bt_retiradas = []
    if os.path.exists(bt_input):
        df_bt = pd.read_csv(bt_input, sep=';', decimal=',')
        bt_retiradas = df_bt[df_bt['operação'].str.contains('Retirada|Withdraw', na=False, case=False)].copy()
        bt_retiradas['quantidade'] = bt_retiradas['quantidade'].apply(lambda x: abs(float(str(x).replace(',','.'))))

    inventory = {}
    relatorio_final = []
    
    # Lista de Fiats para identificar Vendas Tributáveis
    FIAT = ['EUR', 'BRL', 'USD', 'GBP']

    print("Processando inventário e gerando relatório formatado...")

    for tg, group in df_bin.groupby('Time_Group'):
        data_atual = tg
        data_str = tg.strftime('%Y-%m-%d')
        
        entradas = group[group['Val_Numeric'] > 0]
        saidas = group[group['Val_Numeric'] < 0]

        # --- ENTRADAS (Alimentar Estoque) ---
        for _, ent in entradas.iterrows():
            m = ent['Coin']
            qtd = ent['Val_Numeric']
            if m in FIAT: continue

            # Lógica de Origem
            origem_desc = "Binance (Interno)"
            custo = 0.0
            data_aq = data_str
            is_external_unknown = False

            if ent['Operation'] == 'Deposit':
                # Tentar Match com BT
                match = bt_retiradas[(bt_retiradas['Moeda'] == m) & 
                                     (bt_retiradas['quantidade'] >= qtd * 0.99) & 
                                     (bt_retiradas['quantidade'] <= qtd * 1.01)].head(1)
                
                if not match.empty:
                    custo = float(match['Valor (Custo FIFO)'].iloc[0])
                    data_aq = match['Data'].iloc[0] # Data original da BT (ex: 2018)
                    origem_desc = "BitcoinTrade (Histórico)"
                    bt_retiradas = bt_retiradas.drop(match.index[0])
                else:
                    # Depósito sem origem comprovada
                    origem_desc = "Depósito Externo (Sem Match)"
                    is_external_unknown = True
            
            if m not in inventory: inventory[m] = []
            inventory[m].append({
                'qty': qtd, 
                'cost': custo, 
                'date': data_aq, 
                'origem': origem_desc,
                'is_unknown': is_external_unknown
            })

        # --- SAÍDAS (Vendas ou Swaps) ---
        for _, s in saidas.iterrows():
            if 'Fee' in s['Operation']: continue
            
            m_sai = s['Coin']
            qtd_sai = abs(s['Val_Numeric'])
            
            # Verifica se entrou Fiat (Evento Tributável)
            fiat_entry = entradas[entradas['Coin'].isin(FIAT)]
            
            if not fiat_entry.empty:
                # É UMA VENDA TRIBUTÁVEL
                moeda_fiat = fiat_entry['Coin'].iloc[0]
                valor_total_fiat = abs(fiat_entry['Val_Numeric'].sum())
                
                # Consumo FIFO
                if m_sai not in inventory: inventory[m_sai] = []
                # Fallback se inventário vazio
                if not inventory[m_sai]:
                    inventory[m_sai].append({'qty': qtd_sai, 'cost': 0, 'date': data_str, 'origem': 'Forçado', 'is_unknown': True})

                qtd_restante = qtd_sai
                while qtd_restante > 1e-9 and inventory[m_sai]:
                    lote = inventory[m_sai][0]
                    vender = min(lote['qty'], qtd_restante)
                    
                    # Proporcionalidade
                    prop = vender / qtd_sai
                    custo_prop = (lote['cost'] / lote['qty']) * vender if lote['qty'] > 0 else 0
                    receita_prop = valor_total_fiat * prop
                    
                    # --- CÁLCULO DA ISENÇÃO (365 DIAS) ---
                    coluna_isento = ""
                    try:
                        d_venda = pd.to_datetime(data_str)
                        d_aquisicao = pd.to_datetime(lote['date'])
                        delta_dias = (d_venda - d_aquisicao).days
                        
                        if lote['is_unknown']:
                            coluna_isento = "TBD (Origem Externa)"
                        elif delta_dias > 365:
                            coluna_isento = f"{delta_dias} dias (ISENTO)"
                        else:
                            coluna_isento = f"{delta_dias} dias"
                    except:
                        coluna_isento = "Erro Data"

                    # Adicionar ao Relatório com as COLUNAS SOLICITADAS
                    relatorio_final.append({
                        'Data_Venda': data_str,
                        'Ativo': m_sai,
                        'Moeda_Venda': moeda_fiat,
                        'Valor_Venda': round(receita_prop, 2),
                        'Data_Aquisicao': lote['date'],
                        'Custo_Aquisicao_USD': round(custo_prop, 2), # Nome mantido conforme pedido, mesmo que seja EUR
                        'Origem_Externa': lote['origem'],
                        'Resultado': round(receita_prop - custo_prop, 2),
                        'Isento_365d': coluna_isento
                    })

                    # Atualizar Saldo Lote
                    if lote['qty'] <= qtd_restante:
                        qtd_restante -= lote['qty']
                        inventory[m_sai].pop(0)
                    else:
                        lote['qty'] -= vender
                        lote['cost'] -= custo_prop
                        qtd_restante = 0
            else:
                # É UM SWAP (Permuta Isenta - Rolar Custo)
                for _, e in entradas.iterrows():
                    m_ent = e['Coin']
                    if m_sai in inventory and inventory[m_sai]:
                        lote_velho = inventory[m_sai].pop(0)
                        
                        # O novo ativo herda a origem e a data do antigo
                        if m_ent not in inventory: inventory[m_ent] = []
                        inventory[m_ent].append({
                            'qty': e['Val_Numeric'], 
                            'cost': lote_velho['cost'], 
                            'date': lote_velho['date'],
                            'origem': lote_velho['origem'],
                            'is_unknown': lote_velho['is_unknown']
                        })

    # Gerar CSV
    df_final = pd.DataFrame(relatorio_final)
    # Reordenar colunas para garantir a ordem exata pedida
    cols = ['Data_Venda', 'Ativo', 'Moeda_Venda', 'Valor_Venda', 'Data_Aquisicao', 
            'Custo_Aquisicao_USD', 'Origem_Externa', 'Resultado', 'Isento_365d']
    
    # Garante que só exporta se tiver dados, senão cria vazio com cabeçalhos
    if not df_final.empty:
        df_final = df_final[cols]
    else:
        df_final = pd.DataFrame(columns=cols)
        
    df_final.to_csv(output_custom, sep=';', index=False, decimal=',')
    print(f"Relatório Final Gerado: {output_custom}")
    print("Colunas ajustadas conforme solicitação.")

if __name__ == "__main__":
    processar_relatorio_final_customizado()
