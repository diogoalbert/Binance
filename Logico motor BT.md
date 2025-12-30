<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# apresento a lógica estruturada que você deve copiar para o contexto do novo chat.

1. A Lógica do Motor de Cálculo (Resumo para o Próximo Chat)
O script que utilizamos baseia-se em três pilares fundamentais:

Pilar 1: Consolidação por Timestamp (O "Casamento" de Dados)
As corretoras (como a BitcoinTrade) costumam separar uma única operação em várias linhas (uma linha para a saída de BRL, outra para a entrada de Cripto, outra para a Taxa).
A Lógica: O motor agrupa todas as linhas que possuem a mesma Data e Hora exata. Ele identifica que o valor negativo em BRL naquele segundo é o "Custo de Aquisição" da quantidade positiva de Cripto que entrou no mesmo instante.
Pilar 2: Gestão de Inventário FIFO (First-In, First-Out)
O motor mantém uma "fila" (estante) de compras para cada moeda.
A Lógica: Quando ocorre uma saída (Venda ou Retirada), o motor não olha para o preço do dia. Ele vai ao início da fila e retira as moedas mais antigas, carregando o custo original delas para a operação atual.
Pilar 3: Atribuição de Custo em Swaps e Depósitos
Depósitos Externos: Atribuímos Custo Zero (conforme sua instrução para Portugal), criando um novo lote na fila com valor 0,00.
Vendas/Swaps: O motor identifica o Ativo_Contraparte. Se você vendeu CREAL por BRL, o lucro é Valor_Recebido_BRL menos o Custo_FIFO_do_CREAL.

2. O Prompt de Migração (Copie e Cole no Novo Chat)
Ao abrir o chat para a Binance/Nexo, cole este texto:
"Olá. Preciso analisar os CSVs da [Binance/Nexo] para o IRS de Portugal (FIFO). Já processei a BitcoinTrade e aqui está a lógica que o motor anterior utilizou:

Inventário Inicial: Carregue o arquivo Estado_Inventario_Final_BT.csv (anexo) como o saldo inicial de lotes e custos.
Consolidação: As operações devem ser agrupadas por segundo para casar valores de taxas, gastos em fiat/stablecoin e recebimento de ativos.
Rastreio de Transferência: Se houver uma entrada na Binance que coincida em data/quantidade com uma retirada da BitcoinTrade, o custo deve ser herdado (não é um novo custo, é o custo histórico).
Custo Zero: Entradas de origem desconhecida devem ser registradas com custo base zero.
Colunas Necessárias: O relatório final deve ter: Operação, Data, Moeda, Quantidade, Valor (Custo FIFO), Ativo_Contraparte, Valor_Recebido_Contraparte e Fees.
Vamos começar analisando o CSV da [Binance/Nexo] que vou enviar agora."

3. O Fluxo de Conexão entre Exchanges
Para fazer a conexão final, o segredo é o "Match de Transferência":

Saída na Exchange A: Registramos uma "Retirada" que consome o FIFO da Exchange A e gera um "Custo Herdado".
Entrada na Exchange B: Em vez de registrar como uma "Compra" (preço novo), o motor deve ler esse custo vindo da Exchange A.
Resultado: Se você comprou BTC na BitcoinTrade em 2018 e vender na Binance em 2024, o seu lucro será calculado sobre o preço de 2018.
Dica Final: Mantenha os nomes das colunas e o formato de data padronizados (AAAA-MM-DD) entre as exchanges para que o "Grande Relatório Final" possa ser gerado apenas unindo as tabelas (Append) no Excel.
Anexo o motor da BT e o modelo do relatório para que seja seguido a mesma base e possamos adaptar para o extrato da Binance. Depois que confirmar o entendimento prossigo anexando o Relatório completo da Binance.

Perfeito. Já compreendi completamente a lógica e os arquivos. Deixe-me criar um documento estruturado que consolida essa lógica para ser facilmente replicado no próximo chat quando você processar a Binance/Nexo.

***

## **Protocolo de Continuidade - Motor FIFO para Análise de Criptomoedas (IRS Portugal)**

### **1. Pilares Fundamentais do Motor de Cálculo**

#### **Pilar 1: Consolidação por Timestamp (Casamento de Dados)**

- **Problema:** Corretoras como BitcoinTrade separam uma única operação em múltiplas linhas:
    - Linha 1: Saída de BRL (débito)
    - Linha 2: Entrada de Cripto (crédito)
    - Linha 3: Taxa (fee)
- **Solução:** O motor agrupa todas as linhas com a mesma **Data + Hora exata** usando `groupby('Timestamp')`
- **Lógica:** O valor **negativo em BRL** naquele segundo = **Custo de Aquisição** da quantidade positiva de cripto que entrou no mesmo instante
- **Implementação:**

```python
df['Timestamp'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], dayfirst=True)
for ts, group in df.groupby('Timestamp'):
```


#### **Pilar 2: Gestão de Inventário FIFO (First-In, First-Out)**

- **Fila de Lotes:** O motor mantém uma estrutura `inventory[moeda] = [{'qty': X, 'cost': Y}, ...]`
- **Operação de Entrada:** Cada compra/depósito adiciona um novo lote ao final da fila
- **Operação de Saída (Venda/Retirada):** O motor consome lotes **a partir do início** da fila (FIFO)
    - Se `qtd_restante > lote['qty']`: consume o lote inteiro, move para o próximo
    - Se `qtd_restante <= lote['qty']`: consome parcialmente, atualiza o lote
- **Resultado:** O custo herdado é sempre o custo histórico original, não o preço do dia


#### **Pilar 3: Atribuição de Custo em Swaps e Depósitos**

- **Depósitos Externos (Origem Desconhecida):** Custo = **0,00** (conforme legislação portuguesa)
    - Cria um novo lote na fila com `{'qty': X, 'cost': 0.0}`
- **Vendas/Swaps:** O motor identifica `Ativo_Contraparte`:
    - Se vendeu cripto por BRL → `Ativo_Contraparte = "BRL"`, `Valor_Recebido_Contraparte = BRL_recebido`
    - Se fez swap cripto1 → cripto2 → `Ativo_Contraparte = "cripto2"`, `Valor_Recebido_Contraparte = qtd_cripto2`

***

### **2. Estrutura de Colunas do Relatório Final**

O relatório gerado deve seguir **exatamente** esta estrutura:


| Coluna | Tipo | Descrição |
| :-- | :-- | :-- |
| **operação** | String | `Entrada Fiat`, `Compra`, `Venda`, `Retirada para carteira externa`, `Depósito Cripto` |
| **Data** | AAAA-MM-DD | Data em formato ISO (essencial para append de exchanges) |
| **hora** | HH:MM:SS | Hora em formato 24h |
| **Moeda** | String | `Bitcoin`, `Ethereum`, `BRL`, `XRP`, `cReal`, etc. |
| **quantidade** | Float | Quantidade da moeda (vazio para Entrada Fiat) |
| **Valor (Custo FIFO)** | Float | **CUSTO DE AQUISIÇÃO** = o quanto foi pago historicamente |
| **Ativo_Contraparte** | String | `Banco`, `BRL`, `Carteira Externa`, `Ethereum`, etc. |
| **Valor_Recebido_Contraparte** | Float | **VALOR DE VENDA** = o quanto foi recebido (para cálculo do lucro) |
| **Fees** | Float | Taxas cobradas (em moeda da transação) |


***

### **3. Lógica de Cálculo por Tipo de Operação**

#### **A. Entrada Fiat (Depósito Bancário)**

```
operação: "Entrada Fiat"
Moeda: "BRL"
quantidade: [vazio]
Valor (Custo FIFO): [valor absoluto do depósito]
Ativo_Contraparte: "Banco"
Valor_Recebido_Contraparte: [mesmo valor]
Fees: 0.0
```


#### **B. Depósito Cripto (Custo Zero)**

```
operação: "Depósito Cripto"
Moeda: [cripto recebida]
quantidade: [qtd recebida]
Valor (Custo FIFO): 0.0  ← SEMPRE zero (origem desconhecida)
Ativo_Contraparte: "Carteira Externa"
Valor_Recebido_Contraparte: [mesma quantidade]
Fees: 0.0

Inventário: inventory[moeda].append({'qty': qtd, 'cost': 0.0})
```


#### **C. Compra (BRL → Cripto)**

```
operação: "Compra"
Moeda: [cripto comprada]
quantidade: [qtd comprada]
Valor (Custo FIFO): [BRL total gasto / qtd]  ← O que foi pago
Ativo_Contraparte: "BRL"
Valor_Recebido_Contraparte: [BRL total gasto]
Fees: [taxa proporcional à qtd]

Inventário: inventory[moeda].append({'qty': qtd, 'cost': valor_pago})
```


#### **D. Venda / Retirada para Carteira Externa**

```
operação: "Venda" ou "Retirada para carteira externa"
Moeda: [cripto vendida/retirada]
quantidade: [qtd vendida/retirada]
Valor (Custo FIFO): [custo herdado do FIFO]  ← Custo histórico
Ativo_Contraparte: "BRL" (venda) ou "Carteira Externa" (retirada)
Valor_Recebido_Contraparte: [BRL recebido] ou [0.0 para retirada]
Fees: [taxa da operação]

Cálculo FIFO:
qtd_restante = quantidade
while qtd_restante > 0 and inventory[moeda] não vazio:
    lote = inventory[moeda][^1_0]
    if lote['qty'] <= qtd_restante:
        custo_total += lote['cost']
        qtd_restante -= lote['qty']
        inventory[moeda].pop(0)  ← Remove lote consumido
    else:
        fracao = qtd_restante / lote['qty']
        custo_total += lote['cost'] * fracao
        lote['qty'] -= qtd_restante
        lote['cost'] -= lote['cost'] * fracao
        qtd_restante = 0
```


***

### **4. Fluxo de Conexão entre Exchanges (Match de Transferência)**

#### **Cenário: Bitcoin comprado em BitcoinTrade (2018) e vendido em Binance (2024)**

**Passo 1: Na BitcoinTrade**

- Transação: Retirada de 1 BTC
- Custo FIFO: 6.000 BRL (preço de 2018)
- Ativo_Contraparte: "Carteira Externa"
- Operação gera: Consumo do lote no inventário BT

**Passo 2: Na Binance**

- Transação: Entrada de 1 BTC
- ⚠️ **NÃO registrar como "Compra" nova** (preço atual)
- ⚠️ **SIM registrar com custo herdado = 6.000 BRL**

**Match de Transferência (Validação):**

```
Saída BT:    Retirada | 2018-12-03 | 1 BTC | Custo FIFO: 6.000 BRL
Entrada BN:  Depósito | 2018-12-03 | 1 BTC | Custo Herdado: 6.000 BRL
            (data, moeda e quantidade coincidem)
```

**Resultado Final (em 2024):**

- Venda na Binance: 1 BTC por 100.000 BRL
- Lucro = 100.000 BRL (Valor_Recebido_Contraparte) - 6.000 BRL (Valor_Custo_FIFO) = **94.000 BRL**

***

### **5. Estado Final do Inventário (Para Transferência entre Exchanges)**

Ao finalizar o processamento da BitcoinTrade, você terá um arquivo chamado:

```
Estado_Inventario_Final_BT.csv
```

Com a estrutura:

```
Moeda;Quantidade;Custo_Total
Bitcoin;0.5;15000.00
Ethereum;2.3;4560.00
XRP;1000.0;0.00
```

**Uso na Binance:**

1. Carregue este arquivo **antes** de processar o CSV da Binance
2. Use como estado inicial: `inventory[moeda] = [lotes de BT, ...]`
3. Todas as operações subsequentes herdam os custos históricos

***

### **6. Prompt de Migração (Para Copiar no Próximo Chat)**

```
Olá. Preciso analisar os CSVs da [Binance/Nexo] para o IRS de Portugal (FIFO).

CONTEXTO: Já processei a BitcoinTrade e vou anexar:
1. Estado_Inventario_Final_BT.csv → estado inicial de lotes e custos
2. Motor_v2.py → código do processamento FIFO

INSTRUÇÕES PARA O NOVO PROCESSAMENTO:

1. **Consolidação por Timestamp:**
   - Agrupe operações pela Data + Hora exata
   - Identifique: saída em fiat/stablecoin, entrada de cripto, fees

2. **Herança de Custos:**
   - Se houver entrada na Binance (mesma data/qtd) que coincida com retirada BT
   - O custo NÃO é novo → herda o custo histórico da BT

3. **Custo Zero:**
   - Entradas de origem desconhecida = custo base 0.00

4. **Estrutura de Saída:**
   Operação | Data (AAAA-MM-DD) | Hora | Moeda | Quantidade | Valor (Custo FIFO) | Ativo_Contraparte | Valor_Recebido_Contraparte | Fees

5. **Após o Processamento:**
   - Vamos fazer APPEND de todos os relatórios (BT + Binance + Nexo)
   - No Excel: `=[BT_range]; [Binance_range]` (mesmo padrão de colunas)

Vou enviar o CSV da [Binance/Nexo] agora.
```


***

### **7. Validação e Checklist Final**

Antes de finalizar o relatório da nova exchange, verifique:

- [ ] Todas as datas estão no formato `AAAA-MM-DD`
- [ ] Todas as quantidades estão em notação decimal (ponto, não vírgula)
- [ ] Coluna `Valor (Custo FIFO)` reflete o **custo histórico**, não o preço do dia
- [ ] Coluna `Valor_Recebido_Contraparte` reflete o valor **real recebido** na venda
- [ ] Retiradas para carteira externa têm `Valor_Recebido_Contraparte = 0.0`
- [ ] Depósitos cripto têm `Valor (Custo FIFO) = 0.0`
- [ ] Não existem "buracos" de cripto no inventário (saldos negativos = erro)
- [ ] As taxas (fees) estão em unidades da moeda transacionada

***


<div align="center">⁂</div>

[^1_1]: Relatorio_FIFO_Completo_Contraparte.csv

[^1_2]: Motor_v2.py

