#Pega último vacinado por lote
import pandas as pd



#Lotes vencidos nas grafias identificadas no banco de dados
lotes = ["4120Z005",
" 4120Z005",
"414120Z005",
"4120Z005Z",
"004120Z005",
"4120Z005FIO",
"4120z001",
"4120Z001",
"04120Z001",
"CTMAV501"]

#Iteração pelos pedaços dos microdados de vacinação salvando apenas os que incluem os lotes de interesse
chunk_size=500000
batch_no=1
dados = pd.DataFrame()
for chunk in pd.read_csv('part-00000-897d1b55-09cb-4132-add1-9aa35772656f-c000.csv',chunksize=chunk_size, sep=";"):
    print (batch_no)
    batch_no+=1
    chunk = chunk[chunk["vacina_lote"].isin(lotes)]
    dados = dados.append(pd.DataFrame(chunk.groupby(['estabelecimento_municipio_codigo', "vacina_lote"])["vacina_dataAplicacao"].count()))


print("Terminou loop")
dados.groupby(['estabelecimento_municipio_codigo', "vacina_lote"]).sum().to_csv("quantidade por lote e mun.csv")
