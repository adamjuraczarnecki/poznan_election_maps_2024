import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from pathlib import Path
import seaborn as sns
sns.set_theme()

candidates = ['czerwinski', 'garczewski', 'jaskowiak', 'plewinski', 'urbanska']
dpi = 100
width = 1920
hight = 1095
size_multiplyer = 2.5


message = """
podział na komisje wyborcze
autor: Adam Jura-Czarnecki
mail: adam@jura-czarnecki.pl
"""


def print_df(df):
    print(df)
    print(df.dtypes)
    print(type(df))


obwody_map = gpd.read_file('map_service.html.json')
# print_df(obwody_map)
obwody_map['obwod'] = obwody_map['obwod'].astype(str).astype(int)
# print_df(obwody_map)


for candidate in candidates:
    votes = pd.read_json(Path('candidates', 'prezydent', f'{candidate}.json'))
    obwody_map = obwody_map.merge(votes, on='obwod')

obwody_map['all_votes'] = obwody_map['czerwinski'] + obwody_map['garczewski'] + obwody_map['jaskowiak'] + obwody_map['plewinski'] + obwody_map['urbanska']

for candidate in candidates:
    obwody_map[f'{candidate}_proc'] = (obwody_map[candidate] / obwody_map['all_votes']) * 100


print_df(obwody_map)
cols, rows = 3, 2
fig, axs = plt.subplots(nrows=rows, ncols=cols)
count = 0
for irow in range(axs.shape[0]):
    for icol in range(axs.shape[1]):
        if count < len(candidates):
            # plot
            axs[irow][icol].set_title(f'Poparcie dla: {candidates[count]}')
            obwody_map.plot(
                ax=axs[irow][icol],
                column=f'{candidates[count]}_proc',
                edgecolor='black',
                cmap='OrRd',
                vmax=None,
                vmin=None,
                legend=True,
                legend_kwds={
                    "label": "Poparcie w procentach",
                    "orientation": "vertical"
                }
            )
            count += 1
        else:
            # hide plot
            # axs[irow][icol].set_visible(False)
            axs[irow][icol].text(0.1, 0.5, message, fontsize=12, horizontalalignment='left', verticalalignment='center')
            axs[irow][icol].set_title('')
            axs[irow][icol].axis('off')

fig.suptitle(f'Poparcie dla kandydatów w wyborach na prezydenta Poznania w obwodach w procentach:', fontsize=30)
plt.gcf().set_size_inches(width / dpi, hight / dpi)
plt.savefig('prezydenckie_obwody.png', dpi=dpi * size_multiplyer)

okregi = obwody_map.dissolve(by='okreg', aggfunc='sum')
for candidate in candidates:
    okregi[f'{candidate}_proc'] = (okregi[candidate] / okregi['all_votes']) * 100
# print_df(okregi)
okregi.to_excel('prezydent_okregi.xlsx')

fig, axs = plt.subplots(nrows=rows, ncols=cols)
count = 0
for irow in range(axs.shape[0]):
    for icol in range(axs.shape[1]):
        if count < len(candidates):
            # plot
            axs[irow][icol].set_title(f'Poparcie dla: {candidates[count]}')
            okregi.plot(
                ax=axs[irow][icol],
                column=f'{candidates[count]}_proc',
                edgecolor='black',
                cmap='OrRd',
                vmax=None,
                vmin=None,
                legend=True,
                legend_kwds={
                    "label": "Poparcie w procentach",
                    "orientation": "vertical"
                }
            )
            count += 1
        else:
            # hide plot
            # axs[irow][icol].set_visible(False)
            tabelka = pd.DataFrame(index=okregi.index)
            for candidate in candidates:
                tabelka[f'{candidate}'] = round(okregi[f'{candidate}_proc'], 2)
            table = pd.plotting.table(
                ax=axs[irow][icol], data=tabelka, cellLoc='center', colWidths=[0.25] * len(tabelka.columns),
                loc='upper left', fontsize=12
            )
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            axs[irow][icol].text(0, 0, 'autor: Adam Jura-Czarnecki\nemail: adam@jura-czarnecki.pl', fontsize=14, horizontalalignment='left', verticalalignment='bottom')
            axs[irow][icol].set_title('')
            axs[irow][icol].axis('off')

fig.suptitle(f'Poparcie dla kandydatów w wyborach na prezydenta Poznania w okręgach w procentach:', fontsize=30)
plt.gcf().set_size_inches(width / dpi, hight / dpi)
plt.savefig('prezydenckie_okregi.png', dpi=dpi * size_multiplyer)
# plt.show()
