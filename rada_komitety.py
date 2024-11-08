import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from pathlib import Path
import seaborn as sns
sns.set_theme()
komitety_p = ['Trzecia Droga', 'Lewica', 'Koalicja Obywatelska', 'Społeczny Poznań', 'Zjednoczona Prawica', 'Konfederacja Propolska']
komitety_c = ['trzecia_droga', 'lewica', 'koalicja_obywatelska', 'spoleczny_poznan', 'zjednoczona_prawica', 'konfederacja_propolska']
dpi = 100
width = 1920
hight = 1095
size_multiplyer = 2.5


obwody_map = gpd.read_file('map_service.html.json')
# print_df(obwody_map)
obwody_map['obwod'] = obwody_map['obwod'].astype(str).astype(int)
# print_df(obwody_map)

votes = pd.read_json(Path('candidates', 'rada_komitety.json'))
obwody_map = obwody_map.merge(votes, on='obwod')
obwody_map['all_votes'] = obwody_map['trzecia_droga'] + obwody_map['lewica'] + obwody_map['koalicja_obywatelska'] + obwody_map['spoleczny_poznan'] + obwody_map['zjednoczona_prawica'] + obwody_map['konfederacja_propolska']


for komitet in komitety_c:
    obwody_map[f'{komitet}_proc'] = (obwody_map[komitet] / obwody_map['all_votes']) * 100


cols, rows = 3, 2
fig, axs = plt.subplots(nrows=rows, ncols=cols)
count = 0
for irow in range(axs.shape[0]):
    for icol in range(axs.shape[1]):
        if count < len(komitety_c):
            # plot
            axs[irow][icol].set_title(f'Poparcie dla: {komitety_p[count]}')
            obwody_map.plot(
                ax=axs[irow][icol],
                column=f'{komitety_c[count]}_proc',
                edgecolor='black',
                cmap='OrRd',
                vmax=65,
                vmin=1,
                legend=True,
                legend_kwds={
                    "label": "Poparcie w procentach",
                    "orientation": "vertical"
                }
            )
            count += 1
        else:
            # hide plot
            axs[irow][icol].set_visible(False)
            # axs[irow][icol].text(0.1, 0.5, message, fontsize=12, horizontalalignment='left', verticalalignment='center')
            # axs[irow][icol].set_title('')
            # axs[irow][icol].axis('off')

fig.suptitle(f'Poparcie dla komitetów w wyborach do rady miasta Poznania w obwodach w procentach:', fontsize=30)
plt.gcf().set_size_inches(width / dpi, hight / dpi)
plt.savefig('rada_miasta_obwody-norm.png', dpi=dpi * size_multiplyer)

okregi = obwody_map.dissolve(by='okreg', aggfunc='sum')
for komitet in komitety_c:
    okregi[f'{komitet}_proc'] = (okregi[komitet] / okregi['all_votes']) * 100

okregi.to_excel('rada_okregi.xlsx')

fig, axs = plt.subplots(nrows=rows, ncols=cols)
count = 0
for irow in range(axs.shape[0]):
    for icol in range(axs.shape[1]):
        if count < len(komitety_c):
            # plot
            axs[irow][icol].set_title(f'Poparcie dla: {komitety_p[count]}')
            okregi.plot(
                ax=axs[irow][icol],
                column=f'{komitety_c[count]}_proc',
                edgecolor='black',
                cmap='OrRd',
                vmax=55,
                vmin=4.5,
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
            for komitet in komitety_c:
                tabelka[f'{komitet}'] = round(okregi[f'{komitet}_proc'], 2)
            table = pd.plotting.table(
                ax=axs[irow][icol], data=tabelka, cellLoc='center', colWidths=[0.25] * len(tabelka.columns),
                loc='upper left', fontsize=12
            )
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            axs[irow][icol].text(0, 0, 'autor: Adam Jura-Czarnecki\nemail: adam@jura-czarnecki.pl', fontsize=14, horizontalalignment='left', verticalalignment='bottom')
            axs[irow][icol].set_title('')
            axs[irow][icol].axis('off')

fig.suptitle(f'Poparcie dla komitetów w wyborach do rady miasta Poznania w okręgach w procentach:', fontsize=30)
plt.gcf().set_size_inches(width / dpi, hight / dpi)
plt.savefig('rada_miasta_okregi-norm.png', dpi=dpi * size_multiplyer)
