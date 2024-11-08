from scrapper.perfo import Perfo
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from pathlib import Path
import seaborn as sns
sns.set_theme()
perfo = Perfo()
dpi = 100
width = 1920
hight = 1095
size_multiplyer = 4

okreg_map = {
    1: 'I',
    2: 'II',
    3: 'III',
    4: 'IV',
    5: 'V',
    6: 'VI'
}

message = """autor: Adam Jura-Czarnecki, mail: adam@jura-czarnecki.pl"""


def print_df(df):
    print(df)
    print(df.dtypes)
    print(type(df))


class Komitet:
    def __init__(self, x):
        self.okreg_number = x.get('okreg_number')
        self.list_number = x.get('list_number')
        self.list_name = x.get('list_name')
        self.candidates = x.get('candidates')
        self.max_votes = x.get('max_votes')
        self.min_votes = x.get('min_votes')


def get_komitety(okreg_number):
    query = f"""
        select okreg_number, list_number, list_name, count(distinct candidate_number) candidates, max(votes) max_votes, min(votes) min_votes
        from `intel-tool.wybory2024.candidates`
        where okreg_number = {okreg_number}
        group by okreg_number, list_number, list_name
    """
    return [Komitet(x) for x in perfo.bq.query(query)]


def get_komitet_okreg_names(komitet):
    query = f"""
        select candidate_number, name, split(name, ' ')[offset(0)] name_short, sum(votes) votes
        from `intel-tool.wybory2024.candidates`
        where okreg_number = {komitet.okreg_number} and list_number = {komitet.list_number}
        group by candidate_number, name, name_short
        order by candidate_number
    """
    return [dict(x) for x in perfo.bq.query(query)]


def get_candidates_from_komitet_in_okreg(komitet):
    query = f"""
        select
          obwod,
{chr(10).join([f"          sum(case when candidate_number = {x} and list_number = {komitet.list_number} then votes else 0 end) candidate_{x},"
    for x in range(1, komitet.candidates+1)])}
          sum(case when list_number = {komitet.list_number} then votes else 0 end) komitet_votes,
          sum(votes) all_votes
        from `intel-tool.wybory2024.candidates`
        where okreg_number = {komitet.okreg_number}
        group by obwod
    """
    candidates = perfo.bq.query(query).to_dataframe()
    for x in range(1, komitet.candidates + 1):
        candidates[f'candidate_{x}_proc'] = (candidates[f'candidate_{x}'] / candidates['all_votes']) * 100
    candidates['komitet_votes_proc'] = (candidates['komitet_votes'] / candidates['all_votes']) * 100
    return candidates


def generate_komitet_in_okreg_map(komitet):
    obwody_map = gpd.read_file('map_service.html.json')
    obwody_map['obwod'] = obwody_map['obwod'].astype(str).astype(int)
    # wyjebywanie niepotrzebnych okręgów
    obwody_map = obwody_map.loc[obwody_map['okreg'] == okreg_map[komitet.okreg_number]]
    candidates = get_candidates_from_komitet_in_okreg(komitet)
    obwody_map = obwody_map.merge(candidates, on='obwod')
    names = get_komitet_okreg_names(komitet)
    cols, rows = 3 if komitet.candidates < 8 else 4, 3

    # GŁOSY
    fig, axs = plt.subplots(nrows=rows, ncols=cols)
    count = 0
    messsage_flag = False
    summary_map_flag = False
    for irow in range(axs.shape[0]):
        for icol in range(axs.shape[1]):
            if count < komitet.candidates:
                # plot
                if cols == 4:
                    title = names[count]["name"].split(' ')
                    title = f"{title[0]}{chr(10)}{' '.join(title[1:])}"
                else:
                    title = names[count]["name"]
                axs[irow][icol].set_title(title)
                obwody_map.plot(
                    ax=axs[irow][icol],
                    column=f'candidate_{count+1}',
                    edgecolor='black',
                    cmap='OrRd',
                    legend=True,
                    vmax=None,
                    vmin=None,
                    legend_kwds={
                        "label": "głosy",
                        "orientation": "vertical",
                    }
                )
                count += 1
            else:
                if messsage_flag and summary_map_flag:
                    # hide plot
                    axs[irow][icol].set_visible(False)
                elif summary_map_flag:
                    tabelka = pd.DataFrame(names)
                    tabelka = tabelka.set_index('candidate_number')
                    tabelka = tabelka[['name_short', 'votes']]
                    tabelka.columns = ['Osoba kandydacka', 'głosy']
                    # tabelka.set_index('Osoba kandydacka', inplace=True)
                    # print_df(tabelka)
                    table = pd.plotting.table(
                        ax=axs[irow][icol], data=tabelka, cellLoc='center',
                        loc='upper left', fontsize=12
                    )
                    table.auto_set_font_size(False)
                    table.set_fontsize(8)
                    axs[irow][icol].set_title('')
                    axs[irow][icol].axis('off')
                    messsage_flag = True
                    continue
                elif not messsage_flag and not summary_map_flag:
                    axs[irow][icol].set_title(f'Poparcie dla całego komitetu')
                    obwody_map.plot(
                        ax=axs[irow][icol],
                        column=f'komitet_votes',
                        edgecolor='black',
                        cmap='OrRd',
                        legend=True,
                        vmax=None,
                        vmin=None,
                        legend_kwds={
                            "label": "głosy",
                            "orientation": "vertical",
                        }
                    )
                    summary_map_flag = True
    fig.suptitle(f'Głosy oddane na osoby kandydackie komitetu {komitet.list_name} w okręgu {okreg_map[komitet.okreg_number]}:', fontsize=30)
    fig.text(0.05, 0.01, message, horizontalalignment='left', wrap=True)
    path = Path('okregi', f"{komitet.list_name.lower().replace(' ', '_')}")
    path.mkdir(parents=True, exist_ok=True)
    plt.gcf().set_size_inches(width / dpi, hight / dpi)
    # fig.tight_layout()
    plt.subplots_adjust(wspace=0.32, hspace=0.32)
    plt.savefig(f'{path}/okreg_{okreg_map[komitet.okreg_number]}-glosy.png', dpi=dpi * size_multiplyer)
    plt.close()

    # PROCENTY
    fig, axs = plt.subplots(nrows=rows, ncols=cols)
    count = 0
    messsage_flag = False
    summary_map_flag = False
    for irow in range(axs.shape[0]):
        for icol in range(axs.shape[1]):
            if count < komitet.candidates:
                # plot
                if cols == 4:
                    title = names[count]["name"].split(' ')
                    title = f"{title[0]}{chr(10)}{' '.join(title[1:])}"
                else:
                    title = names[count]["name"]
                axs[irow][icol].set_title(title)
                obwody_map.plot(
                    ax=axs[irow][icol],
                    column=f'candidate_{count+1}_proc',
                    edgecolor='black',
                    cmap='OrRd',
                    legend=True,
                    vmax=None,
                    vmin=None,
                    legend_kwds={
                        "label": "Poparcie w procentach",
                        "orientation": "vertical",
                    }
                )
                count += 1
            else:
                if messsage_flag:
                    # hide plot
                    axs[irow][icol].set_visible(False)
                elif summary_map_flag:
                    tabelka = pd.DataFrame(names)
                    tabelka = tabelka.set_index('candidate_number')
                    tabelka = tabelka[['name_short', 'votes']]
                    tabelka.columns = ['Osoba kandydacka', 'głosy']
                    # tabelka.set_index('Osoba kandydacka', inplace=True)
                    # print_df(tabelka)
                    table = pd.plotting.table(
                        ax=axs[irow][icol], data=tabelka, cellLoc='center',
                        loc='upper left', fontsize=12
                    )
                    table.auto_set_font_size(False)
                    table.set_fontsize(8)
                    axs[irow][icol].set_title('')
                    axs[irow][icol].axis('off')
                    # axs[irow][icol].text(0.1, 0.5, message, fontsize=12, horizontalalignment='left', verticalalignment='center')
                    # axs[irow][icol].set_title('')
                    # axs[irow][icol].axis('off')
                    messsage_flag = True
                elif not messsage_flag and not summary_map_flag:
                    axs[irow][icol].set_title(f'Poparcie dla całego komitetu')
                    obwody_map.plot(
                        ax=axs[irow][icol],
                        column=f'komitet_votes_proc',
                        edgecolor='black',
                        cmap='OrRd',
                        legend=True,
                        vmax=None,
                        vmin=None,
                        legend_kwds={
                            "label": "Poparcie w procentach",
                            "orientation": "vertical",
                        }
                    )
                    summary_map_flag = True
    fig.suptitle(f'Poparcie dla osób kandydackich komitetu {komitet.list_name} w okręgu {okreg_map[komitet.okreg_number]}:', fontsize=30)
    fig.text(0.05, 0.01, message, horizontalalignment='left', wrap=True)
    path = Path('okregi', f"{komitet.list_name.lower().replace(' ', '_')}")
    path.mkdir(parents=True, exist_ok=True)
    plt.gcf().set_size_inches(width / dpi, hight / dpi)
    # fig.tight_layout()
    plt.subplots_adjust(wspace=0.32, hspace=0.32)
    plt.savefig(f'{path}/okreg_{okreg_map[komitet.okreg_number]}-poparcie.png', dpi=dpi * size_multiplyer)
    plt.close()


if __name__ == '__main__':
    # okreg_1 = get_komitety(5)
    # generate_komitet_in_okreg_map(okreg_1[0])
    perfo.log('listuje komitety')
    wybory = [get_komitety(x) for x in range(1, 7)]
    for okreg in wybory:
        for komitet in okreg:
            perfo.log(f'Generuję mapę dla komitetu: {komitet.list_name} w okręgu: {okreg_map[komitet.okreg_number]}')
            generate_komitet_in_okreg_map(komitet)
