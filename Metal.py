import time
from multiprocessing  import Pool
import requests
from bs4 import BeautifulSoup
import html5lib
import json
import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd
import numpy as np

import csv
import re
import os
def FetchData():
    enyclopaedia_metallum_country_url = 'https://www.metal-archives.com/browse/country'
    countries_request = requests.get(enyclopaedia_metallum_country_url)
    print(countries_request)

    soup = BeautifulSoup(countries_request.content, 'html5lib')
    countries_soup = soup.find_all('div',{'class': 'countryCol'})
    # print(countries_soup)
    re.compile('<a href="https://www.metal-archives.com\/lists\/(.+)">(.+)</a>')
    all_countries = {}
    country_regex = re.compile('<a href="https://www.metal-archives.com\/lists\/(.+)">(.+)</a>')

    for col in range(len(countries_soup)):
        country_a_href = countries_soup[col].find_all('a') # Further filtering tags to only country tags
        for a_href in range(len(country_a_href)):
            tag = str(country_a_href[a_href]).strip()
            matched = country_regex.match(tag).groups() # (ID, country)
            all_countries[matched[0]] = (matched[1])


    def get_page_query(country, CID, start, end):
        URL = 'http://www.metal-archives.com'
        AJAX_REQUEST_BEGIN = '/browse/ajax-country/c/'
        AJAX_REQUEST_END = '/json/1/'

        payload = {'sEcho': 0,
                   'iDisplayStart': start,
                   'iDisplayLength': end}
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1'}

        r = requests.get(URL + AJAX_REQUEST_BEGIN + CID + AJAX_REQUEST_END,
                         params=payload,
                         headers=headers)
        page_content = json.loads(r.text)
        total = page_content['iTotalRecords']

        r.close()

        return {'finished_query': total <= end, 'data': page_content['aaData'], 'total': total}

    def get_bands_by_country(opened_file, writer, country, CID):
        data = []
        start = 0
        end = 0
        query_complete = False
        name_ahref_tag = re.compile("<a href='(.*)'>(.*)</a>")
        status_spanclass_tag = re.compile('<span class="(.*)">(.*)</span>')
        website_regex = re.compile('https://www.metal-archives.com/bands/(.*)/(.*)')
        while query_complete == False:

            start = end + 1
            end = end + 500
            query = get_page_query(country, CID, start, end)
            query_complete = query['finished_query']
            query_data = query['data']

            band_info = []

            for idx in range(len(query_data)):
                band = query_data[idx]
                name_match = name_ahref_tag.match(band[0]).groups()
                status_match = status_spanclass_tag.match(band[3])

                name = name_match[1]
                website = name_match[0]

                ID = website_regex.match(website).groups()[1]
                genres = band[1]
                location = band[2]
                status = status_match.groups()[1]
                band_info = [name.encode('utf-8'), ID.encode('utf-8'),
                             country.encode('utf-8'), CID.encode('utf-8'), location.encode('utf-8'),
                             genres.encode('utf-8'), status.encode('utf-8'), website.encode('utf-8')]
                writer.writerow(band_info)

        print(country + ' (' + CID + '): SUCCESS [' + str(query['total']) + ']')
        return data


    with open('encyclopedia_metallum_data.csv', 'w') as tsvfile:
        writer = csv.writer(tsvfile, delimiter=',')
        headings = ['band_name', 'band_id',
                    'country', 'CID', 'location',
                    'genres', 'status',
                    'website']

        writer.writerow(headings)
        for CID, country in all_countries.items():
            get_bands_by_country(tsvfile, writer, country, CID)

def Classification(metal_data):

    metal_data[['CID']] = metal_data[['CID']].replace(np.NAN, 'NA')

    genre_regex_str = ['Black', 'Death', 'Doom|Stoner|Sludge',
                       'Electronic|Industrial', 'Experimental|Avant-garde',
                       'Folk|Viking|Pagan', 'Gothic', 'Grindcore|Goregrind',
                       'Groove', 'Hard Rock', 'Heavy', 'Metalcore|Deathcore',
                       'Power', 'Progressive', 'Speed', 'Symphonic', 'Thrash']

    genre_regexes = []
    genre_count = {}

    for g in range(len(genre_regex_str)):
        genre_count[genre_regex_str[g]] = []
        genre_regexes.append(re.compile(genre_regex_str[g]))
    genre_mix = []
    for idx, band in metal_data.iterrows():
        mix = 0

        for idx in range(len(genre_regexes)):
            genre_group = genre_regex_str[idx]
            if genre_regexes[idx].search(band['genres']) != None:
                genre_count[genre_group].append(True)
                mix = mix + 1
            else:
                genre_count[genre_group].append(False)

        genre_mix.append(mix)

    genre_data = metal_data.copy(deep=True)
    genre_data.insert(5, 'genre groups', genre_mix)
    for idx in range(len(genre_regex_str)):
        genre = genre_regex_str[idx]
        genre_data.insert((idx + 6), genre, genre_count[genre])

    genre_by_country = pd.pivot_table(data=genre_data,
                                      index=['country', 'CID'],
                                      values=genre_regex_str,
                                      aggfunc=np.sum)
    genre_total_data = genre_data.copy(deep = True)
    genre_count = []
    genre_categories = list(genre_total_data.columns)[6:]
    for idx in range(len(genre_categories)):
        counts = genre_total_data[genre_categories[idx]].value_counts()
        if True in counts:
            genre_count.append(counts[True])
        else:
            genre_count.append(0)

    g = {'genre': genre_categories, 'band count': genre_count}
    genre_total_df = pd.DataFrame(data = g, index = g['genre'], columns = ['band count'])
    genre_total_df.sort_values('band count', ascending = False)
    print(genre_total_df)
    genre_total_df.to_csv('Classification.csv')

    genre_lst = list(genre_by_country.columns)
    l = len(genre_by_country)

    titles = ['Black Metal', 'Death Metal', 'Doom Metal', 'Electronic/Industrial Metal',
              'Experimental/Avant-Garde Metal', 'Folk/Viking/Pagan Metal', 'Gothic Metal',
              'Grindcore/Goregrind', 'Groove Metal', 'Hard Rock', 'Heavy Metal', 'Metalcore/Deathcore',
              'Power Metal', 'Progressive Metal', 'Speed Metal', 'Symphonic Metal', 'Thrash Metal']
    def genre_bargraph(df, title, ylabel, genre, palette, cutoff=10):
        # Decode byte strings for country names and CID
        df.index = pd.MultiIndex.from_tuples(
            [(country.decode('utf-8') if isinstance(country, bytes) else country,
              CID.decode('utf-8') if isinstance(CID, bytes) else CID) for country, CID in df.index],
            names=['country', 'CID']
        )
        genre_counts = df[genre]
        genre_counts_filtered = genre_counts[genre_counts >= cutoff]
        genre_counts_sorted = genre_counts_filtered.sort_values(ascending=False)
        plt.figure(figsize=(12, 8))
        ax = plt.subplot(111, polar=True)
        theta = np.linspace(0.0, 2 * np.pi, len(genre_counts_sorted), endpoint=False)
        bars = ax.bar(theta, genre_counts_sorted, width=0.3, bottom=20, alpha=0.5, color=palette)
        tick_labels = genre_counts_sorted.index.get_level_values('country').tolist()
        ax.set_xticks(theta)
        ax.set_xticklabels(tick_labels, fontsize=8, rotation=45)
        ax.yaxis.set_label_position("right")
        ax.yaxis.tick_right()
        plt.title(title)
        ax.set_ylabel(str(ylabel))
        plt.show()

    for idx in range(len(genre_lst)):
        genre_bargraph(genre_by_country,
                       'Band Count of ' + titles[idx] + ' by Country',
                       'Band Count',
                       genre_lst[idx],
                       sns.cubehelix_palette(l, start=0.5, rot=-0.75))
    plt.show()


def PlotClassification(genre_total_df):
    plt.figure()
    plt.title('Genre Classification Counts For Metal Bands')
    genre_plot = sns.barplot(x=list(range(0, len(genre_total_df))),
                             y=genre_total_df['band count'],
                             palette=sns.color_palette("cubehelix", len(genre_total_df)))

    genre_plot.set(xlabel='Genre Classification',
                   ylabel='Band Count',
                   xticklabels=list(
                       genre_total_df['Unnamed: 0']))
    plt.xticks(rotation=90)
    genre_plot.set_facecolor('white')
    plt.show()

if __name__ == '__main__':
    global genre_by_country
    if not os.path.exists('encyclopedia_metallum_data.csv'):
        FetchData()
    metal_data = pd.read_csv('encyclopedia_metallum_data.csv', sep=',')
    Classification(metal_data)

    genre_total_df = pd.read_csv('Classification.csv')
    print(genre_total_df)
    PlotClassification(genre_total_df)
